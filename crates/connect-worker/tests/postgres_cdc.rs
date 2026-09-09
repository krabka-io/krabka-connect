#![cfg(unix)]
#![recursion_limit = "256"]

//! Docker-backed acceptance proof for the managed Postgres CDC worker.

use std::{
    io::{self, Read as _, Write as _},
    net::SocketAddr,
    process::{Child, Command},
    time::Duration,
};

use assert2::assert;
use bytes::Bytes;
use krabka_broker::{Broker, BrokerConfig};
use krabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerRecord};
use krabka_connect_postgres::{
    ColumnValue, EntityKey, model::ScalarValue, schema::PostgresProtoEncoder,
};
use krabka_schema_registry::{
    config::{RegistryConfig, RegistryRuntimeConfig, SecurityConfig},
    kafkastore::KafkaStore,
    rest::{self, AppState},
};
use krabka_units::millis;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort as _, WaitFor},
    runners::AsyncRunner as _,
};
use tokio::{task::JoinHandle, time::timeout};
use tokio_postgres::{Client, NoTls};
use tokio_util::sync::CancellationToken;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// The deadline for a container to start, which includes the image pull.
///
/// `AsyncRunner::start` waits for the pull with no bound of its own. A stalled
/// pull thus holds the test process open until the CI job wall stops it, and
/// the job log then names no test as the cause.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);

const POSTGRES_PORT: u16 = 5432;
const REGISTRY_PORT: u16 = 8081;
const TOPIC: &str = "db.public.orders";
const CONNECTOR_ID: &str = "orders-cdc-acceptance";
const WAIT: Duration = Duration::from_secs(30);

#[derive(Clone, Copy, Debug)]
enum RegistryFlavor {
    Krabka,
    Confluent,
}

struct WorkerProcess(Child);

impl WorkerProcess {
    fn kill(mut self) -> TestResult {
        self.0.kill()?;
        let status = self.0.wait()?;
        assert!(!status.success(), "force-killed worker exited successfully");
        Ok(())
    }
}

impl Drop for WorkerProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

enum RunningRegistry {
    Krabka {
        cancel: CancellationToken,
        task: JoinHandle<io::Result<()>>,
    },
    Confluent(Box<ContainerAsync<GenericImage>>),
}

impl RunningRegistry {
    async fn stop(self) -> TestResult {
        match self {
            Self::Krabka { cancel, task } => {
                cancel.cancel();
                task.abort();
                let _ = task.await;
            }
            Self::Confluent(container) => drop(container),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObservedRecord {
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    operation: String,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn postgres_cdc_acceptance_matrix_survives_force_killed_worker() -> TestResult {
    let _ = tracing_subscriber::fmt()
        .with_test_writer()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    for registry in [RegistryFlavor::Krabka, RegistryFlavor::Confluent] {
        run_acceptance_case(registry).await?;
    }
    Ok(())
}

async fn run_acceptance_case(registry_flavor: RegistryFlavor) -> TestResult {
    let postgres = start_postgres().await?;
    let postgres_port = postgres.get_host_port_ipv4(POSTGRES_PORT.tcp()).await?;
    let database_url = format!("postgres://postgres:postgres@127.0.0.1:{postgres_port}/app");
    let (database, database_connection) = connect_postgres(&database_url).await?;
    database
        .batch_execute("CREATE TABLE public.orders (id BIGINT PRIMARY KEY, status TEXT NOT NULL)")
        .await?;

    let log_dir = tempfile::TempDir::new()?;
    let broker_addr = reserve_address()?;
    let gateway = docker_gateway()?;
    let mut broker_config = BrokerConfig::for_tests(log_dir.path().to_path_buf());
    broker_config.listen_addr = broker_addr;
    broker_config.advertised_listener = format!("{gateway}:{}", broker_addr.port());
    let broker = Broker::start(broker_config).await?;
    let bootstrap = format!("{gateway}:{}", broker.listen_addr().port());
    let (registry, registry_url) = start_registry(registry_flavor, &bootstrap).await?;

    let first_worker = start_worker(&database_url, &bootstrap, &registry_url).await?;
    database
        .batch_execute(
            "BEGIN;
             INSERT INTO public.orders (id, status) VALUES (1, 'pending');
             UPDATE public.orders SET status = 'paid' WHERE id = 1;
             DELETE FROM public.orders WHERE id = 1;
             COMMIT;",
        )
        .await?;

    wait_for_record_count(&bootstrap, 3).await?;
    let first = read_records(&bootstrap, "orders-cdc-first", 3, None).await?;
    let encoder = PostgresProtoEncoder::from_registry(&registry_url).await?;
    let key_one = encoded_key(&encoder, 1)?;
    assert!(first.len() == 3);
    assert!(
        first
            .iter()
            .map(|record| record.operation.as_str())
            .collect::<Vec<_>>()
            == ["insert", "update", "delete"]
    );
    assert!(
        first
            .iter()
            .all(|record| record.key.as_deref() == Some(key_one.as_ref()))
    );
    assert!(first[0].value.is_some());
    assert!(first[1].value.is_some());
    assert!(first[0].value != first[1].value);
    assert!(first[2].value.is_none());

    first_worker.kill()?;

    let second_worker = start_worker(&database_url, &bootstrap, &registry_url).await?;
    database
        .batch_execute(
            "ALTER TABLE public.orders ADD COLUMN note TEXT;
             INSERT INTO public.orders (id, status) VALUES (2, 'new');
             UPDATE public.orders SET note = 'schema-v2' WHERE id = 2;",
        )
        .await?;

    wait_for_at_least_record_count(&bootstrap, 5).await?;
    let key_two = encoded_key(&encoder, 2)?;
    let final_records =
        read_records(&bootstrap, "orders-cdc-final", 5, Some(key_two.as_ref())).await?;
    let replayed = final_records.len().saturating_sub(5);
    assert!(
        replayed <= 3,
        "at-least-once replay exceeded one source batch"
    );
    assert!(final_records[..3] == first);
    let evolved = final_records
        .iter()
        .filter(|record| record.key.as_deref() == Some(key_two.as_ref()))
        .collect::<Vec<_>>();
    assert!(evolved.len() == 2);
    assert!(evolved[0].operation == "insert");
    assert!(evolved[1].operation == "update");
    assert!(evolved.iter().all(|record| record.value.is_some()));
    assert!(evolved[0].value != evolved[1].value);

    second_worker.kill()?;
    registry.stop().await?;
    broker.shutdown().await;
    drop(database);
    database_connection.await??;
    drop(postgres);
    Ok(())
}

/// Reserves a port on every interface.
///
/// The registry runs in a container and has to reach this broker, so binding
/// the loopback address would put the broker somewhere the container cannot
/// route to.
fn reserve_address() -> io::Result<std::net::SocketAddr> {
    let listener = std::net::TcpListener::bind("0.0.0.0:0")?;
    listener.local_addr()
}

/// The host's address on Docker's default bridge.
///
/// This is the one address both sides can use: it is a local interface on the
/// host, and it is the default route out of a container on that bridge. The
/// broker advertises it so that the registry container and the test process
/// itself both reach the same listener.
fn docker_gateway() -> TestResult<String> {
    let output = std::process::Command::new("docker")
        .args([
            "network",
            "inspect",
            "bridge",
            "--format",
            "{{ (index .IPAM.Config 0).Gateway }}",
        ])
        .output()?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "could not read the docker bridge gateway: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))
        .into());
    }
    let gateway = String::from_utf8(output.stdout)?.trim().to_owned();
    if gateway.is_empty() {
        return Err(io::Error::other("the docker bridge reported no gateway").into());
    }
    Ok(gateway)
}

async fn start_postgres() -> TestResult<ContainerAsync<GenericImage>> {
    Ok(tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        GenericImage::new("postgres", "18")
            .with_exposed_port(POSTGRES_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_PASSWORD", "postgres")
            .with_env_var("POSTGRES_DB", "app")
            .with_cmd(["postgres", "-c", "wal_level=logical"])
            .start(),
    )
    .await??)
}

async fn connect_postgres(
    database_url: &str,
) -> TestResult<(Client, JoinHandle<Result<(), tokio_postgres::Error>>)> {
    let connected = timeout(WAIT, async {
        loop {
            match tokio_postgres::connect(database_url, NoTls).await {
                Ok((client, connection)) => {
                    return (client, tokio::spawn(connection));
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Postgres did not become ready"))?;
    Ok(connected)
}

async fn start_registry(
    flavor: RegistryFlavor,
    bootstrap: &str,
) -> TestResult<(RunningRegistry, String)> {
    match flavor {
        RegistryFlavor::Krabka => {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?;
            let cancel = CancellationToken::new();
            let store = KafkaStore::start(
                &RegistryConfig {
                    bootstrap: bootstrap.to_owned(),
                    schemas_topic: "_schemas".to_owned(),
                    schemas_topic_rf: 1,
                    client_id: "connect-cdc-acceptance".to_owned(),
                    advertised_url: format!("http://{address}"),
                    group_id: "schema-registry".to_owned(),
                    leader_eligibility: true,
                    runtime: RegistryRuntimeConfig::default(),
                    security: SecurityConfig::default(),
                },
                cancel.clone(),
            )
            .await?;
            let task = tokio::spawn(async move {
                axum::serve(listener, rest::router(AppState { store })).await
            });
            Ok((
                RunningRegistry::Krabka { cancel, task },
                format!("http://{address}"),
            ))
        }
        RegistryFlavor::Confluent => {
            let registry = timeout(
                CONTAINER_START_TIMEOUT,
                GenericImage::new("mirror.gcr.io/confluentinc/cp-schema-registry", "7.7.1")
                    .with_wait_for(WaitFor::message_on_stdout(
                        "Server started, listening for requests",
                    ))
                    .with_env_var("SCHEMA_REGISTRY_HOST_NAME", "localhost")
                    .with_env_var(
                        "SCHEMA_REGISTRY_LISTENERS",
                        format!("http://0.0.0.0:{REGISTRY_PORT}"),
                    )
                    .with_env_var(
                        "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                        format!("PLAINTEXT://{bootstrap}"),
                    )
                    .with_env_var("SCHEMA_REGISTRY_KAFKASTORE_TOPIC", "_schemas")
                    .with_env_var("SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR", "1")
                    .start(),
            )
            .await
            .map_err(|_| io::Error::other("the schema registry did not start in time"))??;
            let port = registry.get_host_port_ipv4(REGISTRY_PORT.tcp()).await?;
            Ok((
                RunningRegistry::Confluent(Box::new(registry)),
                format!("http://127.0.0.1:{port}"),
            ))
        }
    }
}

async fn start_worker(
    database_url: &str,
    bootstrap: &str,
    schema_registry_url: &str,
) -> TestResult<WorkerProcess> {
    let health = reserve_address()?;
    let child = Command::new(env!("CARGO_BIN_EXE_krabka-connect-worker"))
        .args([
            "--connector-id",
            CONNECTOR_ID,
            "--kafka-bootstrap",
            bootstrap,
            "--schema-registry-url",
            schema_registry_url,
            "--postgres-url",
            database_url,
            "--postgres-slot",
            "orders_krabka",
            "--postgres-tables",
            "orders",
            "--batch-size",
            "16",
            "--commit-interval-ms",
            "50",
            "--poll-backoff-ms",
            "20",
            "--health-listen",
            &health.to_string(),
        ])
        .spawn()?;
    let process = WorkerProcess(child);
    wait_for_ready(health).await?;
    Ok(process)
}

async fn wait_for_ready(address: SocketAddr) -> TestResult {
    timeout(WAIT, async {
        loop {
            if let Ok(mut stream) =
                std::net::TcpStream::connect_timeout(&address, Duration::from_millis(100))
            {
                stream.set_read_timeout(Some(Duration::from_millis(500)))?;
                stream.set_write_timeout(Some(Duration::from_millis(500)))?;
                let mut response = String::new();
                if stream
                    .write_all(
                        b"GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                    )
                    .is_ok()
                    && stream.read_to_string(&mut response).is_ok()
                    && response.starts_with("HTTP/1.1 200")
                {
                    return Ok::<(), io::Error>(());
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connector did not start"))??;
    Ok(())
}

async fn wait_for_record_count(bootstrap: &str, expected: usize) -> TestResult {
    wait_for_record_count_matching(bootstrap, expected, true).await
}

async fn wait_for_at_least_record_count(bootstrap: &str, expected: usize) -> TestResult {
    wait_for_record_count_matching(bootstrap, expected, false).await
}

async fn wait_for_record_count_matching(
    bootstrap: &str,
    expected: usize,
    exact: bool,
) -> TestResult {
    timeout(WAIT, async {
        loop {
            if let Ok(records) =
                krabka_replicator::admin_util::read_all(bootstrap, TOPIC, None).await
            {
                let count = records.len();
                if exact {
                    assert!(
                        count <= expected,
                        "observed duplicate records: {count} > {expected}"
                    );
                }
                if count >= expected {
                    return Ok::<(), io::Error>(());
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!("topic did not reach {expected} records"),
        )
    })??;
    Ok(())
}

async fn read_records(
    bootstrap: &str,
    group_id: &str,
    expected: usize,
    repeated_key: Option<&[u8]>,
) -> TestResult<Vec<ObservedRecord>> {
    let mut consumer = timeout(
        WAIT,
        Consumer::builder()
            .bootstrap(bootstrap)
            .group_id(group_id)
            .client_id("krabka-connect-worker-acceptance")
            .subscribe(vec![TOPIC.to_owned()])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .build(),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "consumer did not start"))??;

    let records = timeout(WAIT, async {
        let mut records = Vec::with_capacity(expected);
        while records.len() < expected
            || repeated_key.is_some_and(|key| {
                records
                    .iter()
                    .filter(|record: &&ObservedRecord| record.key.as_deref() == Some(key))
                    .count()
                    < 2
            })
        {
            for record in consumer.poll(millis(250)).await? {
                records.push(observe(record)?);
            }
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(records)
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "consumer did not receive records"))??;
    consumer.close().await?;
    Ok(records)
}

fn observe(record: ConsumerRecord) -> TestResult<ObservedRecord> {
    let operation = record
        .headers
        .iter()
        .find(|header| header.key == "krabka.pg.operation")
        .and_then(|header| header.value.as_deref())
        .ok_or_else(|| io::Error::other("CDC record is missing operation header"))?;
    Ok(ObservedRecord {
        key: record.key.map(|key| key.to_vec()),
        value: record.value.map(|value| value.to_vec()),
        operation: std::str::from_utf8(operation)?.to_owned(),
    })
}

fn encoded_key(encoder: &PostgresProtoEncoder, id: i64) -> TestResult<Bytes> {
    Ok(encoder.encode_key(&EntityKey {
        table: "public.orders".to_owned(),
        columns: vec![ColumnValue {
            name: "id".to_owned(),
            value: ScalarValue::Int(id),
        }],
    })?)
}
