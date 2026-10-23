//! Periodic checkpoint task: translates and emits consumer-group offset checkpoints.
//!
//! Reads committed consumer-group offsets from the SOURCE cluster, translates
//! them to TARGET offsets with an [`OffsetSyncStore`], and writes MM2-compatible
//! [`Checkpoint`] records to `<source>.checkpoints.internal` on the TARGET cluster.

use std::collections::BTreeMap;

use bytes::Bytes;
use krabka_client_admin::{AdminClient, groups::ListGroupsOptions};
use krabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use krabka_client_producer::{Acks, Producer, ProducerRecord};
use krabka_units::{
    fmt::Human as _,
    prelude::{Time, TimeExt as _},
};
use tracing::warn;

use crate::{
    config::{ClientResourcePolicy, NamingPolicy, ReplicatorRuntimePolicy},
    error::ReplicatorError,
    ids::{CommittedOffset, PartitionIndex},
    mm2::{Checkpoint, OffsetSync},
    naming::Renamer,
    offset_sync_store::OffsetSyncStore,
    selector::Selector,
};

/// Parameters required to run the checkpoint task.
pub struct CheckpointParams {
    /// Bootstrap address of the source cluster, where consumer-group offsets live.
    pub source_bootstrap: String,
    /// Bootstrap address of the target cluster, where the task writes checkpoints.
    pub target_bootstrap: String,
    /// Alias of the source cluster, used to derive MM2 topic names.
    pub source_alias: String,
    /// Naming policy for the flow. It renames the topic of the checkpoint to the
    /// remote name that a failed-over consumer reads, for MM2
    /// `RemoteClusterUtils` parity.
    pub naming: NamingPolicy,
    /// Selector for which consumer groups to checkpoint.
    pub group_selector: Selector,
    /// Optional TLS/SASL security applied to the target producer and admin.
    pub security: Option<krabka_client_core::security::ClientSecurity>,
}

/// One translation pass: reads source group offsets, translates them with
/// `store`, and writes [`Checkpoint`] records to the target checkpoints topic.
///
/// # Errors
/// Returns [`ReplicatorError`] on topic-ensure, admin-connect, or produce failures.
#[tracing::instrument(
    level = "info",
    skip_all,
    fields(source_alias = %params.source_alias, groups = tracing::field::Empty),
    err,
)]
pub async fn run_once(
    params: &CheckpointParams,
    store: &OffsetSyncStore,
) -> Result<(), ReplicatorError> {
    run_once_with_policy(params, store, ClientResourcePolicy::default()).await
}

/// Run one checkpoint pass with the deployment's client resource policy.
///
/// # Errors
///
/// Returns [`ReplicatorError`] on topic-ensure, admin-connect, or produce
/// failures.
pub async fn run_once_with_policy(
    params: &CheckpointParams,
    store: &OffsetSyncStore,
    client_resource_policy: ClientResourcePolicy,
) -> Result<(), ReplicatorError> {
    run_once_with_runtime_policy(
        params,
        store,
        client_resource_policy,
        &ReplicatorRuntimePolicy::default(),
    )
    .await
}

async fn run_once_with_runtime_policy(
    params: &CheckpointParams,
    store: &OffsetSyncStore,
    client_resource_policy: ClientResourcePolicy,
    runtime_policy: &ReplicatorRuntimePolicy,
) -> Result<(), ReplicatorError> {
    let checkpoint_topic = Checkpoint::topic_name(&params.source_alias);

    // 1. Ensure the checkpoints topic exists on the target.
    crate::admin_util::ensure_topic_with_runtime_policy(
        &params.target_bootstrap,
        &checkpoint_topic,
        1,
        params.security.clone(),
        client_resource_policy,
        runtime_policy,
        runtime_policy.internal_topic_replication_factor,
    )
    .await
    .map_err(|e| ReplicatorError::Client(format!("ensure checkpoints topic: {e}")))?;

    // 2. Connect to the source cluster to list group offsets.
    let mut admin = AdminClient::connect_with_options(
        std::slice::from_ref(&params.source_bootstrap),
        krabka_client_core::ConnectionOptions {
            dns_timeout: krabka_client_core::ClientDnsTimeout::new(
                runtime_policy.client_dns_timeout,
            )
            .map_err(ReplicatorError::Client)?,
            socket_connection_setup_timeout: runtime_policy.client_connect_timeout,
            socket_connection_setup_timeout_max: runtime_policy.client_connect_timeout,
            request_timeout: runtime_policy.client_request_timeout,
            client_id: "krabka-operator".to_owned(),
            dispatch_queue_capacity: client_resource_policy.dispatch_queue_capacity,
            frame_max: client_resource_policy.frame_max,
            security: None, // source cluster: target security does not apply
            ..Default::default()
        },
    )
    .await
    .map_err(|e| ReplicatorError::Client(format!("source admin connect: {e}")))?;

    // 3. List and filter consumer groups; skip our own internal groups.
    let all_groups = admin
        .list_groups(&ListGroupsOptions::default())
        .await
        .map_err(|e| ReplicatorError::Client(format!("list_groups: {e}")))?
        .all()
        .map_err(|error| ReplicatorError::Client(format!("list_groups: {error:?}")))?;

    let groups: Vec<String> = all_groups
        .into_iter()
        .map(|group| group.group_id)
        .filter(|g| !g.starts_with("krabka-replicator-"))
        .filter(|g| params.group_selector.matches(g))
        .collect();
    tracing::Span::current().record("groups", groups.len());

    // 4. Build a producer to write checkpoint records to the target.
    let producer = build_producer(
        &params.target_bootstrap,
        params.security.clone(),
        client_resource_policy,
    )
    .await
    .map_err(|e| ReplicatorError::Client(format!("build target producer: {e}")))?;

    // 5. For each matched group, translate committed offsets and produce checkpoints.
    // The checkpoint's topic is the REMOTE (renamed) topic a failed-over consumer
    // reads on the target, matching MM2's `renameTopicPartition`. Offset-sync
    // translation still keys on the SOURCE topic name.
    let renamer = Renamer::new(params.naming, &params.source_alias);
    for group in &groups {
        let offsets = match admin.list_consumer_group_offsets(group).await {
            Ok(o) => o,
            Err(e) => {
                warn!(group = %group, error = %e, "list_consumer_group_offsets failed; skipping group");
                continue;
            }
        };

        for ((topic, partition), committed) in offsets {
            // `offsets` comes from the cross-crate admin client as raw
            // `(i32, i64)`; wrap at this boundary so the translation call and
            // the checkpoint fields are type-checked.
            let partition = PartitionIndex(partition);
            let committed = CommittedOffset(committed);
            let Some(downstream) = store.translate(&topic, partition, committed) else {
                continue;
            };

            let checkpoint = Checkpoint {
                group: group.clone(),
                topic: renamer.target_name(&topic),
                partition,
                // A group's committed source offset is the upstream side of the
                // checkpoint; the translated target offset is the downstream.
                upstream: committed.into(),
                downstream,
                metadata: String::new(),
            };

            let _rx = producer
                .enqueue(ProducerRecord {
                    topic: checkpoint_topic.clone(),
                    partition: None,
                    key: Some(Bytes::from(checkpoint.key_bytes())),
                    value: Some(Bytes::from(checkpoint.value_bytes())),
                    headers: Vec::new(),
                    timestamp_ms: None,
                })
                .await
                .map_err(|error| ReplicatorError::Client(format!("checkpoint produce: {error}")))?;
        }
    }

    // 6. Flush to ensure all checkpoint records are durably written.
    producer
        .flush()
        .await
        .map_err(|e| ReplicatorError::Client(format!("producer flush: {e}")))?;

    Ok(())
}

/// Background task that incrementally updates an [`OffsetSyncStore`] from the
/// target cluster's offset-syncs topic and then calls [`run_once`].
pub struct CheckpointTask {
    handle: tokio::task::JoinHandle<()>,
    shutdown: tokio::sync::watch::Sender<bool>,
}

impl CheckpointTask {
    /// Start the checkpoint loop with the given interval.
    ///
    /// Each cycle:
    /// 1. Captures the read-committed end of `mm2-offset-syncs.<source>.internal`.
    /// 2. Streams only records since the preceding cycle into a retained store.
    /// 3. Calls [`run_once`] to write translated checkpoints.
    ///
    /// The loop logs an error in any cycle as a warning and continues until
    /// shutdown.
    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(source_alias = %params.source_alias, interval = %interval.human()),
    )]
    pub fn start(params: CheckpointParams, interval: Time) -> Self {
        Self::start_with_policy(params, interval, ClientResourcePolicy::default())
    }

    /// Start with the deployment's client resource policy.
    #[must_use]
    pub fn start_with_policy(
        params: CheckpointParams,
        interval: Time,
        client_resource_policy: ClientResourcePolicy,
    ) -> Self {
        Self::start_with_runtime_policy(
            params,
            interval,
            client_resource_policy,
            ReplicatorRuntimePolicy::default(),
        )
    }

    pub(crate) fn start_with_runtime_policy(
        params: CheckpointParams,
        interval: Time,
        client_resource_policy: ClientResourcePolicy,
        runtime_policy: ReplicatorRuntimePolicy,
    ) -> Self {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        let handle = tokio::spawn(async move {
            let offset_syncs_topic = OffsetSync::topic_name(&params.source_alias);
            let mut reader: Option<OffsetSyncReader> = None;
            // Retain every historical anchor for the lifetime of the task. An
            // arbitrary cap could make translation unsafe for a lagging group.
            let mut store = OffsetSyncStore::default();
            loop {
                let cycle = async {
                    if reader.is_none() {
                        reader = Some(
                            OffsetSyncReader::connect(
                                &params.target_bootstrap,
                                offset_syncs_topic.clone(),
                                params.security.clone(),
                                client_resource_policy,
                            )
                            .await?,
                        );
                    }
                    let active = reader.as_mut().expect("reader connected above");
                    active.read_cycle(&mut store, &runtime_policy).await?;
                    run_once_with_runtime_policy(
                        &params,
                        &store,
                        client_resource_policy,
                        &runtime_policy,
                    )
                    .await
                    .map_err(|error| error.to_string())
                };
                let result = tokio::select! {
                    result = cycle => Some(result),
                    _ = shutdown_rx.changed() => None,
                };
                match result {
                    None => break,
                    Some(Err(error)) => {
                        warn!(%error, "checkpoint cycle failed; retrying next interval");
                    }
                    Some(Ok(())) => {}
                }

                // Wait for the next interval or a shutdown signal.
                tokio::select! {
                    () = tokio::time::sleep(interval.to_std()) => {}
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }

            if let Some(reader) = reader {
                let _ = reader.close().await;
            }
        });

        Self {
            handle,
            shutdown: shutdown_tx,
        }
    }

    /// Signal the task to stop and await its completion.
    // cargo-mutants: shutdown signal/join not asserted by unit tests
    #[cfg_attr(test, mutants::skip)]
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(true);
        let _ = self.handle.await;
    }

    /// Abort the task without writing another checkpoint.
    pub async fn crash(self) {
        self.handle.abort();
        let _ = self.handle.await;
    }
}

/// A long-lived, manually assigned consumer and its next offsets.
///
/// Every cycle snapshots a finite read-committed end offset. Records appended
/// after that snapshot remain for the next cycle, even when every poll is
/// nonempty. Cursors are intentionally in memory: after a process restart the
/// empty translation store is reconstructed from the earliest retained sync.
struct OffsetSyncReader {
    admin: AdminClient,
    topic_id: Option<String>,
    consumer: Consumer,
    topic: String,
    cursors: BTreeMap<(String, i32), i64>,
}

impl OffsetSyncReader {
    async fn connect(
        bootstrap: &str,
        topic: String,
        security: Option<krabka_client_core::security::ClientSecurity>,
        policy: ClientResourcePolicy,
    ) -> Result<Self, String> {
        let admin = AdminClient::connect_with_options(
            &[bootstrap.to_owned()],
            krabka_client_core::ConnectionOptions {
                dispatch_queue_capacity: policy.dispatch_queue_capacity,
                frame_max: policy.frame_max,
                security: security.clone().map(Box::new),
                ..Default::default()
            },
        )
        .await
        .map_err(|error| error.to_string())?;
        let builder = Consumer::builder()
            .bootstrap(bootstrap)
            .client_id("krabka-replicator-offset-sync-reader")
            .dispatch_queue_capacity(policy.dispatch_queue_capacity.get())
            .frame_max(policy.frame_max.size())
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .allow_auto_create_topics(false)
            .isolation_level(IsolationLevel::ReadCommitted);
        let consumer = match security {
            Some(security) => builder.security(security).build().await,
            None => builder.build().await,
        }
        .map_err(|error| error.to_string())?;
        Ok(Self {
            admin,
            topic_id: None,
            consumer,
            topic,
            cursors: BTreeMap::new(),
        })
    }

    async fn read_cycle(
        &mut self,
        store: &mut OffsetSyncStore,
        runtime_policy: &ReplicatorRuntimePolicy,
    ) -> Result<usize, String> {
        // Offsets alone cannot identify a recreated topic whose new log has
        // already grown past our old cursor. Invalidate both anchors and cursor
        // when Kafka reports a different topic UUID.
        let metadata = self
            .admin
            .metadata(&[&self.topic])
            .await
            .map_err(|error| error.to_string())?;
        let entry = metadata
            .topics
            .into_iter()
            .find(|entry| entry.name == self.topic);
        if let Some(error) = entry.as_ref().and_then(|entry| entry.error.as_ref())
            && error.code != 3
        {
            return Err(format!("{error:?}"));
        }
        let topic_id = entry
            .as_ref()
            .and_then(|entry| entry.topic_id)
            .map(|id| id.to_string());
        if topic_id != self.topic_id {
            // Drop assignment ownership as well as cursors: seeking the same
            // numeric offset must not retain an old topic's buffered fetches.
            self.consumer
                .unsubscribe()
                .await
                .map_err(|error| error.to_string())?;
            store.clear();
            self.cursors.clear();
            self.topic_id = topic_id;
        }
        let partitions = match self.consumer.partitions_for(&self.topic).await {
            Ok(partitions) => partitions
                .into_iter()
                .map(|partition| (self.topic.clone(), partition.partition))
                .collect::<Vec<_>>(),
            Err(error) if crate::admin_util::is_unknown_topic_error(&error.to_string()) => {
                Vec::new()
            }
            Err(error) => return Err(error.to_string()),
        };
        self.consumer
            .assign(&partitions)
            .await
            .map_err(|error| error.to_string())?;
        if partitions.is_empty() {
            store.clear();
            self.cursors.clear();
            self.topic_id = None;
            return Ok(0);
        }

        let beginnings = self
            .consumer
            .beginning_offsets(&partitions)
            .await
            .map_err(|error| error.to_string())?;
        // Under read-committed isolation these are last-stable offsets, not
        // moving high watermarks, so open transactions cannot strand a cycle.
        let targets = self
            .consumer
            .end_offsets(&partitions)
            .await
            .map_err(|error| error.to_string())?;

        let must_rebuild = beginnings.iter().any(|(partition, beginning)| {
            let target = targets.get(partition).copied().unwrap_or(*beginning);
            self.cursors
                .get(partition)
                .is_some_and(|cursor| cursor < beginning || *cursor > target)
        });
        if must_rebuild {
            store.clear();
            self.cursors.clear();
        }

        for ((topic, partition), beginning) in &beginnings {
            let cursor = self
                .cursors
                .get(&(topic.clone(), *partition))
                .copied()
                .unwrap_or(*beginning);
            let target = targets
                .get(&(topic.clone(), *partition))
                .copied()
                .unwrap_or(*beginning);
            let next = if cursor < *beginning || cursor > target {
                *beginning
            } else {
                cursor
            };
            self.consumer
                .seek(topic, *partition, next)
                .await
                .map_err(|error| error.to_string())?;
        }

        let mut ingested = 0;
        while !self.reached(&partitions, &targets).await? {
            let records = self
                .consumer
                .poll(runtime_policy.internal_drain_poll_timeout)
                .await
                .map_err(|error| error.to_string())?;
            for record in records {
                let key = (record.topic.clone(), record.partition);
                if record.offset >= targets.get(&key).copied().unwrap_or(0) {
                    continue;
                }
                if let (Some(key), Some(value)) = (record.key, record.value) {
                    match OffsetSync::from_bytes(&key, &value) {
                        Ok(sync) => {
                            store.ingest(sync);
                            ingested += 1;
                        }
                        Err(error) => {
                            warn!(%error, "failed to decode offset-sync record; skipping");
                        }
                    }
                }
            }
        }

        // A fetch can include records appended after the snapshot. Rewind to
        // the captured boundary so those records are visible next cycle.
        for ((topic, partition), target) in &targets {
            self.consumer
                .seek(topic, *partition, *target)
                .await
                .map_err(|error| error.to_string())?;
            self.cursors.insert((topic.clone(), *partition), *target);
        }
        Ok(ingested)
    }

    async fn reached(
        &self,
        partitions: &[(String, i32)],
        targets: &std::collections::HashMap<(String, i32), i64>,
    ) -> Result<bool, String> {
        for (topic, partition) in partitions {
            let position = self
                .consumer
                .position(topic, *partition)
                .await
                .map_err(|error| error.to_string())?;
            if position
                < targets
                    .get(&(topic.clone(), *partition))
                    .copied()
                    .unwrap_or(0)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn close(self) -> Result<(), String> {
        self.consumer
            .close()
            .await
            .map_err(|error| error.to_string())
    }
}

/// Build a non-idempotent producer with `acks=All` that targets the given
/// bootstrap.
async fn build_producer(
    bootstrap: &str,
    security: Option<krabka_client_core::security::ClientSecurity>,
    client_resource_policy: ClientResourcePolicy,
) -> Result<Producer, krabka_client_producer::ProducerError> {
    let builder = Producer::builder()
        .bootstrap(bootstrap)
        .dispatch_queue_capacity(client_resource_policy.dispatch_queue_capacity.get())
        .frame_max(client_resource_policy.frame_max.size())
        .enable_idempotence(false)
        .acks(Acks::All);
    match security {
        Some(s) => builder.security(s).build().await,
        None => builder.build().await,
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{
        ids::{DownstreamOffset, UpstreamOffset},
        mm2::{Checkpoint, OffsetSync},
        offset_sync_store::OffsetSyncStore,
    };

    async fn produce_sync(bootstrap: &str, topic: &str, offset: i64) {
        let sync = OffsetSync {
            topic: "orders".into(),
            partition: PartitionIndex(0),
            upstream: UpstreamOffset(offset),
            downstream: DownstreamOffset(offset),
        };
        crate::test_util::produce(bootstrap, topic, &sync.key_bytes(), &sync.value_bytes()).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reader_streams_only_offsets_appended_since_previous_cycle() {
        let dir = tempfile::TempDir::new().unwrap();
        let broker = krabka_broker::Broker::start(krabka_broker::BrokerConfig::for_tests(
            dir.path().to_path_buf(),
        ))
        .await
        .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let topic = OffsetSync::topic_name("us-east");
        crate::test_util::create_topic(&bootstrap, &topic, 1).await;
        for offset in 0..3 {
            produce_sync(&bootstrap, &topic, offset).await;
        }

        let mut reader = OffsetSyncReader::connect(
            &bootstrap,
            topic.clone(),
            None,
            ClientResourcePolicy::default(),
        )
        .await
        .unwrap();
        let mut store = OffsetSyncStore::default();
        let policy = ReplicatorRuntimePolicy::default();
        assert2::assert!(reader.read_cycle(&mut store, &policy).await.unwrap() == 3);

        for offset in 3..5 {
            produce_sync(&bootstrap, &topic, offset).await;
        }
        assert2::assert!(reader.read_cycle(&mut store, &policy).await.unwrap() == 2);
        assert2::assert!(
            store.translate("orders", PartitionIndex(0), CommittedOffset(4))
                == Some(DownstreamOffset(4))
        );

        // A restart owns no persisted cursor or translation state and safely
        // reconstructs both from all retained records.
        reader.close().await.unwrap();
        let mut restarted =
            OffsetSyncReader::connect(&bootstrap, topic, None, ClientResourcePolicy::default())
                .await
                .unwrap();
        let mut rebuilt = OffsetSyncStore::default();
        assert2::assert!(restarted.read_cycle(&mut rebuilt, &policy).await.unwrap() == 5);
        restarted.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reader_rebuilds_after_topic_recreation_with_overlapping_offsets() {
        let dir = tempfile::TempDir::new().unwrap();
        let broker = krabka_broker::Broker::start(krabka_broker::BrokerConfig::for_tests(
            dir.path().to_path_buf(),
        ))
        .await
        .unwrap();
        let bootstrap = broker.listen_addr().to_string();
        let topic = OffsetSync::topic_name("recreated");
        crate::test_util::create_topic(&bootstrap, &topic, 1).await;
        produce_sync(&bootstrap, &topic, 100).await;
        let mut reader = OffsetSyncReader::connect(
            &bootstrap,
            topic.clone(),
            None,
            ClientResourcePolicy::default(),
        )
        .await
        .unwrap();
        let mut store = OffsetSyncStore::default();
        let policy = ReplicatorRuntimePolicy::default();
        assert2::assert!(reader.read_cycle(&mut store, &policy).await.unwrap() == 1);
        store.ingest(OffsetSync {
            topic: "removed".into(),
            partition: PartitionIndex(0),
            upstream: UpstreamOffset(0),
            downstream: DownstreamOffset(999),
        });
        reader
            .admin
            .delete_topics(
                &[&topic],
                krabka_client_admin::topics::TopicMutationOptions::default(),
            )
            .await
            .unwrap();
        crate::test_util::create_topic(&bootstrap, &topic, 1).await;
        for offset in 0..3 {
            produce_sync(&bootstrap, &topic, offset).await;
        }
        assert2::assert!(reader.read_cycle(&mut store, &policy).await.unwrap() == 3);
        assert2::assert!(
            store
                .translate("removed", PartitionIndex(0), CommittedOffset(0))
                .is_none()
        );
        reader.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reader_treats_a_missing_topic_as_an_empty_cycle() {
        let dir = tempfile::TempDir::new().unwrap();
        let broker = krabka_broker::Broker::start(krabka_broker::BrokerConfig::for_tests(
            dir.path().to_path_buf(),
        ))
        .await
        .unwrap();
        let mut reader = OffsetSyncReader::connect(
            &broker.listen_addr().to_string(),
            "missing-offset-syncs".into(),
            None,
            ClientResourcePolicy::default(),
        )
        .await
        .unwrap();
        assert2::assert!(
            reader
                .read_cycle(
                    &mut OffsetSyncStore::default(),
                    &ReplicatorRuntimePolicy::default()
                )
                .await
                .unwrap()
                == 0
        );
        reader.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writes_translated_checkpoints() {
        let s_dir = tempfile::TempDir::new().unwrap();
        let t_dir = tempfile::TempDir::new().unwrap();
        let source = krabka_broker::Broker::start(krabka_broker::BrokerConfig::for_tests(
            s_dir.path().to_path_buf(),
        ))
        .await
        .unwrap();
        let target = krabka_broker::Broker::start(krabka_broker::BrokerConfig::for_tests(
            t_dir.path().to_path_buf(),
        ))
        .await
        .unwrap();
        let sb = source.listen_addr().to_string();
        let tb = target.listen_addr().to_string();

        crate::test_util::create_topic(&sb, "orders", 1).await;
        for _ in 0..200 {
            crate::test_util::produce(&sb, "orders", b"k", b"v").await;
        }
        crate::test_util::commit_group(&sb, "g", "orders").await;

        let mut syncs = OffsetSyncStore::default();
        syncs.ingest(OffsetSync {
            topic: "orders".into(),
            partition: PartitionIndex(0),
            upstream: UpstreamOffset(0),
            downstream: DownstreamOffset(0),
        });
        syncs.ingest(OffsetSync {
            topic: "orders".into(),
            partition: PartitionIndex(0),
            upstream: UpstreamOffset(200),
            downstream: DownstreamOffset(165),
        });

        run_once(
            &CheckpointParams {
                source_bootstrap: sb,
                target_bootstrap: tb.clone(),
                source_alias: "us-east".into(),
                naming: NamingPolicy::Default,
                group_selector: Selector::compile(&["g".into()], &[]).unwrap(),
                security: None,
            },
            &syncs,
        )
        .await
        .unwrap();

        let raw = crate::admin_util::read_last_value_for_key(
            &tb,
            &Checkpoint::topic_name("us-east"),
            b"",
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert2::assert!(!raw.is_empty());
    }
}
