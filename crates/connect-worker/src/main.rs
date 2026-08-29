use clap::Parser as _;
use krabka_connect_worker::WorkerConfig;
use tracing_subscriber::{Layer as _, layer::SubscriberExt as _, util::SubscriberInitExt as _};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    rustls_provider();
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "krabka_connect_worker=info,info".into());
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(filter))
        .init();
    krabka_connect_worker::run(WorkerConfig::parse()).await
}

fn rustls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}
