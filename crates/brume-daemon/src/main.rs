use std::sync::Arc;

use anyhow::Result;

use brume_daemon::daemon::{Daemon, DaemonConfig};

use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<()> {
    let fmt_layer = fmt::layer().compact();
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info,tarpc=error"))
        .unwrap();
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    let daemon = Arc::new(Daemon::new(DaemonConfig::default()).await?);

    daemon.run().await
}
