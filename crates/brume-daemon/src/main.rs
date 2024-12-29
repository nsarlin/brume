use std::sync::Arc;

use anyhow::Result;

use brume_daemon::daemon::{Daemon, DaemonConfig};

use env_logger::Builder;
use log::LevelFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let mut logs_builder = Builder::new();
    logs_builder
        .filter_level(LevelFilter::Info)
        .filter(Some("tarpc"), LevelFilter::Error)
        .init();

    let daemon = Arc::new(Daemon::new(DaemonConfig::default())?);

    daemon.run().await
}
