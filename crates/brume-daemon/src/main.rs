use std::sync::Arc;

use anyhow::Result;

use brume_daemon::server::Server as BrumeServer;

use env_logger::Builder;
use log::LevelFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let mut logs_builder = Builder::new();
    logs_builder
        .filter_level(LevelFilter::Info)
        .filter(Some("tarpc"), LevelFilter::Error)
        .init();

    let server = Arc::new(BrumeServer::new()?);

    server.run().await
}
