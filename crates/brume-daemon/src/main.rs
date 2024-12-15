use std::sync::Arc;

use anyhow::{anyhow, Result};

use brume_daemon::server::Server as BrumeServer;

use env_logger::Builder;
use log::{error, info, LevelFilter};
use tokio::time;

#[tokio::main]
async fn main() -> Result<()> {
    let mut logs_builder = Builder::new();
    logs_builder
        .filter_level(LevelFilter::Info)
        .filter(Some("tarpc"), LevelFilter::Error)
        .init();

    let server = Arc::new(BrumeServer::new()?);

    // Handle connections from client apps
    {
        let server = server.clone();
        tokio::spawn(async move { server.serve().await });
    }

    // Synchronize all filesystems
    let mut interval = time::interval(time::Duration::from_secs(10));
    loop {
        info!("Starting full sync for all filesystems");
        let synchro_list = server.synchro_list();

        let results = synchro_list.sync_all().await;

        for res in results {
            if let Err(err) = res {
                let wrapped_err = anyhow!(err);
                error!("Failed to synchronize filesystems: {wrapped_err:?}")
            }
        }

        // Wait and update synchro list with any new sync from user
        loop {
            tokio::select! {
                _ = interval.tick() => break,
                _ = server.update_synchro_list() => continue
            }
        }
    }
}
