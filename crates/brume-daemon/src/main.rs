use std::sync::Arc;

use anyhow::{anyhow, Result};

use brume_daemon::BRUME_SOCK_NAME;

use brume::synchro::Synchronizable;

use env_logger::Builder;
use futures::future::join_all;
use log::{error, info, LevelFilter};
use server::Server as BrumeServer;
use tokio::time;

mod daemon;
mod server;

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
        let daemon = server.daemon();
        let synchro_list = daemon.synchro_list();

        let synchro = synchro_list.read().await;
        let synchro_fut = synchro
            .values()
            .map(|sync| async { sync.lock().await.full_sync().await });

        let results = join_all(synchro_fut).await;
        for res in results {
            if let Err(err) = res {
                let wrapped_err = anyhow!(err);
                error!("Failed to synchronize filesystems: {wrapped_err:?}")
            }
        }
        info!("Full sync done");
        interval.tick().await;
    }
}
