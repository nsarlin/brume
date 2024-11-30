use std::sync::Arc;

use anyhow::Result;

use brume_daemon::BRUME_SOCK_NAME;

use brume::synchro::Synchronizable;

use futures::future::try_join_all;
use server::Server as BrumeServer;
use tokio::time;

mod daemon;
mod server;

#[tokio::main]
async fn main() -> Result<()> {
    let server = Arc::new(BrumeServer::new()?);

    // Handle connections from client apps
    {
        let server = server.clone();
        tokio::spawn(async move { server.serve().await });
    }

    // Synchronize all filesystems
    let mut interval = time::interval(time::Duration::from_secs(10));
    loop {
        let daemon = server.daemon();
        let synchro_list = daemon.synchro_list();

        let synchro = synchro_list.read().await;
        let synchro_fut = synchro
            .values()
            .map(|sync| async { sync.lock().await.full_sync().await });

        try_join_all(synchro_fut).await?;
        interval.tick().await;
    }
}
