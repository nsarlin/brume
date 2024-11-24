use anyhow::{Context, Result};

use brume::{
    concrete::{local::LocalDir, nextcloud::NextcloudFs, ConcreteFsError},
    synchro::Synchro,
};

#[tokio::main]
async fn main() -> Result<()> {
    let remote = NextcloudFs::new("http://localhost:8080", "admin", "admin")
        .map_err(ConcreteFsError::from)
        .context("Failed to connect to remote fs")?;
    let local = LocalDir::new("/tmp/test").context("Failed load local directory")?;

    let mut synchro = Synchro::new(local, remote);

    loop {
        synchro
            .full_sync()
            .await
            .context("Failed to synchronize local and remote dir")?;

        println!("sync done");
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    }
}
