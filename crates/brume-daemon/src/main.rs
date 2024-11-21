use anyhow::{Context, Result};

use brume::{
    concrete::{local::LocalDir, nextcloud::NextcloudFs, ConcreteFsError},
    filesystem::FileSystem,
};

#[tokio::main]
async fn main() -> Result<()> {
    let mut remote = FileSystem::new(
        NextcloudFs::new("http://localhost:8080", "admin", "admin")
            .map_err(ConcreteFsError::from)
            .context("Failed to connect to remote fs")?,
    );
    let mut local =
        FileSystem::new(LocalDir::new("/tmp/test").context("Failed load local directory")?);

    loop {
        local
            .full_sync(&mut remote)
            .await
            .context("Failed to synchronize local and remote dir")?;

        println!("sync done");
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    }
}
