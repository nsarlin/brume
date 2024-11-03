use std::sync::Arc;
use tokio::sync::Mutex;

use ncclient::{
    concrete::{local::LocalDir, nextcloud::NextcloudFs},
    filesystem::FileSystem,
    Error,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let remote = Arc::new(Mutex::new(FileSystem::new(NextcloudFs::new(
        "http://localhost:8080",
        "admin",
        "admin",
    )?)));
    let local = Arc::new(Mutex::new(FileSystem::new(LocalDir::new("/tmp/test")?)));

    let (remote_diff, local_diff) = {
        let local = Arc::clone(&local);
        let remote = Arc::clone(&remote);

        // Unwrap to propagate panics
        let (remote_diff, local_diff) = tokio::try_join!(
            tokio::spawn(async move {
                let mut remote = remote.lock().await;
                remote.update_vfs().await
            }),
            tokio::spawn(async move {
                let mut local = local.lock().await;
                local.update_vfs().await
            }),
        )
        .unwrap();

        (remote_diff?, local_diff?)
    };

    println!("{local_diff:?}");
    println!("====");
    println!("{remote_diff:?}");

    let reconciled = {
        let remote = remote.lock().await;
        let local = local.lock().await;
        local_diff.reconcile(remote_diff, &local, &remote).await?
    };

    println!("====");
    println!("{reconciled:?}");

    Ok(())
}
