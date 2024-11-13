use std::sync::Arc;
use tokio::sync::Mutex;

use ncclient::{
    concrete::{local::LocalDir, nextcloud::NextcloudFs, ConcreteFsError},
    filesystem::FileSystem,
    Error,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let remote = Arc::new(Mutex::new(FileSystem::new(
        NextcloudFs::new("http://localhost:8080", "admin", "admin")
            .map_err(ConcreteFsError::from)?,
    )));
    let local = Arc::new(Mutex::new(FileSystem::new(LocalDir::new("/tmp/test")?)));

    loop {
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

        let (local_applied, remote_applied) = {
            let local = local.lock().await;
            let remote = remote.lock().await;
            local
                .apply_updates_list_concrete(&remote, reconciled)
                .await?
        };

        for applied in local_applied.into_iter() {
            let mut local = local.lock().await;
            local.vfs_mut().apply_update(applied)?;
        }

        for applied in remote_applied.into_iter() {
            let mut remote = remote.lock().await;
            remote.vfs_mut().apply_update(applied)?;
        }

        println!("sync done");
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    }
}
