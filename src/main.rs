use std::sync::Arc;
use tokio::sync::Mutex;

use ncclient::{
    concrete::{local::LocalDir, nextcloud::NextcloudFs, ConcreteFsError},
    filesystem::FileSystem,
    update::{ReconciledUpdate, UpdateTarget},
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

        for update in reconciled.into_iter() {
            match update {
                ReconciledUpdate::Applicable(update) => match update.target() {
                    UpdateTarget::SelfFs => {
                        let mut local = local.lock().await;
                        let remote = remote.lock().await;
                        let applied = local.apply_update_concrete(&remote, update.into()).await?;
                        local.vfs_mut().apply_update(applied)?;
                    }
                    UpdateTarget::OtherFs => {
                        let local = local.lock().await;
                        let mut remote = remote.lock().await;
                        let applied = remote.apply_update_concrete(&local, update.into()).await?;
                        remote.vfs_mut().apply_update(applied)?;
                    }
                },
                ReconciledUpdate::Conflict(path) => println!("WARNING: conflict on {path:?}"),
            }
        }

        println!("sync done");
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    }
}
