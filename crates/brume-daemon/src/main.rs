use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::sync::Mutex;

use brume::{
    concrete::{local::LocalDir, nextcloud::NextcloudFs, ConcreteFsError},
    filesystem::FileSystem,
};

#[tokio::main]
async fn main() -> Result<()> {
    let remote = Arc::new(Mutex::new(FileSystem::new(
        NextcloudFs::new("http://localhost:8080", "admin", "admin")
            .map_err(ConcreteFsError::from)
            .context("Failed to connect to remote fs")?,
    )));
    let local = Arc::new(Mutex::new(FileSystem::new(
        LocalDir::new("/tmp/test").context("Failed load local directory")?,
    )));

    loop {
        let (local_diff, remote_diff) = {
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

            (
                local_diff.context("Failed to reload local vfs")?,
                remote_diff.context("Failed to reload remote vfs")?,
            )
        };

        println!("local:\n{local_diff:?}");
        println!("====");
        println!("remote:\n{remote_diff:?}");

        let reconciled = {
            let local = local.lock().await;
            let remote = remote.lock().await;

            local_diff
                .reconcile(remote_diff, &local, &remote)
                .await
                .context("Failed to reconcile updates between VFS")?
        };

        println!("====");
        println!("reconciled:\n{reconciled:?}");

        let (local_applied, remote_applied) = {
            let local = local.lock().await;
            let remote = remote.lock().await;
            local
                .apply_updates_list_concrete(&remote, reconciled)
                .await
                .context("Failed to apply updates to concrete dir")?
        };
        println!("updates applied on concrete");

        for applied in local_applied.into_iter() {
            let mut local = local.lock().await;
            local
                .vfs_mut()
                .apply_update(applied)
                .context("Failed to apply update to local vfs")?;
        }

        for applied in remote_applied.into_iter() {
            let mut remote = remote.lock().await;
            remote
                .vfs_mut()
                .apply_update(applied)
                .context("Failed to apply update to remote vfs")?;
        }

        println!("sync done");
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    }
}
