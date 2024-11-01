use std::sync::Arc;

use ncclient::{
    concrete::{local::LocalDir, remote::RemoteFs, ConcreteFS},
    vfs::{VfsNodePatch, VirtualPath},
    Error,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let remote = Arc::new(RemoteFs::new("http://localhost:8080", "admin", "admin")?);
    let local = Arc::new(LocalDir::new("/tmp/test")?);

    let (vfs_remote, vfs_local) = {
        let local = local.clone();
        let remote = remote.clone();

        // Unwrap to propagate panics
        let (vfs_remote, vfs_local) = tokio::try_join!(
            tokio::spawn(async move { remote.load_virtual().await }),
            tokio::spawn(async move { local.load_virtual().await }),
        )
        .unwrap();

        (vfs_remote?, vfs_local?)
    };

    println!("{vfs_local:?}");

    let remote_diff: VfsNodePatch = vfs_remote.root().to_created_diff(VirtualPath::root());
    let local_diff: VfsNodePatch = vfs_local.root().to_created_diff(VirtualPath::root());

    println!("{local_diff:?}");
    println!("====");
    println!("{remote_diff:?}");

    let reconciled = local_diff
        .reconcile(
            &remote_diff,
            &vfs_local,
            &vfs_remote,
            local.as_ref(),
            remote.as_ref(),
        )
        .await?;

    println!("====");
    println!("{reconciled:?}");

    Ok(())
}
