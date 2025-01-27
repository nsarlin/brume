use std::{process::exit, sync::Arc, time::Duration};

use brume::{concrete::local::LocalDir, filesystem::FileSystem};
use env_logger::Builder;

use brume_daemon::{
    daemon::{Daemon, DaemonConfig, ErrorMode},
    protocol::{AnyFsCreationInfo, LocalDirCreationInfo, NextcloudFsCreationInfo},
};
use log::{info, LevelFilter};
use tarpc::context;
use tokio::time::sleep;

#[path = "utils.rs"]
mod utils;

use utils::{
    connect_to_daemon, get_random_port, get_random_sock_name, start_nextcloud, stop_nextcloud,
};

#[tokio::test]
async fn main() {
    let mut logs_builder = Builder::new();
    logs_builder
        .filter_level(LevelFilter::Info)
        .filter(Some("tarpc"), LevelFilter::Error)
        .init();

    // Start daemon
    let sock_name = get_random_sock_name();
    info!("using sock name {sock_name}");
    let config = DaemonConfig::default()
        .with_sync_interval(Duration::from_secs(2))
        .with_error_mode(ErrorMode::Exit)
        .with_sock_name(&sock_name);
    let daemon = Daemon::new(config).unwrap();
    let daemon = Arc::new(daemon);

    let daemon_task = {
        let daemon = daemon.clone();
        tokio::spawn(async move {
            daemon.run().await.unwrap();
        })
    };

    // Create 2 folders that will be synchronized
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();

    info!("dir_a: {:?}", dir_a.path());
    info!("dir_b: {:?}", dir_b.path());

    // Create a nextcloud server
    let nextcloud_port = get_random_port();
    let nextcloud_url = format!("http://localhost:{}", nextcloud_port);
    info!("nextcloud url: {}", &nextcloud_url);

    // Start nextcloud container
    let container = start_nextcloud(nextcloud_port, &nextcloud_url).await;
    info!("container started: {}", container.id());

    // Initiate the first synchro
    let local_a = AnyFsCreationInfo::LocalDir(LocalDirCreationInfo::new(dir_a.path()));
    let remote = AnyFsCreationInfo::Nextcloud(NextcloudFsCreationInfo::new(
        &nextcloud_url,
        "admin",
        "admin",
    ));

    let rpc = connect_to_daemon(&sock_name).await.unwrap();

    rpc.new_synchro(context::current(), local_a, remote.clone(), None)
        .await
        .unwrap()
        .unwrap();

    // Wait a full sync
    sleep(Duration::from_secs(15)).await;
    if !daemon.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    let list = rpc.list_synchros(context::current()).await.unwrap();
    assert_eq!(list.len(), 1);

    // Initiate the second synchro
    let local_b = AnyFsCreationInfo::LocalDir(LocalDirCreationInfo::new(dir_b.path()));

    rpc.new_synchro(context::current(), local_b, remote, None)
        .await
        .unwrap()
        .unwrap();

    // Wait a full sync
    sleep(Duration::from_secs(15)).await;
    if !daemon.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    let list = rpc.list_synchros(context::current()).await.unwrap();
    assert_eq!(list.len(), 2);

    // Check filesystem content
    let concrete_a: LocalDir = LocalDirCreationInfo::new(dir_a.path()).try_into().unwrap();
    let mut fs_a = FileSystem::new(concrete_a);

    let concrete_b: LocalDir = LocalDirCreationInfo::new(dir_b.path()).try_into().unwrap();
    let mut fs_b = FileSystem::new(concrete_b);

    fs_a.diff_vfs().await.unwrap();
    fs_b.diff_vfs().await.unwrap();
    assert!(fs_a.vfs().structural_eq(fs_b.vfs()));

    // Remove a file on one side
    std::fs::remove_file(dir_a.path().to_path_buf().join("Documents/Example.md")).unwrap();

    // Wait for propagation on both fs
    sleep(Duration::from_secs(10)).await;
    if !daemon.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    fs_a.diff_vfs().await.unwrap();
    fs_b.diff_vfs().await.unwrap();
    assert!(fs_a.vfs().structural_eq(fs_b.vfs()));

    // Modify a file
    let content_a = b"This is a test";
    std::fs::write(
        dir_a.path().to_path_buf().join("Templates/Readme.md"),
        content_a,
    )
    .unwrap();

    // Wait for propagation on both fs
    sleep(Duration::from_secs(10)).await;
    if !daemon.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    fs_a.diff_vfs().await.unwrap();
    fs_b.diff_vfs().await.unwrap();
    assert!(fs_a.vfs().structural_eq(fs_b.vfs()));
    let content_b = std::fs::read(dir_a.path().to_path_buf().join("Templates/Readme.md")).unwrap();
    assert_eq!(content_a, content_b.as_slice());

    // Create a dir
    std::fs::create_dir(dir_a.path().to_path_buf().join("testdir")).unwrap();

    // Wait for propagation on both fs
    sleep(Duration::from_secs(10)).await;
    if !daemon.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    fs_a.diff_vfs().await.unwrap();
    fs_b.diff_vfs().await.unwrap();
    assert!(fs_a.vfs().structural_eq(fs_b.vfs()));

    // Create a file
    let content_a = b"Hello, world";
    std::fs::write(
        dir_a.path().to_path_buf().join("testdir/testfile"),
        content_a,
    )
    .unwrap();

    // Wait for propagation on both fs
    sleep(Duration::from_secs(10)).await;
    if !daemon.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    fs_a.diff_vfs().await.unwrap();
    fs_b.diff_vfs().await.unwrap();
    assert!(fs_a.vfs().structural_eq(fs_b.vfs()));
    let content_b = std::fs::read(dir_a.path().to_path_buf().join("testdir/testfile")).unwrap();
    assert_eq!(content_a, content_b.as_slice());

    std::fs::remove_dir_all(dir_a.path().to_path_buf().join("testdir")).unwrap();

    // Wait for propagation on both fs
    sleep(Duration::from_secs(10)).await;
    if !daemon.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    fs_a.diff_vfs().await.unwrap();
    fs_b.diff_vfs().await.unwrap();
    assert!(fs_a.vfs().structural_eq(fs_b.vfs()));

    // Test deletion
    let first_sync = list.keys().collect::<Vec<_>>()[0];
    rpc.delete_synchro(context::current(), *first_sync)
        .await
        .unwrap()
        .unwrap();

    // Wait for deletion
    sleep(Duration::from_secs(5)).await;
    if !daemon.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }
    let list = rpc.list_synchros(context::current()).await.unwrap();
    assert_eq!(list.len(), 1);

    daemon_task.abort();
    daemon_task.await.unwrap_err();
}
