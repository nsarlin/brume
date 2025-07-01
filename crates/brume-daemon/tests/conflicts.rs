use std::{process::exit, sync::Arc, time::Duration};

use brume::{
    concrete::local::LocalDir, filesystem::FileSystem, synchro::SynchroSide, vfs::VirtualPathBuf,
};
use tracing_flame::FlameLayer;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use brume_daemon::{
    daemon::{Daemon, DaemonConfig},
    db::DatabaseConfig,
};
use brume_daemon_proto::{
    AnyFsCreationInfo, LocalDirCreationInfo, NextcloudFsCreationInfo, SynchroStatus,
    config::ErrorMode,
};
use tarpc::context;
use tracing::info;

#[path = "utils.rs"]
mod utils;

use utils::{
    connect_to_daemon, get_random_port, get_random_sock_name, start_nextcloud, stop_nextcloud,
    wait_full_sync,
};

#[tokio::test]
async fn main() {
    let fmt_layer = fmt::layer().compact();
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info,tarpc=error"))
        .unwrap();

    let (flame_layer, _guard) = FlameLayer::with_file("./conflicts-tracing.folded").unwrap();

    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .with(flame_layer)
        .init();

    // Start daemon
    let sock_name = get_random_sock_name();
    info!("using sock name {sock_name}");
    let sync_interval = Duration::from_secs(2);
    let config = DaemonConfig::default()
        .with_sync_interval(sync_interval)
        .with_error_mode(ErrorMode::Exit)
        .with_db_config(DatabaseConfig::InMemory)
        .with_sock_name(&sock_name);
    let daemon = Daemon::new(config).await.unwrap();
    let daemon = Arc::new(daemon);

    let daemon_task = daemon.spawn().await;

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
    wait_full_sync(sync_interval, &rpc).await;
    if !daemon.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    let list = rpc.list_synchros(context::current()).await.unwrap();
    assert_eq!(list.len(), 1);

    let sync_id_a = *list.keys().next().unwrap();

    // Initiate the second synchro
    let local_b = AnyFsCreationInfo::LocalDir(LocalDirCreationInfo::new(dir_b.path()));

    rpc.new_synchro(context::current(), local_b, remote, None)
        .await
        .unwrap()
        .unwrap();

    // Wait a full sync
    wait_full_sync(sync_interval, &rpc).await;
    if !daemon.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    let list = rpc.list_synchros(context::current()).await.unwrap();
    assert_eq!(list.len(), 2);

    let sync_id_b = *list.keys().find(|k| **k != sync_id_a).unwrap();

    // Check filesystem content
    let concrete_a: LocalDir = LocalDirCreationInfo::new(dir_a.path()).try_into().unwrap();
    let mut fs_a = FileSystem::new(concrete_a);

    let concrete_b: LocalDir = LocalDirCreationInfo::new(dir_b.path()).try_into().unwrap();
    let mut fs_b = FileSystem::new(concrete_b);

    fs_a.diff_vfs().await.unwrap();
    fs_b.diff_vfs().await.unwrap();
    assert!(fs_a.vfs().structural_eq(fs_b.vfs()));

    // Pause the first synchro
    rpc.pause_synchro(context::current(), sync_id_a)
        .await
        .unwrap()
        .unwrap();

    // Modify a file on both side
    let content_a = b"This is a test";
    std::fs::write(
        dir_a.path().to_path_buf().join("Templates/Readme.md"),
        content_a,
    )
    .unwrap();

    let content_b = b"This is a toast";
    std::fs::write(
        dir_b.path().to_path_buf().join("Templates/Readme.md"),
        content_b,
    )
    .unwrap();

    // Wait for propagation on the first fs
    wait_full_sync(sync_interval, &rpc).await;
    if !daemon.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    // Resume the synchro
    rpc.resume_synchro(context::current(), sync_id_a)
        .await
        .unwrap()
        .unwrap();

    // Wait for another propagation
    wait_full_sync(sync_interval, &rpc).await;
    if !daemon.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    let list = rpc.list_synchros(context::current()).await.unwrap();
    let sync_a = list.get(&sync_id_a).unwrap();
    let sync_b = list.get(&sync_id_b).unwrap();
    assert_eq!(sync_a.status(), SynchroStatus::Conflict);
    assert_eq!(sync_b.status(), SynchroStatus::Ok);

    rpc.resolve_conflict(
        context::current(),
        sync_id_a,
        VirtualPathBuf::new("/Templates/Readme.md").unwrap(),
        SynchroSide::Local,
    )
    .await
    .unwrap()
    .unwrap();

    // Wait for a full propagation
    wait_full_sync(sync_interval, &rpc).await;
    if !daemon.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    fs_a.diff_vfs().await.unwrap();
    fs_b.diff_vfs().await.unwrap();
    assert!(fs_a.vfs().structural_eq(fs_b.vfs()));
    let content_b = std::fs::read(dir_a.path().to_path_buf().join("Templates/Readme.md")).unwrap();
    assert_eq!(content_a, content_b.as_slice());

    let list = rpc.list_synchros(context::current()).await.unwrap();
    let sync_a = list.get(&sync_id_a).unwrap();
    let sync_b = list.get(&sync_id_b).unwrap();
    assert_eq!(sync_a.status(), SynchroStatus::Ok);
    assert_eq!(sync_b.status(), SynchroStatus::Ok);

    // Create a second conflict
    rpc.pause_synchro(context::current(), sync_id_a)
        .await
        .unwrap()
        .unwrap();

    let content_a = b"This is a second test";
    std::fs::write(
        dir_a.path().to_path_buf().join("Templates/Readme.md"),
        content_a,
    )
    .unwrap();

    let content_b = b"This is a second toast";
    std::fs::write(
        dir_b.path().to_path_buf().join("Templates/Readme.md"),
        content_b,
    )
    .unwrap();

    // Wait for propagation on the first fs
    wait_full_sync(sync_interval, &rpc).await;
    if !daemon.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    // Resume the synchro
    rpc.resume_synchro(context::current(), sync_id_a)
        .await
        .unwrap()
        .unwrap();

    // Wait for another propagation
    wait_full_sync(sync_interval, &rpc).await;
    if !daemon.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    let list = rpc.list_synchros(context::current()).await.unwrap();
    let sync_a = list.get(&sync_id_a).unwrap();
    let sync_b = list.get(&sync_id_b).unwrap();
    assert_eq!(sync_a.status(), SynchroStatus::Conflict);
    assert_eq!(sync_b.status(), SynchroStatus::Ok);

    rpc.resolve_conflict(
        context::current(),
        sync_id_a,
        VirtualPathBuf::new("/Templates/Readme.md").unwrap(),
        SynchroSide::Local,
    )
    .await
    .unwrap()
    .unwrap();

    // Wait for a full propagation
    wait_full_sync(2 * sync_interval, &rpc).await;
    if !daemon.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    fs_a.diff_vfs().await.unwrap();
    fs_b.diff_vfs().await.unwrap();
    assert!(fs_a.vfs().structural_eq(fs_b.vfs()));
    let content_b = std::fs::read(dir_a.path().to_path_buf().join("Templates/Readme.md")).unwrap();
    assert_eq!(content_a, content_b.as_slice());

    let list = rpc.list_synchros(context::current()).await.unwrap();
    let sync_a = list.get(&sync_id_a).unwrap();
    let sync_b = list.get(&sync_id_b).unwrap();
    assert_eq!(sync_a.status(), SynchroStatus::Ok);
    assert_eq!(sync_b.status(), SynchroStatus::Ok);

    daemon.stop();
    daemon_task.await.unwrap().unwrap();
}
