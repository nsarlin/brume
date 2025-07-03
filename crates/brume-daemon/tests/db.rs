use std::{process::exit, sync::Arc, time::Duration};

use brume_daemon_proto::{
    AnyFsCreationInfo, LocalDirCreationInfo, NextcloudFsCreationInfo, SynchroState, SynchroStatus,
    config::ErrorMode,
};

use brume_daemon::{
    daemon::{Daemon, DaemonConfig},
    db::DatabaseConfig,
};
use tarpc::context;
use tracing::info;
use tracing_subscriber::FmtSubscriber;

#[path = "utils.rs"]
mod utils;

use utils::{
    connect_to_daemon, get_random_port, get_random_sock_name, start_nextcloud, stop_nextcloud,
    wait_full_sync,
};

#[tokio::test]
async fn main() {
    let logs_subscriber = FmtSubscriber::new();
    tracing::subscriber::set_global_default(logs_subscriber).unwrap();

    // Start daemon
    let sock_name = get_random_sock_name();
    info!("using sock name {sock_name}");
    let sync_interval = Duration::from_secs(2);
    let dir_db = tempfile::tempdir().unwrap();

    let config = DaemonConfig::default()
        .with_sync_interval(sync_interval)
        .with_error_mode(ErrorMode::Exit)
        .with_db_config(DatabaseConfig::OnDisk(
            dir_db.path().join("db.sqlite").into(),
        ))
        .with_sock_name(&sock_name);
    let daemon = Daemon::new(config.clone()).await.unwrap();
    let daemon = Arc::new(daemon);

    daemon.spawn().await;

    // Create 2 folders that will be synchronized
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();

    info!("dir_a: {:?}", dir_a.path());
    info!("dir_b: {:?}", dir_b.path());

    // Create a nextcloud server
    let nextcloud_port = get_random_port();
    let nextcloud_url = format!("http://localhost:{nextcloud_port}");
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
    let sync_id_a = *list.keys().next().unwrap();

    // Initiate the second synchro
    let local_b = AnyFsCreationInfo::LocalDir(LocalDirCreationInfo::new(dir_b.path()));

    rpc.new_synchro(context::current(), local_b, remote, None)
        .await
        .unwrap()
        .unwrap();

    wait_full_sync(sync_interval, &rpc).await;

    // Pause the first synchro
    rpc.pause_synchro(context::current(), sync_id_a)
        .await
        .unwrap()
        .unwrap();

    // Wait for change to be effective
    wait_full_sync(sync_interval, &rpc).await;

    // Stop the daemon
    daemon.stop();

    let sock_name = get_random_sock_name();
    info!("using sock name {sock_name}");

    // Restart the daemon with the same db
    let daemon = Daemon::new(config.clone().with_sock_name(&sock_name))
        .await
        .unwrap();
    let daemon = Arc::new(daemon);

    daemon.spawn().await;
    let rpc = connect_to_daemon(&sock_name).await.unwrap();

    wait_full_sync(sync_interval, &rpc).await;
    if !daemon.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    let list = rpc.list_synchros(context::current()).await.unwrap();
    assert_eq!(list.len(), 2);
    let sync_id_b = *list.keys().find(|k| **k != sync_id_a).unwrap();

    let sync_a = list.get(&sync_id_a).unwrap();
    assert_eq!(sync_a.state(), SynchroState::Paused);
    let sync_b = list.get(&sync_id_b).unwrap();
    assert_eq!(sync_b.state(), SynchroState::Running);

    // Create a conflict to check status storage in db
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

    wait_full_sync(sync_interval, &rpc).await;
    if !daemon.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    // Stop the daemon
    daemon.stop();

    let sock_name = get_random_sock_name();
    info!("using sock name {sock_name}");

    // Restart the daemon with the same db
    let daemon = Daemon::new(config.with_sock_name(&sock_name))
        .await
        .unwrap();
    let daemon = Arc::new(daemon);
    daemon.spawn().await;

    let rpc = connect_to_daemon(&sock_name).await.unwrap();

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

    daemon.stop()
}
