use std::{net::TcpListener, process::exit, sync::Arc, time::Duration};

use brume::{concrete::local::LocalDir, filesystem::FileSystem};
use env_logger::Builder;
use interprocess::local_socket::{
    tokio::{prelude::*, Stream},
    GenericNamespaced, ToNsName,
};

use log::{info, LevelFilter};
use tarpc::{
    context, serde_transport, tokio_serde::formats::Bincode,
    tokio_util::codec::LengthDelimitedCodec,
};
use testcontainers::{
    core::{wait::HttpWaitStrategy, IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};

use brume_daemon::{
    protocol::{
        AnyFsCreationInfo, BrumeServiceClient, LocalDirCreationInfo, NextcloudFsCreationInfo,
        BRUME_SOCK_NAME,
    },
    server::{ErrorMode, Server as BrumeServer, ServerConfig},
};
use tokio::time::sleep;

fn get_random_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to address");
    listener
        .local_addr()
        .expect("Failed to get local address")
        .port()
}

pub async fn connect_to_daemon() -> Result<BrumeServiceClient, std::io::Error> {
    let name = BRUME_SOCK_NAME.to_ns_name::<GenericNamespaced>()?;

    let conn = Stream::connect(name).await?;

    let codec_builder = LengthDelimitedCodec::builder();

    let transport = serde_transport::new(codec_builder.new_framed(conn), Bincode::default());

    Ok(BrumeServiceClient::new(Default::default(), transport).spawn())
}

pub async fn start_nextcloud(exposed_port: u16, url: &str) -> ContainerAsync<GenericImage> {
    GenericImage::new("nextcloud", "30.0")
        .with_wait_for(WaitFor::http(
            HttpWaitStrategy::new(url)
                .with_port(80.tcp())
                .with_expected_status_code(200u16),
        ))
        .with_env_var("NEXTCLOUD_ADMIN_USER", "admin")
        .with_env_var("NEXTCLOUD_ADMIN_PASSWORD", "admin")
        .with_env_var("SQLITE_DATABASE", "admin")
        .with_mapped_port(exposed_port, 80.tcp())
        .start()
        .await
        .expect("Failed to start Nextcloud server")
}

pub async fn stop_nextcloud(container: ContainerAsync<GenericImage>) {
    container.stop().await.unwrap();
    container.rm().await.unwrap();
}

#[tokio::test]
async fn main() {
    let mut logs_builder = Builder::new();
    logs_builder
        .filter_level(LevelFilter::Info)
        .filter(Some("tarpc"), LevelFilter::Error)
        .init();

    // Start daemon
    let config = ServerConfig::default()
        .with_sync_interval(Duration::from_secs(2))
        .with_error_mode(ErrorMode::Exit);
    let server = BrumeServer::new(config).unwrap();
    let server = Arc::new(server);

    let daemon = {
        let server = server.clone();
        tokio::spawn(async move {
            server.run().await.unwrap();
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

    connect_to_daemon()
        .await
        .unwrap()
        .new_synchro(context::current(), local_a, remote.clone())
        .await
        .unwrap()
        .unwrap();

    // Wait a full sync
    sleep(Duration::from_secs(15)).await;
    if !server.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    // Initiate the second synchro
    let local_b = AnyFsCreationInfo::LocalDir(LocalDirCreationInfo::new(dir_b.path()));

    connect_to_daemon()
        .await
        .unwrap()
        .new_synchro(context::current(), local_b, remote)
        .await
        .unwrap()
        .unwrap();

    // Wait a full sync
    sleep(Duration::from_secs(15)).await;
    if !server.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    // Check filesystem content
    let concrete_a: LocalDir = LocalDirCreationInfo::new(dir_a.path()).try_into().unwrap();
    let mut fs_a = FileSystem::new(concrete_a);

    let concrete_b: LocalDir = LocalDirCreationInfo::new(dir_b.path()).try_into().unwrap();
    let mut fs_b = FileSystem::new(concrete_b);

    fs_a.update_vfs().await.unwrap();
    fs_b.update_vfs().await.unwrap();
    assert!(fs_a.vfs().structural_eq(fs_b.vfs()));

    // Remove a file on one side
    std::fs::remove_file(dir_a.path().to_path_buf().join("Documents/Example.md")).unwrap();

    // Wait for propagation on both fs
    sleep(Duration::from_secs(10)).await;
    if !server.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    fs_a.update_vfs().await.unwrap();
    fs_b.update_vfs().await.unwrap();
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
    if !server.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    fs_a.update_vfs().await.unwrap();
    fs_b.update_vfs().await.unwrap();
    assert!(fs_a.vfs().structural_eq(fs_b.vfs()));
    let content_b = std::fs::read(dir_a.path().to_path_buf().join("Templates/Readme.md")).unwrap();
    assert_eq!(content_a, content_b.as_slice());

    // Create a dir
    std::fs::create_dir(dir_a.path().to_path_buf().join("testdir")).unwrap();

    // Wait for propagation on both fs
    sleep(Duration::from_secs(10)).await;
    if !server.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    fs_a.update_vfs().await.unwrap();
    fs_b.update_vfs().await.unwrap();
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
    if !server.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    fs_a.update_vfs().await.unwrap();
    fs_b.update_vfs().await.unwrap();
    assert!(fs_a.vfs().structural_eq(fs_b.vfs()));
    let content_b = std::fs::read(dir_a.path().to_path_buf().join("testdir/testfile")).unwrap();
    assert_eq!(content_a, content_b.as_slice());

    std::fs::remove_dir_all(dir_a.path().to_path_buf().join("testdir")).unwrap();

    // Wait for propagation on both fs
    sleep(Duration::from_secs(10)).await;
    if !server.is_running() {
        stop_nextcloud(container).await;
        exit(1)
    }

    fs_a.update_vfs().await.unwrap();
    fs_b.update_vfs().await.unwrap();
    assert!(fs_a.vfs().structural_eq(fs_b.vfs()));

    daemon.abort();
    daemon.await.unwrap_err();
}
