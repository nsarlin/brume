use std::{net::TcpListener, time::Duration};

use interprocess::local_socket::{
    GenericNamespaced, ToNsName,
    tokio::{Stream, prelude::*},
};
use rand::{Rng, rng};

use tarpc::{
    context, serde_transport, tokio_serde::formats::Bincode,
    tokio_util::codec::LengthDelimitedCodec,
};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor, wait::HttpWaitStrategy},
    runners::AsyncRunner,
};

use brume_daemon_proto::{BrumeServiceClient, SynchroStatus};
use tokio::time::sleep;

pub fn get_random_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to address");
    listener
        .local_addr()
        .expect("Failed to get local address")
        .port()
}

pub fn get_random_sock_name() -> String {
    let suffix: String = (0..5)
        .map(|_| rng().random_range(b'a'..=b'z') as char)
        .collect();

    format!("brume-{suffix}.socket")
}

pub async fn connect_to_daemon(sock_name: &str) -> Result<BrumeServiceClient, std::io::Error> {
    let name = sock_name.to_ns_name::<GenericNamespaced>()?;

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

pub async fn wait_full_sync(sync_interval: Duration, rpc: &BrumeServiceClient) {
    // Wait to at least get one sync to start
    sleep(sync_interval).await;

    // Wait for the sync to end
    loop {
        sleep(Duration::from_secs(1)).await;
        let list = rpc.list_synchros(context::current()).await.unwrap();
        if list
            .values()
            .all(|sync| sync.status() != SynchroStatus::SyncInProgress)
        {
            break;
        }
    }
}
