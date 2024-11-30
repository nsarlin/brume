use std::path::PathBuf;

use interprocess::local_socket::{
    tokio::{prelude::*, Stream},
    GenericNamespaced,
};
use tarpc::{
    context, serde_transport, tokio_serde::formats::Bincode,
    tokio_util::codec::LengthDelimitedCodec,
};

use brume_daemon::{BrumeServiceClient, FsDescription, NextcloudLoginInfo, BRUME_SOCK_NAME};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let name = BRUME_SOCK_NAME.to_ns_name::<GenericNamespaced>()?;

    let conn = Stream::connect(name).await?;

    let codec_builder = LengthDelimitedCodec::builder();

    let transport = serde_transport::new(codec_builder.new_framed(conn), Bincode::default());
    let res = BrumeServiceClient::new(Default::default(), transport)
        .spawn()
        .new_synchro(
            context::current(),
            FsDescription::LocalDir(PathBuf::from("/tmp/test")),
            FsDescription::Nextcloud(NextcloudLoginInfo {
                url: "http://localhost:8080".to_string(),
                login: "admin".to_string(),
                password: "admin".to_string(),
            }),
        )
        .await??;

    println!("{res:?}");

    Ok(())
}
