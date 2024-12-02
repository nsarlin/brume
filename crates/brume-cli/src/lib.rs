use interprocess::local_socket::{
    tokio::{prelude::*, Stream},
    GenericNamespaced,
};
use tarpc::{
    serde_transport, tokio_serde::formats::Bincode, tokio_util::codec::LengthDelimitedCodec,
};

use brume_daemon::protocol::{BrumeServiceClient, BRUME_SOCK_NAME};

pub async fn connect_to_daemon() -> Result<BrumeServiceClient, std::io::Error> {
    let name = BRUME_SOCK_NAME.to_ns_name::<GenericNamespaced>()?;

    let conn = Stream::connect(name).await?;

    let codec_builder = LengthDelimitedCodec::builder();

    let transport = serde_transport::new(codec_builder.new_framed(conn), Bincode::default());

    Ok(BrumeServiceClient::new(Default::default(), transport).spawn())
}
