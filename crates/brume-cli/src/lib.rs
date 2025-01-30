use std::collections::HashMap;

use interprocess::local_socket::{
    tokio::{prelude::*, Stream},
    GenericNamespaced,
};
use tarpc::{
    serde_transport, tokio_serde::formats::Bincode, tokio_util::codec::LengthDelimitedCodec,
};

use brume_daemon::{
    protocol::{BrumeServiceClient, SynchroId, BRUME_SOCK_NAME},
    synchro_list::AnySynchroRef,
};

pub async fn connect_to_daemon() -> Result<BrumeServiceClient, std::io::Error> {
    let name = BRUME_SOCK_NAME.to_ns_name::<GenericNamespaced>()?;

    let conn = Stream::connect(name).await?;

    let codec_builder = LengthDelimitedCodec::builder();

    let transport = serde_transport::new(codec_builder.new_framed(conn), Bincode::default());

    Ok(BrumeServiceClient::new(Default::default(), transport).spawn())
}

pub async fn get_synchro_id(
    synchro_list: &HashMap<SynchroId, AnySynchroRef>,
    synchro_descriptor: &str,
) -> Option<SynchroId> {
    for (id, sync) in synchro_list {
        if (synchro_descriptor.len() > 3 && id.id().to_string().starts_with(synchro_descriptor))
            || sync.name() == synchro_descriptor
        {
            return Some(*id);
        }
    }
    None
}

pub async fn get_synchro(
    synchro_list: &HashMap<SynchroId, AnySynchroRef>,
    synchro_descriptor: &str,
) -> Option<(SynchroId, AnySynchroRef)> {
    for (id, sync) in synchro_list {
        if (synchro_descriptor.len() > 3 && id.id().to_string().starts_with(synchro_descriptor))
            || sync.name() == synchro_descriptor
        {
            return Some((*id, sync.clone()));
        }
    }
    None
}
