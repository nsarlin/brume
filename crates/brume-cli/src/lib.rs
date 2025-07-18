pub mod commands;
pub mod prompt;

use std::collections::HashMap;

use interprocess::local_socket::{
    GenericNamespaced,
    tokio::{Stream, prelude::*},
};
use tarpc::{
    serde_transport, tokio_serde::formats::Bincode, tokio_util::codec::LengthDelimitedCodec,
};

use brume_daemon_proto::{
    BRUME_SOCK_NAME, BrumeServiceClient, ConfigLoadError, SynchroId, SynchroMeta,
    brume_config_path, config::BrumeUserConfig, load_brume_config,
};

pub fn load_config() -> Result<BrumeUserConfig, ConfigLoadError> {
    if let Some(config_path) = brume_config_path() {
        if config_path.exists() {
            load_brume_config(config_path)
        } else {
            Ok(BrumeUserConfig::default())
        }
    } else {
        Ok(BrumeUserConfig::default())
    }
}

pub fn daemon_sock_name(config: &BrumeUserConfig) -> &str {
    config
        .daemon
        .sock_name
        .as_deref()
        .unwrap_or(BRUME_SOCK_NAME)
}

pub async fn connect_to_daemon(sock_name: &str) -> Result<BrumeServiceClient, std::io::Error> {
    let name = sock_name.to_ns_name::<GenericNamespaced>()?;

    let conn = Stream::connect(name).await?;

    let codec_builder = LengthDelimitedCodec::builder();

    let transport = serde_transport::new(codec_builder.new_framed(conn), Bincode::default());

    Ok(BrumeServiceClient::new(Default::default(), transport).spawn())
}

pub fn get_synchro_id(
    synchro_list: &HashMap<SynchroId, SynchroMeta>,
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

pub fn get_synchro(
    synchro_list: &HashMap<SynchroId, SynchroMeta>,
    synchro_descriptor: &str,
) -> Option<(SynchroId, SynchroMeta)> {
    for (id, sync) in synchro_list {
        if (synchro_descriptor.len() > 3 && id.id().to_string().starts_with(synchro_descriptor))
            || sync.name() == synchro_descriptor
        {
            return Some((*id, sync.clone()));
        }
    }
    None
}
