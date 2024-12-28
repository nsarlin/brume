//! The daemon provides rpc to remotely manipulate the list of synchronized Filesystems

use log::{info, warn};
use tarpc::context::Context;
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    protocol::{AnyFsCreationInfo, AnyFsDescription, BrumeService},
    synchro_list::ReadOnlySynchroList,
};

/// The daemon holds the list of the synchronized folders, and can be queried by client applications
#[derive(Clone)]
pub struct BrumeDaemon {
    to_server: UnboundedSender<(AnyFsCreationInfo, AnyFsCreationInfo)>,
    synchro_list: ReadOnlySynchroList,
}

impl BrumeDaemon {
    pub(crate) fn new(
        to_server: UnboundedSender<(AnyFsCreationInfo, AnyFsCreationInfo)>,
        synchro_list: ReadOnlySynchroList,
    ) -> Self {
        Self {
            to_server,
            synchro_list,
        }
    }
}

impl BrumeService for BrumeDaemon {
    async fn new_synchro(
        self,
        _context: Context,
        local: AnyFsCreationInfo,
        remote: AnyFsCreationInfo,
    ) -> Result<(), String> {
        let local_desc = AnyFsDescription::from(local.clone());
        let remote_desc = AnyFsDescription::from(remote.clone());
        info!("Received synchro creation request: local {local_desc}, remote {remote_desc}");

        local.validate().await.inspect_err(|e| warn!("{e}"))?;
        remote.validate().await.inspect_err(|e| warn!("{e}"))?;

        // Check if the fs pair is already in sync to return an error to the user
        {
            let list = self.synchro_list.read().await;

            if list.is_synchronized(&local_desc, &remote_desc) {
                warn!("Duplicate sync request, skipping");
                return Err("Filesystems are already in sync".to_string());
            }
        }

        self.to_server
            .send((local, remote))
            .map_err(|e| e.to_string())?;

        Ok(())
    }
}
