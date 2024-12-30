//! The server provides rpc to remotely manipulate the list of synchronized Filesystems

use std::collections::HashMap;

use log::{info, warn};
use tarpc::context::Context;
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    protocol::{
        AnyFsCreationInfo, AnyFsDescription, AnySynchroCreationInfo, BrumeService, SynchroId,
    },
    synchro_list::{AnySynchroRef, ReadOnlySynchroList},
};

/// A Server that handle RPC connections from client applications
///
/// The server and the [`Daemon`] are running in separate tasks to be able to give a quick feedback
/// to client applications even when a synchronization is in progress.
///
/// [`Daemon`]: crate::daemon::Daemon
#[derive(Clone)]
pub struct Server {
    creation_chan: UnboundedSender<AnySynchroCreationInfo>,
    deletion_chan: UnboundedSender<SynchroId>,
    synchro_list: ReadOnlySynchroList,
}

impl Server {
    pub(crate) fn new(
        creation_chan: UnboundedSender<AnySynchroCreationInfo>,
        deletion_chan: UnboundedSender<SynchroId>,
        synchro_list: ReadOnlySynchroList,
    ) -> Self {
        Self {
            creation_chan,
            deletion_chan,
            synchro_list,
        }
    }
}

impl BrumeService for Server {
    async fn new_synchro(
        self,
        _context: Context,
        local: AnyFsCreationInfo,
        remote: AnyFsCreationInfo,
        name: Option<String>,
    ) -> Result<(), String> {
        let local_desc = AnyFsDescription::from(local.clone());
        let remote_desc = AnyFsDescription::from(remote.clone());
        info!("Received synchro creation request: local {local_desc}, remote {remote_desc}");

        // Check if the info are suitable for filesystem creation
        local
            .validate()
            .await
            .inspect_err(|e| warn!("{e}, skipping"))?;
        remote
            .validate()
            .await
            .inspect_err(|e| warn!("{e}, skipping"))?;

        // Check if the fs pair is already in sync to return an error to the user
        {
            let list = self.synchro_list.read().await;

            if list.is_synchronized(&local_desc, &remote_desc) {
                warn!("Duplicate sync request, skipping");
                return Err("Filesystems are already in sync".to_string());
            }
        }

        let synchro = AnySynchroCreationInfo::new(local, remote, name);

        self.creation_chan
            .send(synchro)
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    async fn list_synchros(self, _context: Context) -> HashMap<SynchroId, AnySynchroRef> {
        let list = self.synchro_list.read().await;

        list.synchro_ref_list().to_owned()
    }

    async fn delete_synchro(self, _context: Context, id: SynchroId) -> Result<(), String> {
        info!("Received synchro deletion request: id {id:?}");
        self.deletion_chan.send(id).map_err(|e| e.to_string())
    }
}
