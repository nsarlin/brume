//! The server provides rpc to remotely manipulate the list of synchronized Filesystems

use std::collections::HashMap;

use brume::vfs::VirtualPathBuf;
use log::{info, warn};
use tarpc::context::Context;
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    daemon::{ConflictResolutionRequest, StateChangeRequest, SynchroState},
    protocol::{
        AnyFsCreationInfo, AnyFsDescription, AnySynchroCreationInfo, BrumeService, SynchroId,
        SynchroSide,
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
    state_change_chan: UnboundedSender<StateChangeRequest>,
    conflict_resolution_chan: UnboundedSender<ConflictResolutionRequest>,
    synchro_list: ReadOnlySynchroList,
}

impl Server {
    pub(crate) fn new(
        creation_chan: UnboundedSender<AnySynchroCreationInfo>,
        deletion_chan: UnboundedSender<SynchroId>,
        state_change_chan: UnboundedSender<StateChangeRequest>,
        conflict_resolution_chan: UnboundedSender<ConflictResolutionRequest>,
        synchro_list: ReadOnlySynchroList,
    ) -> Self {
        Self {
            creation_chan,
            deletion_chan,
            state_change_chan,
            conflict_resolution_chan,
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

            if list.is_synchronized(&local_desc, &remote_desc).await {
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

        let mut ret = HashMap::new();
        for (key, val) in list.synchro_ref_list() {
            ret.insert(*key, val.read().await.clone());
        }

        ret
    }

    async fn delete_synchro(self, _context: Context, id: SynchroId) -> Result<(), String> {
        info!("Received synchro deletion request: id {id:?}");
        self.deletion_chan.send(id).map_err(|e| e.to_string())
    }

    async fn pause_synchro(self, _context: Context, id: SynchroId) -> Result<(), String> {
        info!("Received synchro pause request: id {id:?}");
        let request = StateChangeRequest::new(id, SynchroState::Paused);
        self.state_change_chan
            .send(request)
            .map_err(|e| e.to_string())
    }

    async fn resume_synchro(self, _context: Context, id: SynchroId) -> Result<(), String> {
        info!("Received synchro resume request: id {id:?}");
        let request = StateChangeRequest::new(id, SynchroState::Running);
        self.state_change_chan
            .send(request)
            .map_err(|e| e.to_string())
    }

    async fn resolve_conflict(
        self,
        _context: Context,
        id: SynchroId,
        path: VirtualPathBuf,
        side: SynchroSide,
    ) -> Result<(), String> {
        info!("Received conflict resolution request: id {id:?}, path {path:?}, side: {side:?}");
        // Check if the synchro is valid and if the file exist to be able to return an early error
        let (local_vfs, remote_vfs) = {
            let list = self.synchro_list.read().await;

            let local = list
                .get_vfs(id, SynchroSide::Local)
                .await
                .map_err(|e| e.to_string())?;

            let remote = list
                .get_vfs(id, SynchroSide::Remote)
                .await
                .map_err(|e| e.to_string())?;

            (local, remote)
        };

        let node = local_vfs
            .find_node(&path)
            .or_else(|| remote_vfs.find_node(&path))
            .ok_or_else(|| "Invalid path".to_string())?;

        if !node.state().is_conflict() {
            return Err("Node is not in conflict".to_string());
        }

        self.conflict_resolution_chan
            .send(ConflictResolutionRequest::new(id, path, side))
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}
