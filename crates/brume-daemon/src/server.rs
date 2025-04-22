//! The server provides rpc to remotely manipulate the list of synchronized Filesystems

use std::collections::HashMap;

use brume::vfs::VirtualPathBuf;
use log::{info, warn};
use tarpc::context::Context;
use tokio::sync::mpsc::UnboundedSender;

use brume_daemon_proto::{
    AnyFsCreationInfo, AnyFsDescription, AnySynchroCreationInfo, BrumeService, SynchroId,
    SynchroMeta, SynchroSide, SynchroState,
};

use crate::{
    daemon::{
        ConflictResolutionRequest, StateChangeRequest, SynchroCreationRequest,
        SynchroDeletionRequest, UserCommand,
    },
    synchro_list::ReadOnlySynchroList,
};

/// A Server that handle RPC connections from client applications
///
/// The server and the [`Daemon`] are running in separate tasks to be able to give a quick feedback
/// to client applications even when a synchronization is in progress.
///
/// [`Daemon`]: crate::daemon::Daemon
#[derive(Clone)]
pub struct Server {
    commands_chan: UnboundedSender<UserCommand>,
    synchro_list: ReadOnlySynchroList,
}

impl Server {
    pub(crate) fn new(
        commands_chan: UnboundedSender<UserCommand>,
        synchro_list: ReadOnlySynchroList,
    ) -> Self {
        Self {
            commands_chan,
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
        let command = UserCommand::SynchroCreation(SynchroCreationRequest::new(synchro));

        self.commands_chan
            .send(command)
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    async fn list_synchros(self, _context: Context) -> HashMap<SynchroId, SynchroMeta> {
        self.synchro_list.read().await.synchro_meta_list().await
    }

    async fn delete_synchro(self, _context: Context, id: SynchroId) -> Result<(), String> {
        info!("Received synchro deletion request: id {id:?}");

        let command = UserCommand::SynchroDeletion(SynchroDeletionRequest::new(id));
        self.commands_chan.send(command).map_err(|e| e.to_string())
    }

    async fn pause_synchro(self, _context: Context, id: SynchroId) -> Result<(), String> {
        info!("Received synchro pause request: id {id:?}");
        let request = StateChangeRequest::new(id, SynchroState::Paused);
        let command = UserCommand::StateChange(request);
        self.commands_chan.send(command).map_err(|e| e.to_string())
    }

    async fn resume_synchro(self, _context: Context, id: SynchroId) -> Result<(), String> {
        info!("Received synchro resume request: id {id:?}");
        let request = StateChangeRequest::new(id, SynchroState::Running);
        let command = UserCommand::StateChange(request);
        self.commands_chan.send(command).map_err(|e| e.to_string())
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

        let request = ConflictResolutionRequest::new(id, path, side);
        let command = UserCommand::ConflictResolution(request);

        self.commands_chan
            .send(command)
            .map_err(|e| e.to_string())?;
        Ok(())
    }
}
