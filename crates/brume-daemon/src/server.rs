//! The server provides rpc to remotely manipulate the list of synchronized Filesystems

use std::collections::HashMap;

use brume::vfs::{StatefulVfs, VirtualPathBuf};
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

        let (selected_node, opposite_node) = match side {
            SynchroSide::Local => (
                local_vfs.find_conflict(&path),
                remote_vfs.find_conflict(&path),
            ),
            SynchroSide::Remote => (
                remote_vfs.find_conflict(&path),
                local_vfs.find_conflict(&path),
            ),
        };

        // If the conflict is something like:
        // - Local: DirRemoved(/a/b)
        // - Remote: FileModified(/a/b/c)
        // - resolve_conflict(/a/b, Remote)
        // /a/b won't appear as a conflict in remote fs. We need to get the conflict on the
        // (opposite) local side and resolve it using the files it is conflicting with.
        let path_to_resolve = if selected_node.is_some() {
            vec![path]
        } else if let Some(node) = opposite_node {
            node.otherside_conflict_paths().to_vec()
        } else {
            return Err("Node is not in conflict".to_string());
        };

        for path in path_to_resolve {
            let request = ConflictResolutionRequest::new(id, path, side);
            let command = UserCommand::ConflictResolution(request);

            self.commands_chan
                .send(command)
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    async fn get_vfs(
        self,
        _context: Context,
        id: SynchroId,
        side: SynchroSide,
    ) -> Result<StatefulVfs<()>, String> {
        let list = self.synchro_list.read().await;

        list.get_vfs(id, side).await.map_err(|e| e.to_string())
    }
}
