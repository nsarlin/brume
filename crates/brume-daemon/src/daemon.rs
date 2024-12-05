//! The daemon provides rpc to remotely manipulate the list of synchronized Filesystems

use log::{info, warn};
use tarpc::context::Context;
use tokio::sync::mpsc::UnboundedSender;

use brume::{
    concrete::{local::LocalDir, nextcloud::NextcloudFs, ConcreteFS},
    synchro::{Synchro, Synchronizable},
    Error,
};

use crate::{
    protocol::{AnyFsCreationInfo, AnyFsDescription, BrumeService, SynchroId},
    server::ReadOnlySynchroList,
};

/// Represent [`Synchro`] object where both concrete Filesystem types are only known at runtime.
// TODO: create using a macro ?
pub enum AnySynchro {
    LocalNextcloud(Synchro<LocalDir, NextcloudFs>),
    NextcloudLocal(Synchro<NextcloudFs, LocalDir>),
    LocalLocal(Synchro<LocalDir, LocalDir>),
    NextcloudNextcloud(Synchro<NextcloudFs, NextcloudFs>),
}

impl AnySynchro {
    fn description(&self) -> (AnyFsDescription, AnyFsDescription) {
        match self {
            AnySynchro::LocalNextcloud(synchro) => (
                AnyFsDescription::LocalDir(synchro.local().concrete().description()),
                AnyFsDescription::Nextcloud(synchro.remote().concrete().description()),
            ),
            AnySynchro::NextcloudLocal(synchro) => (
                AnyFsDescription::Nextcloud(synchro.local().concrete().description()),
                AnyFsDescription::LocalDir(synchro.remote().concrete().description()),
            ),
            AnySynchro::LocalLocal(synchro) => (
                AnyFsDescription::LocalDir(synchro.local().concrete().description()),
                AnyFsDescription::LocalDir(synchro.remote().concrete().description()),
            ),

            AnySynchro::NextcloudNextcloud(synchro) => (
                AnyFsDescription::Nextcloud(synchro.local().concrete().description()),
                AnyFsDescription::Nextcloud(synchro.remote().concrete().description()),
            ),
        }
    }
}

impl Synchronizable for AnySynchro {
    async fn full_sync(&mut self) -> Result<(), Error> {
        match self {
            AnySynchro::LocalNextcloud(inner) => inner.full_sync().await,
            AnySynchro::NextcloudLocal(inner) => inner.full_sync().await,
            AnySynchro::LocalLocal(inner) => inner.full_sync().await,
            AnySynchro::NextcloudNextcloud(inner) => inner.full_sync().await,
        }
    }
}

/// The daemon holds the list of the synchronized folders, and can be queried by client applications
#[derive(Clone)]
pub struct BrumeDaemon {
    to_server: UnboundedSender<(SynchroId, AnySynchro)>,
    synchro_list: ReadOnlySynchroList,
}

impl BrumeDaemon {
    pub(crate) fn new(
        to_server: UnboundedSender<(SynchroId, AnySynchro)>,
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
    ) -> Result<SynchroId, String> {
        let local_desc = AnyFsDescription::from(&local);
        let remote_desc = AnyFsDescription::from(&remote);
        info!("Received synchro creation request: local {local_desc}, remote {remote_desc}");
        let sync = match (local, remote) {
            (AnyFsCreationInfo::LocalDir(local_path), AnyFsCreationInfo::LocalDir(remote_path)) => {
                let local = LocalDir::try_from(local_path).map_err(|e| e.to_string())?;
                let remote = LocalDir::try_from(remote_path).map_err(|e| e.to_string())?;

                AnySynchro::LocalLocal(Synchro::new(local, remote))
            }
            (AnyFsCreationInfo::LocalDir(local_path), AnyFsCreationInfo::Nextcloud(remote_log)) => {
                let local = LocalDir::try_from(local_path).map_err(|e| e.to_string())?;
                let remote = NextcloudFs::try_from(remote_log).map_err(|e| e.to_string())?;

                AnySynchro::LocalNextcloud(Synchro::new(local, remote))
            }
            (AnyFsCreationInfo::Nextcloud(local_log), AnyFsCreationInfo::LocalDir(remote_path)) => {
                let local = NextcloudFs::try_from(local_log).map_err(|e| e.to_string())?;
                let remote = LocalDir::try_from(remote_path).map_err(|e| e.to_string())?;

                AnySynchro::NextcloudLocal(Synchro::new(local, remote))
            }
            (AnyFsCreationInfo::Nextcloud(local_log), AnyFsCreationInfo::Nextcloud(remote_log)) => {
                let local = NextcloudFs::try_from(local_log).map_err(|e| e.to_string())?;

                let remote = NextcloudFs::try_from(remote_log).map_err(|e| e.to_string())?;

                AnySynchro::NextcloudNextcloud(Synchro::new(local, remote))
            }
        };

        {
            let list = self.synchro_list.read().await;

            // TODO: also do before insertion to avoir race with elems in queue
            for sync in list.values() {
                let sync = sync.lock().await;
                let (cur_local_desc, cur_remote_desc) = sync.description();
                println!("cur: {cur_local_desc}, {cur_remote_desc}");
                println!("adding: {local_desc}, {remote_desc}");
                if (cur_local_desc == local_desc || cur_local_desc == cur_remote_desc)
                    && (cur_remote_desc == local_desc || cur_remote_desc == remote_desc)
                {
                    warn!("Duplicate sync request, skipping");
                    return Err("Filesystems are already in sync".to_string());
                }
            }
        }

        let syncid = SynchroId::new();

        self.to_server
            .send((syncid, sync))
            .map_err(|e| e.to_string())?;

        info!("Synchro created with id {}", syncid.id());

        Ok(syncid)
    }
}
