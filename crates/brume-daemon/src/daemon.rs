//! The daemon provides rpc to remotely manipulate the list of synchronized Filesystems

use log::info;
use tarpc::context::Context;
use tokio::sync::mpsc::UnboundedSender;

use brume::{
    concrete::{local::LocalDir, nextcloud::NextcloudFs},
    synchro::{Synchro, Synchronizable},
    Error,
};

use crate::protocol::{BrumeService, FsDescription, SynchroId};

/// Represent [`Synchro`] object where both concrete Filesystem types are only known at runtime.
// TODO: create using a macro ?
pub enum AnySynchro {
    LocalNextcloud(Synchro<LocalDir, NextcloudFs>),
    NextcloudLocal(Synchro<NextcloudFs, LocalDir>),
    LocalLocal(Synchro<LocalDir, LocalDir>),
    NextcloudNextcloud(Synchro<NextcloudFs, NextcloudFs>),
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
}

impl BrumeDaemon {
    pub(crate) fn new(to_server: UnboundedSender<(SynchroId, AnySynchro)>) -> Self {
        Self { to_server }
    }
}

impl BrumeService for BrumeDaemon {
    async fn new_synchro(
        self,
        _context: Context,
        local: FsDescription,
        remote: FsDescription,
    ) -> Result<SynchroId, String> {
        info!("Received synchro creation request: local {local}, remote {remote}");
        let sync = match (local, remote) {
            (FsDescription::LocalDir(local_path), FsDescription::LocalDir(remote_path)) => {
                let local = LocalDir::new(local_path).map_err(|e| e.to_string())?;
                let remote = LocalDir::new(remote_path).map_err(|e| e.to_string())?;

                AnySynchro::LocalLocal(Synchro::new(local, remote))
            }
            (FsDescription::LocalDir(local_path), FsDescription::Nextcloud(remote_log)) => {
                let local = LocalDir::new(local_path).map_err(|e| e.to_string())?;
                let remote =
                    NextcloudFs::new(&remote_log.url, &remote_log.login, &remote_log.password)
                        .map_err(|e| e.to_string())?;

                AnySynchro::LocalNextcloud(Synchro::new(local, remote))
            }
            (FsDescription::Nextcloud(local_log), FsDescription::LocalDir(remote_path)) => {
                let local = NextcloudFs::new(&local_log.url, &local_log.login, &local_log.password)
                    .map_err(|e| e.to_string())?;
                let remote = LocalDir::new(remote_path).map_err(|e| e.to_string())?;

                AnySynchro::NextcloudLocal(Synchro::new(local, remote))
            }
            (FsDescription::Nextcloud(local_log), FsDescription::Nextcloud(remote_log)) => {
                let local = NextcloudFs::new(&local_log.url, &local_log.login, &local_log.password)
                    .map_err(|e| e.to_string())?;

                let remote =
                    NextcloudFs::new(&remote_log.url, &remote_log.login, &remote_log.password)
                        .map_err(|e| e.to_string())?;

                AnySynchro::NextcloudNextcloud(Synchro::new(local, remote))
            }
        };

        // TODO: check if synchro already exists to send an error to the user
        let syncid = SynchroId::new();

        self.to_server
            .send((syncid, sync))
            .map_err(|e| e.to_string())?;

        info!("Synchro created with id {}", syncid.id());

        Ok(syncid)
    }
}
