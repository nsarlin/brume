use std::{any::Any, collections::HashMap, fmt::Display, sync::Arc};

use brume::{
    concrete::{
        local::{LocalDir, LocalDirDescription},
        nextcloud::{NextcloudFs, NextcloudFsDescription},
        ConcreteFS, ConcreteFsError,
    },
    filesystem::FileSystem,
    synchro::Synchro,
};
use futures::future::join_all;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};

use crate::protocol::{AnyFsCreationInfo, AnyFsDescription, SynchroId};

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("Error during sync process")]
    SyncFailed(#[from] SynchroFailed),
    #[error("Invalid synchro")]
    InvalidSynchroState(#[from] InvalidSynchro),
}

#[derive(Error, Debug)]
#[error("Failed to synchronize {synchro}")]
pub struct SynchroFailed {
    synchro: AnySynchroRef,
    source: brume::Error,
}

#[derive(Error, Debug)]
#[error("Synchro in invalid state: {synchro}")]
pub struct InvalidSynchro {
    synchro: AnySynchroRef,
}

impl From<AnySynchroRef> for InvalidSynchro {
    fn from(value: AnySynchroRef) -> Self {
        InvalidSynchro { synchro: value }
    }
}

#[derive(Error, Debug)]
pub enum SynchroCreationError {
    #[error("The filesystems are already synchronized")]
    AlreadyPresent,
    #[error("Failed to instantiate filesystem object")]
    FileSystemCreationError(#[from] ConcreteFsError),
}

/// A [`Synchro`] where the [`Concrete`] filesystems are only known at runtime.
///
/// This type represents only an index and needs a valid [`SynchroList`], to actually be used. It
/// must not be used after the Synchro it points to has been removed from the SynchroList.
///
/// [`Concrete`]: ConcreteFS
#[derive(Clone, Debug)]
pub struct AnySynchroRef {
    local: AnyFsDescription,
    remote: AnyFsDescription,
    _id: SynchroId,
}

impl Display for AnySynchroRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "synchro between {} and {}", self.local, self.remote)
    }
}

/// A [`SynchroList`] that allows only read-only access.
///
/// The list content cannot be modified, but since the underlying [`FileSystems`] are locked behind
/// mutexes, they are themselves modifiabled.
///
/// [`FileSystems`]: FileSystem
#[derive(Clone)]
pub struct ReadOnlySynchroList {
    maps: Arc<RwLock<SynchroList>>,
}

impl ReadOnlySynchroList {
    pub async fn read(&self) -> RwLockReadGuard<SynchroList> {
        self.maps.read().await
    }
}

/// A synchronized Filesystem pair where both filesystems are in a Mutex
struct SynchroMutex<
    'local,
    'remote,
    LocalConcrete: ConcreteFS + 'static,
    RemoteConcrete: ConcreteFS + 'static,
> {
    local: &'local Mutex<FileSystem<LocalConcrete>>,
    remote: &'remote Mutex<FileSystem<RemoteConcrete>>,
}

/// Allow to easily convert the given type into [`Any`] for runtime downcast
trait DynTyped {
    fn as_any(&self) -> &dyn Any;
}

impl<T: ConcreteFS + 'static> DynTyped for Mutex<FileSystem<T>> {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Holds a list of pair of [`FileSystems`] that can be synchronized using [`Synchro::full_sync`].
///
/// The filesystems can be any of the [`supported types`]
///
/// [`FileSystems`]: FileSystem
/// [`supported types`]: crate::protocol::AnyFsCreationInfo
#[derive(Default)]
pub struct SynchroList {
    synchros: Vec<AnySynchroRef>,
    nextcloud_list: HashMap<NextcloudFsDescription, Mutex<FileSystem<NextcloudFs>>>,
    local_dir_list: HashMap<LocalDirDescription, Mutex<FileSystem<LocalDir>>>,
}

impl SynchroList {
    /// Create a new empty list
    pub fn new() -> Self {
        Self::default()
    }

    fn create_and_insert_fs(&mut self, fs_info: AnyFsCreationInfo) -> Result<(), ConcreteFsError> {
        // TODO: improve duplicates handling
        match fs_info {
            AnyFsCreationInfo::LocalDir(info) => {
                let creation_info = info.clone().try_into()?;
                self.local_dir_list
                    .entry(info.into())
                    .or_insert_with(|| Mutex::new(FileSystem::new(creation_info)));
                Ok(())
            }
            AnyFsCreationInfo::Nextcloud(info) => {
                let creation_info = info.clone().try_into()?;
                self.nextcloud_list
                    .entry(info.into())
                    .or_insert_with(|| Mutex::new(FileSystem::new(creation_info)));
                Ok(())
            }
        }
    }

    /// Check if the two Filesystems in the pair are already synchronized together.
    pub fn is_synchronized(
        &self,
        local_desc: &AnyFsDescription,
        remote_desc: &AnyFsDescription,
    ) -> bool {
        for sync in &self.synchros {
            if (&sync.local == local_desc || &sync.local == remote_desc)
                && (&sync.remote == local_desc || &sync.remote == remote_desc)
            {
                return false;
            }
        }

        true
    }

    /// Create and insert a new filesystem Synchro in the list
    pub fn insert(
        &mut self,
        local: AnyFsCreationInfo,
        remote: AnyFsCreationInfo,
    ) -> Result<(), SynchroCreationError> {
        let local_desc = local.clone().into();
        let remote_desc = remote.clone().into();

        if self.is_synchronized(&local_desc, &remote_desc) {
            return Err(SynchroCreationError::AlreadyPresent);
        }
        let id = SynchroId::new();

        self.create_and_insert_fs(local.clone())?;
        self.create_and_insert_fs(remote.clone())?;

        let synchro = AnySynchroRef {
            local: local.into(),
            remote: remote.into(),
            _id: id,
        };

        self.synchros.push(synchro);

        Ok(())
    }

    fn get_fs<Concrete: ConcreteFS + 'static>(
        &self,
        desc: &AnyFsDescription,
    ) -> Option<&Mutex<FileSystem<Concrete>>> {
        match desc {
            AnyFsDescription::LocalDir(desc) => self
                .local_dir_list
                .get(desc)
                .and_then(|fs| fs.as_any().downcast_ref::<Mutex<FileSystem<Concrete>>>()),
            AnyFsDescription::Nextcloud(desc) => self
                .nextcloud_list
                .get(desc)
                .and_then(|fs| fs.as_any().downcast_ref::<Mutex<FileSystem<Concrete>>>()),
        }
    }

    fn get_sync<LocalConcrete: ConcreteFS + 'static, RemoteConcrete: ConcreteFS + 'static>(
        &self,
        synchro: &AnySynchroRef,
    ) -> Option<SynchroMutex<LocalConcrete, RemoteConcrete>> {
        let local = self.get_fs::<LocalConcrete>(&synchro.local)?;
        let remote = self.get_fs::<RemoteConcrete>(&synchro.remote)?;

        Some(SynchroMutex { local, remote })
    }

    /// Perfom a [`full_sync`] on the provided synchro, that should be in the list
    ///
    /// [`full_sync`]: brume::synchro::Synchro::full_sync
    pub async fn sync_one<
        LocalConcrete: ConcreteFS + 'static,
        RemoteConcrete: ConcreteFS + 'static,
    >(
        &self,
        synchro: &AnySynchroRef,
    ) -> Result<(), SyncError> {
        let synchro_mutex = self
            .get_sync::<LocalConcrete, RemoteConcrete>(synchro)
            .ok_or_else(|| InvalidSynchro::from(synchro.clone()))?;

        let mut local_fs = synchro_mutex.local.lock().await;
        let mut remote_fs = synchro_mutex.remote.lock().await;
        let mut sync = Synchro::new(&mut local_fs, &mut remote_fs);
        sync.full_sync().await.map_err(|e| {
            SynchroFailed {
                synchro: synchro.to_owned(),
                source: e,
            }
            .into()
        })
    }

    /// Perfom a [`full_sync`] on all the synchro in the list
    ///
    /// [`full_sync`]: brume::synchro::Synchro::full_sync
    pub async fn sync_all(&self) -> Vec<Result<(), SyncError>> {
        let futures: Vec<_> = self
            .synchros
            .iter()
            .map(|synchro| async {
                match (&synchro.local, &synchro.remote) {
                    (AnyFsDescription::LocalDir(_), AnyFsDescription::LocalDir(_)) => {
                        self.sync_one::<LocalDir, LocalDir>(synchro).await
                    }
                    (AnyFsDescription::LocalDir(_), AnyFsDescription::Nextcloud(_)) => {
                        self.sync_one::<LocalDir, NextcloudFs>(synchro).await
                    }
                    (AnyFsDescription::Nextcloud(_), AnyFsDescription::LocalDir(_)) => {
                        self.sync_one::<NextcloudFs, LocalDir>(synchro).await
                    }
                    (AnyFsDescription::Nextcloud(_), AnyFsDescription::Nextcloud(_)) => {
                        self.sync_one::<NextcloudFs, NextcloudFs>(synchro).await
                    }
                }
            })
            .collect();

        // TODO: switch to FutureUnordered ?
        join_all(futures).await
    }
}

/// A [`SynchroList`] that allows read-write access and can be shared between threads.
#[derive(Clone)]
pub struct ReadWriteSynchroList {
    maps: Arc<RwLock<SynchroList>>,
}

impl Default for ReadWriteSynchroList {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadWriteSynchroList {
    pub async fn read(&self) -> RwLockReadGuard<SynchroList> {
        self.maps.read().await
    }

    /// Insert a new synchronized pair of filesystem in the list
    pub async fn insert(
        &self,
        local: AnyFsCreationInfo,
        remote: AnyFsCreationInfo,
    ) -> Result<(), SynchroCreationError> {
        self.maps.write().await.insert(local, remote)
    }

    /// Force a synchro of all the filesystems in the list
    pub async fn sync_all(&self) -> Vec<Result<(), SyncError>> {
        let maps = self.maps.read().await;

        maps.sync_all().await
    }

    /// Return a read-only view of the list
    pub fn as_read_only(&self) -> ReadOnlySynchroList {
        ReadOnlySynchroList {
            maps: self.maps.clone(),
        }
    }

    /// Create a new empty list
    pub fn new() -> Self {
        Self {
            maps: Arc::new(RwLock::new(SynchroList::new())),
        }
    }
}
