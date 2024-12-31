use std::{any::Any, collections::HashMap, fmt::Display, sync::Arc};

use brume::{
    concrete::{local::LocalDir, nextcloud::NextcloudFs, ConcreteFS, ConcreteFsError},
    filesystem::FileSystem,
    synchro::{Synchro, SynchroStatus},
};
use futures::{future::join_all, stream, StreamExt};
use log::info;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};
use uuid::Uuid;

use crate::protocol::{AnyFsCreationInfo, AnyFsDescription, AnySynchroCreationInfo, SynchroId};

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

#[derive(Error, Debug)]
pub enum SynchroDeletionError {
    #[error("Invalid synchro")]
    InvalidSynchroState(#[from] InvalidSynchro),
    #[error("Synchro not found: {0:?}")]
    SynchroNotFound(SynchroId),
}

/// A reference to a filesystem in the [`SynchroList`]
///
/// This type represents only an index and needs a valid [`SynchroList`], to actually be used. It
/// must not be used after the Filesystem it points to has been removed from the SynchroList.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AnyFsRef {
    id: Uuid,
    description: AnyFsDescription,
}

impl Display for AnyFsRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl From<AnyFsCreationInfo> for AnyFsRef {
    fn from(value: AnyFsCreationInfo) -> Self {
        Self {
            id: Uuid::new_v4(),
            description: value.into(),
        }
    }
}

impl AnyFsRef {
    pub fn description(&self) -> &AnyFsDescription {
        &self.description
    }
}

/// A [`Synchro`] where the [`Concrete`] filesystems are only known at runtime.
///
/// This type represents only an index and needs a valid [`SynchroList`], to actually be used. It
/// must not be used after the Synchro it points to has been removed from the SynchroList.
///
/// [`Concrete`]: ConcreteFS
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AnySynchroRef {
    local: AnyFsRef,
    remote: AnyFsRef,
    status: SynchroStatus,
    name: String,
}

impl Display for AnySynchroRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "synchro between {} and {}", self.local, self.remote)
    }
}

impl AnySynchroRef {
    /// Returns the local counterpart of the synchro
    pub fn local(&self) -> &AnyFsRef {
        &self.local
    }

    /// Returns the remote counterpart of the synchro
    pub fn remote(&self) -> &AnyFsRef {
        &self.remote
    }

    /// Returns the name of the synchro
    ///
    /// This name is only "relatively" unique, meaning that names can be reused. However, at a
    /// specific point in time and for a specific [`SynchroList`], there should be no collision.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the status of this synchro
    pub fn status(&self) -> SynchroStatus {
        self.status
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
    synchros: HashMap<SynchroId, RwLock<AnySynchroRef>>,
    nextcloud_list: HashMap<Uuid, Mutex<FileSystem<NextcloudFs>>>,
    local_dir_list: HashMap<Uuid, Mutex<FileSystem<LocalDir>>>,
}

impl SynchroList {
    /// Create a new empty list
    pub fn new() -> Self {
        Self::default()
    }

    pub fn synchro_ref_list(&self) -> &HashMap<SynchroId, RwLock<AnySynchroRef>> {
        &self.synchros
    }

    fn create_and_insert_fs(
        &mut self,
        fs_info: AnyFsCreationInfo,
    ) -> Result<AnyFsRef, ConcreteFsError> {
        let fs_ref: AnyFsRef = fs_info.clone().into();
        match fs_info {
            AnyFsCreationInfo::LocalDir(info) => {
                let concrete = info.try_into()?;
                self.local_dir_list
                    .insert(fs_ref.id, Mutex::new(FileSystem::new(concrete)));
                Ok(fs_ref)
            }
            AnyFsCreationInfo::Nextcloud(info) => {
                let concrete = info.try_into()?;
                self.nextcloud_list
                    .insert(fs_ref.id, Mutex::new(FileSystem::new(concrete)));
                Ok(fs_ref)
            }
        }
    }

    /// Checks if the two Filesystems in the pair are already synchronized together.
    pub async fn is_synchronized(
        &self,
        local_desc: &AnyFsDescription,
        remote_desc: &AnyFsDescription,
    ) -> bool {
        for sync in self.synchros.values() {
            let sync = sync.read().await;
            if (sync.local.description() == local_desc || sync.local.description() == remote_desc)
                && (sync.remote.description() == local_desc
                    || sync.remote.description() == remote_desc)
            {
                return true;
            }
        }

        false
    }

    async fn name_is_unique(&self, name: &str) -> bool {
        for sync in self.synchros.values() {
            if sync.read().await.name == name {
                return false;
            }
        }
        true
    }

    async fn make_unique_name(&self, name: &str) -> String {
        if self.name_is_unique(name).await {
            return name.to_string();
        }

        let mut counter = 1;
        loop {
            let new_name = format!("{}{}", name, counter);
            if self.name_is_unique(name).await {
                return new_name;
            }
            counter += 1;
        }
    }

    /// Generates a unique and simple name for a synchro, based on the name of the remote and local
    /// fs
    async fn unique_synchro_name(&self, local_name: &str, remote_name: &str) -> String {
        let same_remote: Vec<_> = stream::iter(self.synchros.values())
            .filter(|sync| async { sync.read().await.remote.description.name() == remote_name })
            .collect()
            .await;

        if same_remote.is_empty() && self.name_is_unique(remote_name).await {
            return remote_name.to_string();
        }

        let same_local: Vec<_> = stream::iter(same_remote)
            .filter(|sync| async { sync.read().await.local.description.name() == local_name })
            .collect()
            .await;

        let name = format!("{local_name}-{remote_name}");
        if same_local.is_empty() && self.name_is_unique(&name).await {
            return name;
        }

        self.make_unique_name(&name).await
    }

    /// Creates and inserts a new filesystem Synchro in the list
    pub async fn insert(
        &mut self,
        sync_info: AnySynchroCreationInfo,
    ) -> Result<SynchroId, SynchroCreationError> {
        let local_desc = sync_info.local().clone().into();
        let remote_desc = sync_info.remote().clone().into();

        if self.is_synchronized(&local_desc, &remote_desc).await {
            return Err(SynchroCreationError::AlreadyPresent);
        }
        let id = SynchroId::new();

        let local_ref = self.create_and_insert_fs(sync_info.local().clone())?;
        let remote_ref = self.create_and_insert_fs(sync_info.remote().clone())?;
        let name = if let Some(name) = sync_info.name() {
            self.make_unique_name(name).await
        } else {
            self.unique_synchro_name(local_ref.description.name(), remote_ref.description.name())
                .await
        };

        info!("Synchro created: name: {name}, id: {id:?}");
        let synchro = AnySynchroRef {
            local: local_ref,
            remote: remote_ref,
            status: SynchroStatus::default(),
            name,
        };

        self.synchros.insert(id, RwLock::new(synchro));

        Ok(id)
    }

    /// Deletes a synchronization from the list
    pub fn remove(&mut self, id: SynchroId) -> Result<(), SynchroDeletionError> {
        if let Some(sync) = self.synchros.remove(&id) {
            let mut res = Ok(());
            let sync = sync.into_inner();

            if !self.remove_fs(&sync.local) {
                res = Err(SynchroDeletionError::InvalidSynchroState(
                    sync.clone().into(),
                ));
            }
            if !self.remove_fs(&sync.remote) {
                res = Err(SynchroDeletionError::InvalidSynchroState(
                    sync.clone().into(),
                ));
            }

            info!("Synchro deleted: {id:?}");
            res
        } else {
            Err(SynchroDeletionError::SynchroNotFound(id))
        }
    }

    fn get_fs<Concrete: ConcreteFS + 'static>(
        &self,
        fs: &AnyFsRef,
    ) -> Option<&Mutex<FileSystem<Concrete>>> {
        match fs.description() {
            AnyFsDescription::LocalDir(_) => self
                .local_dir_list
                .get(&fs.id)
                .and_then(|fs| fs.as_any().downcast_ref::<Mutex<FileSystem<Concrete>>>()),
            AnyFsDescription::Nextcloud(_) => self
                .nextcloud_list
                .get(&fs.id)
                .and_then(|fs| fs.as_any().downcast_ref::<Mutex<FileSystem<Concrete>>>()),
        }
    }

    fn remove_fs(&mut self, fs: &AnyFsRef) -> bool {
        match fs.description() {
            AnyFsDescription::LocalDir(_) => self.local_dir_list.remove(&fs.id).is_some(),
            AnyFsDescription::Nextcloud(_) => self.nextcloud_list.remove(&fs.id).is_some(),
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

    /// Perfoms a [`full_sync`] on the provided synchro, that should be in the list
    ///
    /// [`full_sync`]: brume::synchro::Synchro::full_sync
    pub async fn sync_one<
        LocalConcrete: ConcreteFS + 'static,
        RemoteConcrete: ConcreteFS + 'static,
    >(
        &self,
        synchro_lock: &RwLock<AnySynchroRef>,
    ) -> Result<(), SyncError> {
        let res = {
            let synchro = synchro_lock.read().await;

            // Skip synchro that are already identified as desynchronized until the user fixes it
            if synchro.status == SynchroStatus::Desync {
                return Ok(());
            }

            let synchro_mutex = self
                .get_sync::<LocalConcrete, RemoteConcrete>(&synchro)
                .ok_or_else(|| InvalidSynchro::from(synchro.clone()))?;

            let mut local_fs = synchro_mutex.local.lock().await;
            let mut remote_fs = synchro_mutex.remote.lock().await;
            let mut sync = Synchro::new(&mut local_fs, &mut remote_fs);
            sync.full_sync().await
        };

        match res {
            Ok(status) => {
                let mut synchro = synchro_lock.write().await;
                synchro.status = status;
                Ok(())
            }
            Err(err) => {
                let mut synchro = synchro_lock.write().await;
                synchro.status = SynchroStatus::from(&err);
                Err(SynchroFailed {
                    synchro: synchro.to_owned(),
                    source: err,
                }
                .into())
            }
        }
    }

    /// Perfoms a [`full_sync`] on all the synchro in the list
    ///
    /// [`full_sync`]: brume::synchro::Synchro::full_sync
    pub async fn sync_all(&self) -> Vec<Result<(), SyncError>> {
        let futures: Vec<_> = self
            .synchros
            .values()
            .map(|synchro| async move {
                let local_desc = synchro.read().await.local.description().clone();
                let remote_desc = synchro.read().await.remote.description().clone();
                match (local_desc, remote_desc) {
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

    /// Inserts a new synchronized pair of filesystem in the list
    pub async fn insert(
        &self,
        sync_info: AnySynchroCreationInfo,
    ) -> Result<SynchroId, SynchroCreationError> {
        self.maps.write().await.insert(sync_info).await
    }

    /// Deletes a synchronization from the list
    pub async fn remove(&self, id: SynchroId) -> Result<(), SynchroDeletionError> {
        self.maps.write().await.remove(id)
    }

    /// Forces a synchro of all the filesystems in the list
    pub async fn sync_all(&self) -> Vec<Result<(), SyncError>> {
        let maps = self.maps.read().await;

        maps.sync_all().await
    }

    /// Returns a read-only view of the list
    pub fn as_read_only(&self) -> ReadOnlySynchroList {
        ReadOnlySynchroList {
            maps: self.maps.clone(),
        }
    }

    /// Creates a new empty list
    pub fn new() -> Self {
        Self {
            maps: Arc::new(RwLock::new(SynchroList::new())),
        }
    }
}

#[cfg(test)]
mod test {
    use brume::concrete::{local::LocalDirCreationInfo, nextcloud::NextcloudFsCreationInfo};

    use crate::protocol::{AnyFsCreationInfo, AnySynchroCreationInfo};

    use super::*;

    #[tokio::test]
    async fn test_insert_remove() {
        let mut list = SynchroList::new();

        let loc_a = LocalDirCreationInfo::new("/a");
        let loc_b = LocalDirCreationInfo::new("/b");
        let sync1 = AnySynchroCreationInfo::new(
            AnyFsCreationInfo::LocalDir(loc_a),
            AnyFsCreationInfo::LocalDir(loc_b),
            None,
        );

        let id1 = list.insert(sync1).await.unwrap();

        assert_eq!(list.synchros.len(), 1);
        assert_eq!(list.local_dir_list.len(), 2);
        assert_eq!(list.nextcloud_list.len(), 0);

        let nx_a = NextcloudFsCreationInfo::new("https://cloud.com", "user", "user");
        let loc_b = LocalDirCreationInfo::new("/b");
        let sync2 = AnySynchroCreationInfo::new(
            AnyFsCreationInfo::LocalDir(loc_b),
            AnyFsCreationInfo::Nextcloud(nx_a),
            None,
        );

        list.insert(sync2).await.unwrap();

        assert_eq!(list.synchros.len(), 2);
        assert_eq!(list.local_dir_list.len(), 3);
        assert_eq!(list.nextcloud_list.len(), 1);

        list.remove(id1).unwrap();

        assert_eq!(list.synchros.len(), 1);
        assert_eq!(list.local_dir_list.len(), 1);
        assert_eq!(list.nextcloud_list.len(), 1);
    }
}
