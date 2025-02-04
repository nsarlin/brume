use std::{any::Any, collections::HashMap, fmt::Display, sync::Arc, thread::sleep, time::Duration};

use brume::{
    concrete::{local::LocalDir, nextcloud::NextcloudFs, FSBackend, FsBackendError},
    filesystem::FileSystem,
    synchro::{FullSyncStatus, Synchro, SynchroSide},
    vfs::{Vfs, VirtualPath},
};
use futures::{future::join_all, stream, StreamExt};
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};
use uuid::Uuid;

use crate::{
    daemon::{SynchroState, SynchroStatus},
    protocol::{AnyFsCreationInfo, AnyFsDescription, AnySynchroCreationInfo, SynchroId},
};

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("Error during sync process")]
    SyncFailed(#[from] SynchroFailed),
    #[error("Invalid synchro")]
    InvalidSynchroState(#[from] InvalidSynchro),
    #[error("Synchro not found: {0:?}")]
    SynchroNotFound(SynchroId),
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
    FileSystemCreationError(#[from] FsBackendError),
}

#[derive(Error, Debug)]
pub enum SynchroDeletionError {
    #[error("Invalid synchro")]
    InvalidSynchroState(#[from] InvalidSynchro),
    #[error("Synchro not found: {0:?}")]
    SynchroNotFound(SynchroId),
}

#[derive(Error, Debug)]
pub enum SynchroModificationError {
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

/// A [`Synchro`] where the [`Backend`] filesystems are only known at runtime.
///
/// This type represents only an index and needs a valid [`SynchroList`], to actually be used. It
/// must not be used after the Synchro it points to has been removed from the SynchroList.
///
/// [`Backend`]: FSBackend
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AnySynchroRef {
    local: AnyFsRef,
    remote: AnyFsRef,
    /// the status of the synchro is automatically updated, for example in case of error or
    /// conflict
    status: SynchroStatus,
    /// the state is defined by the user, for example running or paused
    state: SynchroState,
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

    /// Returns the state of this synchro
    pub fn state(&self) -> SynchroState {
        self.state
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

    pub async fn len(&self) -> usize {
        self.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

/// A synchronized Filesystem pair where both filesystems are in a Mutex
struct SynchroMutex<
    'local,
    'remote,
    LocalBackend: FSBackend + 'static,
    RemoteBackend: FSBackend + 'static,
> {
    local: &'local Mutex<FileSystem<LocalBackend>>,
    remote: &'remote Mutex<FileSystem<RemoteBackend>>,
}

/// Allow to easily convert the given type into [`Any`] for runtime downcast
trait DynTyped {
    fn as_any(&self) -> &dyn Any;
}

impl<T: FSBackend + 'static> DynTyped for Mutex<FileSystem<T>> {
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

    pub fn len(&self) -> usize {
        self.synchros.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn synchro_ref_list(&self) -> &HashMap<SynchroId, RwLock<AnySynchroRef>> {
        &self.synchros
    }

    fn create_and_insert_fs(
        &mut self,
        fs_info: AnyFsCreationInfo,
    ) -> Result<AnyFsRef, FsBackendError> {
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
            state: SynchroState::default(),
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

    pub async fn resolve_conflict(
        &self,
        id: SynchroId,
        path: &VirtualPath,
        side: SynchroSide,
    ) -> Result<(), SyncError> {
        let synchro = self
            .synchros
            .get(&id)
            .ok_or_else(|| SyncError::SynchroNotFound(id))?;
        let local_desc = synchro.read().await.local.description().clone();
        let remote_desc = synchro.read().await.remote.description().clone();
        match (local_desc, remote_desc) {
            (AnyFsDescription::LocalDir(_), AnyFsDescription::LocalDir(_)) => {
                self.resolve_conflict_sync::<LocalDir, LocalDir>(synchro, path, side)
                    .await
            }
            (AnyFsDescription::LocalDir(_), AnyFsDescription::Nextcloud(_)) => {
                self.resolve_conflict_sync::<LocalDir, NextcloudFs>(synchro, path, side)
                    .await
            }
            (AnyFsDescription::Nextcloud(_), AnyFsDescription::LocalDir(_)) => {
                self.resolve_conflict_sync::<NextcloudFs, LocalDir>(synchro, path, side)
                    .await
            }
            (AnyFsDescription::Nextcloud(_), AnyFsDescription::Nextcloud(_)) => {
                self.resolve_conflict_sync::<NextcloudFs, NextcloudFs>(synchro, path, side)
                    .await
            }
        }
    }

    fn get_fs<Backend: FSBackend + 'static>(
        &self,
        fs: &AnyFsRef,
    ) -> Option<&Mutex<FileSystem<Backend>>> {
        match fs.description() {
            AnyFsDescription::LocalDir(_) => self
                .local_dir_list
                .get(&fs.id)
                .and_then(|fs| fs.as_any().downcast_ref::<Mutex<FileSystem<Backend>>>()),
            AnyFsDescription::Nextcloud(_) => self
                .nextcloud_list
                .get(&fs.id)
                .and_then(|fs| fs.as_any().downcast_ref::<Mutex<FileSystem<Backend>>>()),
        }
    }

    fn remove_fs(&mut self, fs: &AnyFsRef) -> bool {
        match fs.description() {
            AnyFsDescription::LocalDir(_) => self.local_dir_list.remove(&fs.id).is_some(),
            AnyFsDescription::Nextcloud(_) => self.nextcloud_list.remove(&fs.id).is_some(),
        }
    }

    fn get_sync<LocalBackend: FSBackend + 'static, RemoteBackend: FSBackend + 'static>(
        &self,
        synchro: &AnySynchroRef,
    ) -> Option<SynchroMutex<LocalBackend, RemoteBackend>> {
        let local = self.get_fs::<LocalBackend>(&synchro.local)?;
        let remote = self.get_fs::<RemoteBackend>(&synchro.remote)?;

        Some(SynchroMutex { local, remote })
    }

    /// Resolves a conflict on a synchro in the list by applying the update from the chose side
    pub async fn resolve_conflict_sync<
        LocalBackend: FSBackend + 'static,
        RemoteBackend: FSBackend + 'static,
    >(
        &self,
        synchro_lock: &RwLock<AnySynchroRef>,
        path: &VirtualPath,
        side: SynchroSide,
    ) -> Result<(), SyncError> {
        // Wait for synchro to be ready
        loop {
            {
                let mut synchro = synchro_lock.write().await;

                if synchro.status.is_synchronizable() {
                    synchro.status = SynchroStatus::SyncInProgress;
                    break;
                }
            }
            sleep(Duration::from_secs(1)); // TODO: make configurable
        }

        let res = {
            let synchro = synchro_lock.read().await;

            let synchro_mutex = self
                .get_sync::<LocalBackend, RemoteBackend>(&synchro)
                .ok_or_else(|| InvalidSynchro::from(synchro.clone()))
                .unwrap();

            let mut local_fs = synchro_mutex.local.lock().await;
            let mut remote_fs = synchro_mutex.remote.lock().await;

            let mut sync: Synchro<'_, '_, LocalBackend, RemoteBackend> =
                Synchro::new(&mut local_fs, &mut remote_fs);
            sync.resolve_conflict(path, side).await
        };

        match res {
            Ok(status) => {
                let mut synchro = synchro_lock.write().await;
                synchro.status = status.into();
                Ok(())
            }
            Err(err) => {
                let mut synchro = synchro_lock.write().await;
                synchro.status = FullSyncStatus::from(&err).into();
                Err(SynchroFailed {
                    synchro: synchro.to_owned(),
                    source: err,
                }
                .into())
            }
        }
    }

    /// Performs a [`full_sync`] on the provided synchro, that should be in the list
    ///
    /// [`full_sync`]: brume::synchro::Synchro::full_sync
    pub async fn sync_one<LocalBackend: FSBackend + 'static, RemoteBackend: FSBackend + 'static>(
        &self,
        synchro_lock: &RwLock<AnySynchroRef>,
    ) -> Result<(), SyncError> {
        {
            let mut synchro = synchro_lock.write().await;

            // Skip synchro that are already identified as desynchronized until the user fixes it
            if !synchro.status.is_synchronizable() {
                return Ok(());
            }
            synchro.status = SynchroStatus::SyncInProgress;
        }

        let res = {
            let synchro = synchro_lock.read().await;
            debug!("Starting full_sync for Synchro {}", synchro.name);

            let synchro_mutex = self
                .get_sync::<LocalBackend, RemoteBackend>(&synchro)
                .ok_or_else(|| InvalidSynchro::from(synchro.clone()))?;

            let mut local_fs = synchro_mutex.local.lock().await;
            let mut remote_fs = synchro_mutex.remote.lock().await;
            let mut sync = Synchro::new(&mut local_fs, &mut remote_fs);
            sync.full_sync().await
        };

        match res {
            Ok(status) => {
                let mut synchro = synchro_lock.write().await;
                debug!("Synchro {} returned with status: {status:?}", synchro.name);
                synchro.status = status.into();
                Ok(())
            }
            Err(err) => {
                let mut synchro = synchro_lock.write().await;
                error!("Synchro {} returned an error: {err}", synchro.name);
                synchro.status = FullSyncStatus::from(&err).into();
                Err(SynchroFailed {
                    synchro: synchro.to_owned(),
                    source: err,
                }
                .into())
            }
        }
    }

    /// Performs a [`full_sync`] on all the synchro in the list
    ///
    /// [`full_sync`]: brume::synchro::Synchro::full_sync
    pub async fn sync_all(&self) -> Vec<Result<(), SyncError>> {
        let futures: Vec<_> = self
            .synchros
            .values()
            .map(|synchro| async move {
                if matches!(synchro.read().await.state(), SynchroState::Paused) {
                    return Ok(());
                }

                let (local_desc, remote_desc) = {
                    let sync = synchro.read().await;
                    (
                        sync.local.description().clone(),
                        sync.remote.description().clone(),
                    )
                };
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

    /// Sets the state of a synchro in the list
    pub async fn set_state(
        &self,
        id: SynchroId,
        state: SynchroState,
    ) -> Result<(), SynchroModificationError> {
        let sync = self
            .synchros
            .get(&id)
            .ok_or(SynchroModificationError::SynchroNotFound(id))?;

        sync.write().await.state = state;
        Ok(())
    }

    pub async fn get_vfs(&self, id: SynchroId, side: SynchroSide) -> Result<Vfs<()>, SyncError> {
        let synchro = self
            .synchros
            .get(&id)
            .ok_or_else(|| SyncError::SynchroNotFound(id))?
            .read()
            .await;

        let (desc, id) = match side {
            SynchroSide::Local => (synchro.local.description().clone(), synchro.local.id),
            SynchroSide::Remote => (synchro.remote.description().clone(), synchro.remote.id),
        };

        match desc {
            AnyFsDescription::LocalDir(_) => {
                let fs = self
                    .local_dir_list
                    .get(&id)
                    .ok_or_else(|| InvalidSynchro::from(synchro.clone()))?
                    .lock()
                    .await;

                Ok(fs.vfs().into())
            }
            AnyFsDescription::Nextcloud(_) => {
                let fs = self
                    .nextcloud_list
                    .get(&id)
                    .ok_or_else(|| InvalidSynchro::from(synchro.clone()))?
                    .lock()
                    .await;

                Ok(fs.vfs().into())
            }
        }
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

    /// Resolves a conflict on a path inside a synchro, by applying the update from `side`
    pub async fn resolve_conflict(
        &self,
        id: SynchroId,
        path: &VirtualPath,
        side: SynchroSide,
    ) -> Result<(), SyncError> {
        let maps = self.maps.read().await;

        maps.resolve_conflict(id, path, side).await
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

    pub async fn len(&self) -> usize {
        self.read().await.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
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
