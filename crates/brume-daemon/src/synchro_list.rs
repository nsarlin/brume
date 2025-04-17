use std::{any::Any, collections::HashMap, sync::Arc, thread::sleep, time::Duration};

use brume::{
    concrete::{
        local::{LocalDir, LocalSyncInfo},
        nextcloud::{NextcloudFs, NextcloudSyncInfo},
        FSBackend, FsBackendError, Named, ToBytes,
    },
    filesystem::FileSystem,
    synchro::{ConflictResolutionState, FullSyncStatus, Synchro, SynchroSide},
    vfs::{Vfs, VirtualPath},
};
use futures::{future::join_all, stream, StreamExt};
use log::{debug, error, info};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};
use uuid::Uuid;

use brume_daemon_proto::{
    AnyFsCreationInfo, AnyFsDescription, AnyFsRef, AnySynchroCreationInfo, AnySynchroRef,
    SynchroId, SynchroState, SynchroStatus,
};

use crate::db::{Database, DatabaseError};

#[derive(Error, Debug)]
pub enum SyncError {
    #[error("Error during sync process")]
    SyncFailed(#[from] SynchroFailed),
    #[error("Invalid synchro")]
    InvalidSynchroState(#[from] InvalidSynchro),
    #[error("Synchro not found: {0:?}")]
    SynchroNotFound(SynchroId),
    #[error("Database error")]
    Database(#[from] DatabaseError),
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
    #[error("The provided synchro is not of the expected type")]
    InvalidType { expected: String, found: String },
}

impl SynchroCreationError {
    fn invalid_type<Expected: Named, Found: Named>() -> Self {
        Self::InvalidType {
            expected: Expected::TYPE_NAME.to_string(),
            found: Found::TYPE_NAME.to_string(),
        }
    }
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

/// Result of a synchro creation
#[derive(Clone)]
pub struct CreatedSynchro {
    id: SynchroId,
    name: String,
    local_id: Uuid,
    remote_id: Uuid,
}

impl CreatedSynchro {
    pub fn id(&self) -> SynchroId {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn local_id(&self) -> Uuid {
        self.local_id
    }

    pub fn remote_id(&self) -> Uuid {
        self.remote_id
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
/// [`supported types`]: brume_daemon_proto::AnyFsCreationInfo
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

    pub(crate) fn synchro_ref_list_mut(
        &mut self,
    ) -> &mut HashMap<SynchroId, RwLock<AnySynchroRef>> {
        &mut self.synchros
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
                    .insert(fs_ref.id(), Mutex::new(FileSystem::new(concrete)));
                Ok(fs_ref)
            }
            AnyFsCreationInfo::Nextcloud(info) => {
                let concrete = info.try_into()?;
                self.nextcloud_list
                    .insert(fs_ref.id(), Mutex::new(FileSystem::new(concrete)));
                Ok(fs_ref)
            }
        }
    }

    pub(crate) fn insert_existing_fs<SyncInfo: Named + 'static>(
        &mut self,
        fs_info: AnyFsCreationInfo,
        vfs: &Vfs<SyncInfo>,
        id: Uuid,
    ) -> Result<(), SynchroCreationError> {
        match fs_info {
            AnyFsCreationInfo::LocalDir(info) => {
                let concrete = info.try_into().map_err(FsBackendError::from)?;
                let mut fs = FileSystem::new(concrete);
                *fs.vfs_mut() = (vfs as &dyn Any)
                    .downcast_ref::<Vfs<LocalSyncInfo>>()
                    .ok_or_else(|| SynchroCreationError::invalid_type::<LocalSyncInfo, SyncInfo>())?
                    .clone();
                self.local_dir_list.insert(id, Mutex::new(fs));
                Ok(())
            }
            AnyFsCreationInfo::Nextcloud(info) => {
                let concrete = info.try_into().map_err(FsBackendError::from)?;
                let mut fs = FileSystem::new(concrete);
                *fs.vfs_mut() = (vfs as &dyn Any)
                    .downcast_ref::<Vfs<NextcloudSyncInfo>>()
                    .ok_or_else(|| {
                        SynchroCreationError::invalid_type::<NextcloudSyncInfo, SyncInfo>()
                    })?
                    .clone();
                self.nextcloud_list.insert(id, Mutex::new(fs));
                Ok(())
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
            if (sync.local().description() == local_desc
                || sync.local().description() == remote_desc)
                && (sync.remote().description() == local_desc
                    || sync.remote().description() == remote_desc)
            {
                return true;
            }
        }

        false
    }

    async fn name_is_unique(&self, name: &str) -> bool {
        for sync in self.synchros.values() {
            if sync.read().await.name() == name {
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
            .filter(|sync| async { sync.read().await.remote().name() == remote_name })
            .collect()
            .await;

        if same_remote.is_empty() && self.name_is_unique(remote_name).await {
            return remote_name.to_string();
        }

        let same_local: Vec<_> = stream::iter(same_remote)
            .filter(|sync| async { sync.read().await.local().name() == local_name })
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
    ) -> Result<CreatedSynchro, SynchroCreationError> {
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
            self.unique_synchro_name(local_ref.name(), remote_ref.name())
                .await
        };

        info!("Synchro created: name: {name}, id: {id:?}");
        let res = CreatedSynchro {
            id,
            name: name.clone(),
            local_id: local_ref.id(),
            remote_id: remote_ref.id(),
        };

        let synchro = AnySynchroRef::new(local_ref, remote_ref, name);

        self.synchros.insert(id, RwLock::new(synchro.clone()));

        Ok(res)
    }

    /// Deletes a synchronization from the list
    pub fn remove(&mut self, id: SynchroId) -> Result<(), SynchroDeletionError> {
        if let Some(sync) = self.synchros.remove(&id) {
            let mut res = Ok(());
            let sync = sync.into_inner();

            if !self.remove_fs(sync.local()) {
                res = Err(SynchroDeletionError::InvalidSynchroState(
                    sync.clone().into(),
                ));
            }
            if !self.remove_fs(sync.remote()) {
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
        db: &Database,
    ) -> Result<(), SyncError> {
        let synchro = self
            .synchros
            .get(&id)
            .ok_or_else(|| SyncError::SynchroNotFound(id))?;
        let local_desc = synchro.read().await.local().description().clone();
        let remote_desc = synchro.read().await.remote().description().clone();
        match (local_desc, remote_desc) {
            (AnyFsDescription::LocalDir(_), AnyFsDescription::LocalDir(_)) => {
                self.resolve_conflict_sync::<LocalDir, LocalDir>(id, synchro, path, side, db)
                    .await
            }
            (AnyFsDescription::LocalDir(_), AnyFsDescription::Nextcloud(_)) => {
                self.resolve_conflict_sync::<LocalDir, NextcloudFs>(id, synchro, path, side, db)
                    .await
            }
            (AnyFsDescription::Nextcloud(_), AnyFsDescription::LocalDir(_)) => {
                self.resolve_conflict_sync::<NextcloudFs, LocalDir>(id, synchro, path, side, db)
                    .await
            }
            (AnyFsDescription::Nextcloud(_), AnyFsDescription::Nextcloud(_)) => {
                self.resolve_conflict_sync::<NextcloudFs, NextcloudFs>(id, synchro, path, side, db)
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
                .get(&fs.id())
                .and_then(|fs| fs.as_any().downcast_ref::<Mutex<FileSystem<Backend>>>()),
            AnyFsDescription::Nextcloud(_) => self
                .nextcloud_list
                .get(&fs.id())
                .and_then(|fs| fs.as_any().downcast_ref::<Mutex<FileSystem<Backend>>>()),
        }
    }

    fn remove_fs(&mut self, fs: &AnyFsRef) -> bool {
        match fs.description() {
            AnyFsDescription::LocalDir(_) => self.local_dir_list.remove(&fs.id()).is_some(),
            AnyFsDescription::Nextcloud(_) => self.nextcloud_list.remove(&fs.id()).is_some(),
        }
    }

    fn get_sync<LocalBackend: FSBackend + 'static, RemoteBackend: FSBackend + 'static>(
        &self,
        synchro: &AnySynchroRef,
    ) -> Option<SynchroMutex<LocalBackend, RemoteBackend>> {
        let local = self.get_fs::<LocalBackend>(synchro.local())?;
        let remote = self.get_fs::<RemoteBackend>(synchro.remote())?;

        Some(SynchroMutex { local, remote })
    }

    /// Resolves a conflict on a synchro in the list by applying the update from the chosen side
    pub async fn resolve_conflict_sync<
        LocalBackend: FSBackend + 'static,
        RemoteBackend: FSBackend + 'static,
    >(
        &self,
        id: SynchroId,
        synchro_lock: &RwLock<AnySynchroRef>,
        path: &VirtualPath,
        side: SynchroSide,
        db: &Database,
    ) -> Result<(), SyncError>
    where
        LocalBackend::SyncInfo: ToBytes,
        RemoteBackend::SyncInfo: ToBytes,
    {
        // Wait for synchro to be ready
        loop {
            {
                let mut synchro = synchro_lock.write().await;

                if synchro.status().is_synchronizable() {
                    let status = SynchroStatus::SyncInProgress;
                    synchro.set_status(status);
                    db.set_synchro_status(id, status).await?;
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
            Ok(conflict_result) => {
                let status = conflict_result.status().into();
                synchro_lock.write().await.set_status(status);
                db.set_synchro_status(id, status).await?;

                match conflict_result.state() {
                    ConflictResolutionState::Local(node_state) => {
                        db.update_vfs_node_state(
                            synchro_lock.read().await.local().id(),
                            path,
                            node_state,
                        )
                        .await?
                    }
                    ConflictResolutionState::Remote(node_state) => {
                        db.update_vfs_node_state(
                            synchro_lock.read().await.remote().id(),
                            path,
                            node_state,
                        )
                        .await?
                    }
                    ConflictResolutionState::None => {
                        db.delete_vfs_node(synchro_lock.read().await.local().id(), path)
                            .await?
                    }
                }
                Ok(())
            }

            Err(err) => {
                let mut synchro = synchro_lock.write().await;
                let status = FullSyncStatus::from(&err).into();
                synchro.set_status(status);
                db.set_synchro_status(id, status).await?;
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
        id: SynchroId,
        synchro_lock: &RwLock<AnySynchroRef>,
        db: &Database,
    ) -> Result<(), SyncError>
    where
        LocalBackend::SyncInfo: ToBytes,
        RemoteBackend::SyncInfo: ToBytes,
    {
        {
            let mut synchro = synchro_lock.write().await;

            // Skip synchro that are already identified as desynchronized until the user fixes it
            if !synchro.status().is_synchronizable() {
                return Ok(());
            }
            let status = SynchroStatus::SyncInProgress;
            synchro.set_status(status);
            db.set_synchro_status(id, status).await?;
        }

        let res = {
            let synchro = synchro_lock.read().await;
            debug!("Starting full_sync for Synchro {}", synchro.name());

            let synchro_mutex = self
                .get_sync::<LocalBackend, RemoteBackend>(&synchro)
                .ok_or_else(|| InvalidSynchro::from(synchro.clone()))?;

            let mut local_fs = synchro_mutex.local.lock().await;
            let mut remote_fs = synchro_mutex.remote.lock().await;
            let mut sync = Synchro::new(&mut local_fs, &mut remote_fs);
            sync.full_sync().await
        };

        match res {
            Ok(sync_res) => {
                // Propagate updates to the db
                for update in sync_res.local_updates() {
                    db.update_vfs(synchro_lock.read().await.local().id(), update)
                        .await?;
                }

                for update in sync_res.remote_updates() {
                    db.update_vfs(synchro_lock.read().await.remote().id(), update)
                        .await?;
                }

                let status = sync_res.status();
                let mut synchro = synchro_lock.write().await;
                debug!(
                    "Synchro {} returned with status: {status:?}",
                    synchro.name()
                );
                let status = status.into();
                synchro.set_status(status);
                db.set_synchro_status(id, status).await?;
                Ok(())
            }
            Err(err) => {
                let mut synchro = synchro_lock.write().await;
                error!("Synchro {} returned an error: {err}", synchro.name());
                let status = FullSyncStatus::from(&err).into();
                synchro.set_status(status);
                db.set_synchro_status(id, status).await?;
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
    pub async fn sync_all(&self, db: &Database) -> Vec<Result<(), SyncError>> {
        let futures: Vec<_> = self
            .synchros
            .iter()
            .map(|(id, synchro)| async move {
                if matches!(synchro.read().await.state(), SynchroState::Paused) {
                    return Ok(());
                }

                let (local_desc, remote_desc) = {
                    let sync = synchro.read().await;
                    (
                        sync.local().description().clone(),
                        sync.remote().description().clone(),
                    )
                };
                match (local_desc, remote_desc) {
                    (AnyFsDescription::LocalDir(_), AnyFsDescription::LocalDir(_)) => {
                        self.sync_one::<LocalDir, LocalDir>(*id, synchro, db).await
                    }
                    (AnyFsDescription::LocalDir(_), AnyFsDescription::Nextcloud(_)) => {
                        self.sync_one::<LocalDir, NextcloudFs>(*id, synchro, db)
                            .await
                    }
                    (AnyFsDescription::Nextcloud(_), AnyFsDescription::LocalDir(_)) => {
                        self.sync_one::<NextcloudFs, LocalDir>(*id, synchro, db)
                            .await
                    }
                    (AnyFsDescription::Nextcloud(_), AnyFsDescription::Nextcloud(_)) => {
                        self.sync_one::<NextcloudFs, NextcloudFs>(*id, synchro, db)
                            .await
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

        sync.write().await.set_state(state);
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
            SynchroSide::Local => (synchro.local().description().clone(), synchro.local().id()),
            SynchroSide::Remote => (
                synchro.remote().description().clone(),
                synchro.remote().id(),
            ),
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

impl From<SynchroList> for ReadWriteSynchroList {
    fn from(value: SynchroList) -> Self {
        Self {
            maps: Arc::new(RwLock::new(value)),
        }
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
    ) -> Result<CreatedSynchro, SynchroCreationError> {
        self.maps.write().await.insert(sync_info).await
    }

    /// Deletes a synchronization from the list
    pub async fn remove(&self, id: SynchroId) -> Result<(), SynchroDeletionError> {
        self.maps.write().await.remove(id)
    }

    /// Forces a synchro of all the filesystems in the list, and updates the db accordingly
    pub async fn sync_all(&self, db: &Database) -> Vec<Result<(), SyncError>> {
        let maps = self.maps.read().await;

        maps.sync_all(db).await
    }

    /// Resolves a conflict on a path inside a synchro, by applying the update from `side`.
    /// Updates the db accordingly
    pub async fn resolve_conflict(
        &self,
        id: SynchroId,
        path: &VirtualPath,
        side: SynchroSide,
        db: &Database,
    ) -> Result<(), SyncError> {
        let maps = self.maps.read().await;

        maps.resolve_conflict(id, path, side, db).await
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
    use brume_daemon_proto::{AnyFsCreationInfo, AnySynchroCreationInfo};

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

        let id1 = list.insert(sync1).await.unwrap().id();

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
