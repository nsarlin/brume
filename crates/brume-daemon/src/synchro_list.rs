use std::{collections::HashMap, sync::Arc, thread::sleep, time::Duration};

use brume::{
    concrete::{FsBackendError, local::LocalDir, nextcloud::Nextcloud},
    filesystem::FileSystem,
    synchro::{FullSyncResult, FullSyncStatus, Synchro, SynchroSide, Synchronized},
    vfs::{StatefulVfs, VirtualPath},
};
use futures::{StreamExt, future::join_all, stream};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, RwLockReadGuard};
use tracing::{debug, error, info, instrument};
use uuid::Uuid;

use brume_daemon_proto::{
    AnyFsCreationInfo, AnyFsDescription, AnySynchroCreationInfo, FileSystemMeta, SynchroId,
    SynchroMeta, SynchroState, SynchroStatus,
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
    synchro: SynchroMeta,
    source: brume::Error,
}

#[derive(Error, Debug)]
#[error("Synchro in invalid state: {synchro}")]
pub struct InvalidSynchro {
    synchro: SynchroMeta,
}

impl From<SynchroMeta> for InvalidSynchro {
    fn from(value: SynchroMeta) -> Self {
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

/// Similar to [`AnySynchroCreationInfo`], but the name has been chosen and can't be empty
#[derive(Clone)]
pub struct NamedSynchroCreationInfo {
    local: AnyFsCreationInfo,
    remote: AnyFsCreationInfo,
    name: String,
}

impl From<NamedSynchroCreationInfo> for SynchroMeta {
    fn from(value: NamedSynchroCreationInfo) -> Self {
        let local_meta = value.local.into();
        let remote_meta = value.remote.into();

        Self::new(local_meta, remote_meta, value.name)
    }
}

/// Creates a Box<dyn Synchronized> (effectively a dyn typed Synchro<Local, Remote>) from a
/// NamedSynchroCreationInfo.
///
/// This should be called with the list of filesystem types (see example call below)
///
/// The macro will create a nested match for all supported combinations of filesystem types.
/// This uses the fact that the [`AnyFsCreationInfo`] variants have the same name as the various
/// concrete filesystem types.
macro_rules! generate_synchro_constructor {
    // Entry point
    ($ab:tt) => {
        pub fn instantiate(self) -> Result<Box<dyn Synchronized + Send + Sync>, FsBackendError> {
            let local_info = self.local;
            let remote_info = self.remote;
            generate_synchro_constructor!(@outer local_info, remote_info, $ab, $ab)
        }
    };

    // Outer match, matching the local fs type
    (@outer $local_info:ident, $remote_info:ident, [$($local_ty:ident),* $(,)?], $remote_ty:tt) => {
        match $local_info {
            $(
                AnyFsCreationInfo::$local_ty(info) => {
                    let concrete: $local_ty = info.try_into()?;
                    let local = FileSystem::new(concrete);
                    generate_synchro_constructor!(@inner local, $remote_info, $remote_ty)
                }
            )*
        }
    };

    // Inner match, matching the remote fs type
    (@inner $local_fs:ident, $remote_info:ident, [$($remote_ty:ident),* $(,)?]) => {
        match $remote_info {
            $(
                AnyFsCreationInfo::$remote_ty(info) => {
                    let concrete: $remote_ty = info.try_into()?;
                    let remote = FileSystem::new(concrete);
                    Ok(Box::new(Synchro::new($local_fs, remote)))
                },
            )*
        }
    }
}

impl NamedSynchroCreationInfo {
    pub fn new(local: AnyFsCreationInfo, remote: AnyFsCreationInfo, name: String) -> Self {
        Self {
            local,
            remote,
            name,
        }
    }

    generate_synchro_constructor!([LocalDir, Nextcloud]);

    pub fn name(&self) -> &str {
        &self.name
    }
}

/// A synchro that has been inserted in the list
struct RegisteredSynchro {
    synchro: Mutex<Box<dyn Synchronized + Send + Sync>>,
    meta: SynchroMeta,
}

impl TryFrom<NamedSynchroCreationInfo> for RegisteredSynchro {
    fn try_from(value: NamedSynchroCreationInfo) -> Result<Self, Self::Error> {
        let meta = value.clone().into();
        let synchro = Mutex::new(value.instantiate()?);

        Ok(Self { synchro, meta })
    }

    type Error = FsBackendError;
}

impl RegisteredSynchro {
    pub fn name(&self) -> &str {
        self.meta.name()
    }

    pub fn local(&self) -> &FileSystemMeta {
        self.meta.local()
    }

    pub fn remote(&self) -> &FileSystemMeta {
        self.meta.remote()
    }

    /// Returns the status of this synchro
    pub fn status(&self) -> SynchroStatus {
        self.meta.status()
    }

    /// Updates the status of this synchro
    pub fn set_status(&mut self, status: SynchroStatus) {
        self.meta.set_status(status)
    }

    /// Returns the state of this synchro
    pub fn state(&self) -> SynchroState {
        self.meta.state()
    }

    /// Updates the state of this synchro
    pub fn set_state(&mut self, state: SynchroState) {
        self.meta.set_state(state)
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
    synchros: HashMap<SynchroId, RwLock<RegisteredSynchro>>,
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

    /// Returns a list of the metadata for all the synchros
    pub async fn synchro_meta_list(&self) -> HashMap<SynchroId, SynchroMeta> {
        join_all(
            self.synchros
                .iter()
                .map(async |(id, sync)| (*id, sync.read().await.meta.clone())),
        )
        .await
        .into_iter()
        .collect()
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
            let new_name = format!("{name}{counter}");
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

    /// Find a locally unique name for a Synchro before its creation
    pub async fn name_synchro(
        &self,
        sync_info: AnySynchroCreationInfo,
    ) -> NamedSynchroCreationInfo {
        let local = sync_info.local().clone();
        let remote = sync_info.remote().clone();
        let name = if let Some(name) = sync_info.name() {
            self.make_unique_name(name).await
        } else {
            let local_desc = AnyFsDescription::from(local.clone());
            let remote_desc = AnyFsDescription::from(remote.clone());
            self.unique_synchro_name(local_desc.name(), remote_desc.name())
                .await
        };

        NamedSynchroCreationInfo {
            local,
            remote,
            name,
        }
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
        let named_info = self.name_synchro(sync_info).await;
        let name = named_info.name().to_string();

        let synchro: RegisteredSynchro = named_info.try_into()?;

        info!("Synchro created: name: {name}, id: {id:?}");
        let res = CreatedSynchro {
            id,
            name: name.clone(),
            local_id: synchro.meta.local().id(),
            remote_id: synchro.meta.remote().id(),
        };

        self.synchros.insert(id, RwLock::new(synchro));

        Ok(res)
    }

    /// Insert an already created synchro in the list
    pub fn insert_existing(
        &mut self,
        id: SynchroId,
        synchro: Box<dyn Synchronized + Send + Sync + 'static>,
        meta: SynchroMeta,
    ) {
        let registered = RegisteredSynchro {
            synchro: Mutex::new(synchro),
            meta,
        };
        self.synchros.insert(id, RwLock::new(registered));
    }

    /// Deletes a synchronization from the list
    #[allow(clippy::result_large_err)]
    pub fn remove(&mut self, id: SynchroId) -> Result<(), SynchroDeletionError> {
        if self.synchros.remove(&id).is_some() {
            info!("Synchro deleted: {id:?}");
            Ok(())
        } else {
            Err(SynchroDeletionError::SynchroNotFound(id))
        }
    }

    /// Resolves a conflict on a synchro in the list by applying the update from the chosen side
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

        // TODO: what to do if synchro is paused? Store the request in a queue or process it
        // immediately? Or return an error?
        Self::resolve_conflict_sync(id, synchro, path, side, db).await
    }

    /// Resolves a conflict on a synchro in the list by applying the update from the chosen side
    async fn resolve_conflict_sync(
        id: SynchroId,
        synchro_lock: &RwLock<RegisteredSynchro>,
        path: &VirtualPath,
        side: SynchroSide,
        db: &Database,
    ) -> Result<(), SyncError> {
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
            let mut mutex = synchro.synchro.lock().await;

            mutex.resolve_conflict(path, side).await
        };

        Self::handle_sync_result(id, res, synchro_lock, db).await
    }

    /// Completely resync a Synchro. The filesystems will be scanned again and updates will be
    /// propagated
    pub async fn force_resync(&self, id: SynchroId, db: &Database) -> Result<(), SyncError> {
        let synchro_lock = self
            .synchros
            .get(&id)
            .ok_or_else(|| SyncError::SynchroNotFound(id))?;

        loop {
            {
                let mut synchro = synchro_lock.write().await;

                // Here the synchro might be in the "Desync" state
                if synchro.status() != SynchroStatus::SyncInProgress {
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

            db.reset_vfs(synchro.meta.local().id()).await?;
            db.reset_vfs(synchro.meta.remote().id()).await?;

            let mut mutex = synchro.synchro.lock().await;

            mutex.force_resync().await
        };

        Self::handle_sync_result(id, res, synchro_lock, db).await
    }

    /// Handles the result of a sync, updating the database and the synchro status
    async fn handle_sync_result(
        id: SynchroId,
        res: Result<FullSyncResult, brume::Error>,
        synchro_lock: &RwLock<RegisteredSynchro>,
        db: &Database,
    ) -> Result<(), SyncError> {
        match res {
            Ok(sync_res) => {
                let mut status = sync_res.status();

                // Propagate updates to the db
                let res = db
                    .apply_sync_updates(
                        sync_res.local_updates(),
                        synchro_lock.read().await.local().id(),
                        sync_res.remote_updates(),
                        synchro_lock.read().await.remote().id(),
                    )
                    .await
                    .map_err(|e| {
                        status = FullSyncStatus::Desync;
                        e.into()
                    });

                let mut synchro = synchro_lock.write().await;
                debug!(
                    "Synchro {} returned with status: {status:?}",
                    synchro.name()
                );
                let status = status.into();
                synchro.set_status(status);
                db.set_synchro_status(id, status).await?;
                res
            }
            Err(err) => {
                let mut synchro = synchro_lock.write().await;
                error!("Synchro {} returned an error: {err}", synchro.name());
                let status = FullSyncStatus::from(&err).into();
                synchro.set_status(status);
                db.set_synchro_status(id, status).await?;
                Err(SynchroFailed {
                    synchro: synchro.meta.clone(),
                    source: err,
                }
                .into())
            }
        }
    }

    /// Performs a [`full_sync`] on the provided synchro, that should be in the list
    ///
    /// [`full_sync`]: brume::synchro::Synchro::full_sync
    #[instrument(skip_all, fields(sync_id = %id))]
    async fn sync_one(
        id: SynchroId,
        synchro_lock: &RwLock<RegisteredSynchro>,
        db: &Database,
    ) -> Result<(), SyncError> {
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

            let mut mutex = synchro.synchro.lock().await;

            mutex.full_sync().await
        };

        Self::handle_sync_result(id, res, synchro_lock, db).await
    }

    /// Performs a [`full_sync`] on all the synchro in the list
    ///
    /// [`full_sync`]: brume::synchro::Synchro::full_sync
    pub async fn sync_all(&self, db: &Database) -> Vec<Result<(), SyncError>> {
        let futures: Vec<_> = self
            .synchros
            .iter()
            .map(async move |(id, synchro)| {
                if matches!(synchro.read().await.state(), SynchroState::Paused) {
                    return Ok(());
                }

                Self::sync_one(*id, synchro, db).await
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

    /// Returns the [`Vfs`] associated with the provided synchro and side
    ///
    /// The SyncInfo will be erased but the directory structure and metadata will be preserved
    ///
    /// [`Vfs`]: brume::vfs::Vfs
    pub async fn get_vfs(
        &self,
        id: SynchroId,
        side: SynchroSide,
    ) -> Result<StatefulVfs<()>, SyncError> {
        let synchro = self
            .synchros
            .get(&id)
            .ok_or_else(|| SyncError::SynchroNotFound(id))?
            .read()
            .await;

        let synchro_lock = synchro.synchro.lock().await;
        match side {
            SynchroSide::Local => Ok(synchro_lock.local_vfs()),
            SynchroSide::Remote => Ok(synchro_lock.remote_vfs()),
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
    #[instrument(skip_all)]
    pub async fn sync_all(&self, db: &Database) -> Vec<Result<(), SyncError>> {
        let maps = self.maps.read().await;

        maps.sync_all(db).await
    }

    /// Force resync a Synchro from the list
    pub async fn force_resync(&self, id: SynchroId, db: &Database) -> Result<(), SyncError> {
        self.maps.write().await.force_resync(id, db).await
    }

    /// Resolves a conflict on a path inside a synchro, by applying the update from `side`.
    /// Updates the db accordingly
    #[instrument(skip_all)]
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
    use brume::{
        concrete::{local::LocalDirCreationInfo, nextcloud::NextcloudFsCreationInfo},
        update::VfsUpdate,
    };
    use brume_daemon_proto::{AnyFsCreationInfo, AnySynchroCreationInfo, VirtualPathBuf};

    use crate::db::DatabaseConfig;

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
        let meta_list = list.synchro_meta_list().await;
        assert!(matches!(
            meta_list[&id1].local().description(),
            AnyFsDescription::LocalDir(_),
        ));
        assert!(matches!(
            meta_list[&id1].remote().description(),
            AnyFsDescription::LocalDir(_),
        ));

        let nx_a = NextcloudFsCreationInfo::new("https://cloud.com", "user", "user");
        let loc_b = LocalDirCreationInfo::new("/b");
        let sync2 = AnySynchroCreationInfo::new(
            AnyFsCreationInfo::LocalDir(loc_b),
            AnyFsCreationInfo::Nextcloud(nx_a),
            None,
        );

        let id2 = list.insert(sync2).await.unwrap().id();

        assert_eq!(list.synchros.len(), 2);
        let meta_list = list.synchro_meta_list().await;
        assert!(matches!(
            meta_list[&id1].local().description(),
            AnyFsDescription::LocalDir(_),
        ));
        assert!(matches!(
            meta_list[&id1].remote().description(),
            AnyFsDescription::LocalDir(_),
        ));
        assert!(matches!(
            meta_list[&id2].local().description(),
            AnyFsDescription::LocalDir(_),
        ));
        assert!(matches!(
            meta_list[&id2].remote().description(),
            AnyFsDescription::Nextcloud(_),
        ));

        list.remove(id1).unwrap();

        assert_eq!(list.synchros.len(), 1);
        assert!(matches!(
            meta_list[&id1].local().description(),
            AnyFsDescription::LocalDir(_),
        ));
        assert!(matches!(
            meta_list[&id1].remote().description(),
            AnyFsDescription::LocalDir(_),
        ));
    }

    /// This test checks manual error recovery with force_resync.
    /// We simulate a db corruption that triggers and error and then use force_resync to recover
    /// from it.
    #[tokio::test]
    async fn test_force_resync() {
        let db = Database::new(&DatabaseConfig::InMemory).await.unwrap();
        let mut list = SynchroList::new();

        // Create 2 folders that will be synchronized
        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();

        let loc_a = LocalDirCreationInfo::new(dir_a.path());
        let loc_b = LocalDirCreationInfo::new(dir_b.path());
        let sync = AnySynchroCreationInfo::new(
            AnyFsCreationInfo::LocalDir(loc_a),
            AnyFsCreationInfo::LocalDir(loc_b),
            None,
        );

        let created = list.insert(sync.clone()).await.unwrap();
        let id = created.id();
        db.insert_synchro(created, sync).await.unwrap();

        // Update some files in the folders
        std::fs::create_dir(dir_a.path().to_path_buf().join("testdir")).unwrap();
        let content_a = b"Hello, world";
        std::fs::write(
            dir_a.path().to_path_buf().join("testdir/testfile"),
            content_a,
        )
        .unwrap();

        std::fs::create_dir(dir_b.path().to_path_buf().join("testdir")).unwrap();
        std::fs::create_dir(dir_b.path().to_path_buf().join("testdir").join("toasty")).unwrap();
        let content_b = b"Cruel World";
        std::fs::write(
            dir_b.path().to_path_buf().join("testdir/anotherfile"),
            content_b,
        )
        .unwrap();

        // Propagate them
        let res = list.sync_all(&db).await;
        assert!(res[0].is_ok());

        assert_eq!(list.synchros[&id].read().await.status(), SynchroStatus::Ok);

        // Modify a file again
        let content_b = b"Sad World";
        std::fs::write(
            dir_b.path().to_path_buf().join("testdir/anotherfile"),
            content_b,
        )
        .unwrap();

        // Simulate a db corruption
        let fs_b = list.synchros[&id].read().await.remote().id();
        let file_rm = VfsUpdate::FileRemoved(VirtualPathBuf::new("/testdir/anotherfile").unwrap());
        db.update_vfs(fs_b, &file_rm).await.unwrap();

        let res = list.sync_all(&db).await;
        assert!(res[0].is_err());

        assert_eq!(
            list.synchros[&id].read().await.status(),
            SynchroStatus::Desync
        );

        list.force_resync(id, &db).await.unwrap();
        assert_eq!(list.synchros[&id].read().await.status(), SynchroStatus::Ok);

        // Check filesystem content
        let concrete_a: LocalDir = LocalDirCreationInfo::new(dir_a.path()).try_into().unwrap();
        let mut fs_a = FileSystem::new(concrete_a);

        let concrete_b: LocalDir = LocalDirCreationInfo::new(dir_b.path()).try_into().unwrap();
        let mut fs_b = FileSystem::new(concrete_b);

        fs_a.diff_vfs().await.unwrap();
        fs_b.diff_vfs().await.unwrap();
        assert!(fs_a.vfs().structural_eq(fs_b.vfs()));
    }
}
