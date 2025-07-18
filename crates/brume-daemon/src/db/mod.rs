//! The sqlite database that is use to persist the state of the daemon

use std::error::Error;
use std::path::Path;

use brume::concrete::InvalidBytesSyncInfo;
use brume_daemon_proto::config::DatabaseUserConfig;
use deadpool_diesel::PoolError;
use deadpool_diesel::sqlite::{Hook, HookError};
use deadpool_diesel::{
    Runtime,
    sqlite::{Manager, Pool},
};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel_migrations::{EmbeddedMigrations, MigrationHarness, embed_migrations};
use futures::future::try_join;
use thiserror::Error;
use uuid::Uuid;

use brume_daemon_proto::{
    AnyFsCreationInfo, AnySynchroCreationInfo, FileSystemMeta, SynchroId, SynchroMeta,
    SynchroState, SynchroStatus,
};

use crate::daemon::DataPath;
use crate::synchro_list::NamedSynchroCreationInfo;
use crate::{
    schema::{filesystems, synchros},
    synchro_list::{CreatedSynchro, SynchroList},
};

pub mod vfs;

/// Information loaded from a filesystem in the db
#[derive(Queryable, Selectable, Identifiable)]
#[diesel(table_name = filesystems)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
struct DbFileSystem {
    id: i32,
    uuid: Vec<u8>,
    creation_info: Vec<u8>,
}

/// Information needed to insert a new filesystem in the db
#[derive(Insertable)]
#[diesel(table_name = filesystems)]
struct DbNewFileSystem<'a> {
    uuid: &'a [u8],
    creation_info: &'a [u8],
    root_node: i32,
}

/// Information loaded from a synchro in the db
#[derive(Queryable, Selectable, Identifiable)]
#[diesel(table_name = synchros)]
#[diesel(check_for_backend(diesel::sqlite::Sqlite))]
struct DbSynchro {
    id: i32,
    uuid: Vec<u8>,
    name: String,
    local_fs: i32,
    remote_fs: i32,
    state: String,
    status: String,
}

/// Information needed to insert a new synchro in the db
#[derive(Insertable)]
#[diesel(table_name = synchros)]
struct DbNewSynchro<'a> {
    uuid: &'a [u8],
    name: &'a str,
    local_fs: i32,
    remote_fs: i32,
    status: &'a str,
    state: &'a str,
}

/// Metadata retrevied from a loaded filesystem in the db, allows to re-create the concrete FS.
///
/// Does not hold the Vfs
#[derive(Debug, Clone)]
pub struct LoadedFileSystem {
    uuid: Uuid,
    creation_info: AnyFsCreationInfo,
}

impl From<LoadedFileSystem> for FileSystemMeta {
    fn from(value: LoadedFileSystem) -> Self {
        Self::new(value.uuid, value.creation_info.into())
    }
}

impl From<LoadedFileSystem> for AnyFsCreationInfo {
    fn from(value: LoadedFileSystem) -> Self {
        value.creation_info
    }
}

/// A connection to the database
pub struct Database {
    pool: Pool,
}

// Loads migrations from the sql in crates/brume-daemon/migrations
const MIGRATIONS: EmbeddedMigrations = embed_migrations!();

const DB_DEFAULT_FILENAME: &str = "brume.sqlite";

/// The config used for the daemon database
#[derive(Debug, Clone)]
pub enum DatabaseConfig {
    InMemory,
    OnDisk(DataPath),
}

impl From<DatabaseUserConfig> for DatabaseConfig {
    fn from(value: DatabaseUserConfig) -> Self {
        match value {
            DatabaseUserConfig::InMemory => Self::InMemory,
            DatabaseUserConfig::OnDisk => Self::OnDisk(DataPath::new::<&str>(None)),
            DatabaseUserConfig::OnDiskPath(path_buf) => Self::OnDisk(DataPath::new(Some(path_buf))),
        }
    }
}

impl DatabaseConfig {
    pub fn to_string_lossy(&self) -> Option<String> {
        match self {
            DatabaseConfig::InMemory => Some(String::from(":memory:")),
            DatabaseConfig::OnDisk(path_buf) => Some(
                path_buf
                    .resolve(DB_DEFAULT_FILENAME)?
                    .as_os_str()
                    .to_string_lossy()
                    .to_string(),
            ),
        }
    }

    pub fn new_ondisk<P: AsRef<Path>>(path: Option<P>) -> Self {
        Self::OnDisk(DataPath::new(path))
    }

    pub fn new_inmemory() -> Self {
        Self::InMemory
    }
}

#[derive(Error, Debug)]
#[error("Failed to create database")]
pub enum DatabaseCreationError {
    #[error(
        "Invalid path: {0}. Try to configure it to a path that you can write to or set the $XDG_DATA_HOME environment variable"
    )]
    InvalidDbPath(String),
    #[error("Failed to update db to the latest schema")]
    MigrationError(#[source] Box<dyn Error + Send + Sync>),
}

#[derive(Error, Debug)]
#[error("Failed to create database")]
pub enum DatabaseError {
    #[error("failed to connect to the database")]
    ConnectionError(#[from] PoolError),
    #[error("Invalid database state")]
    InvalidState(#[from] diesel::result::Error),
    #[error("Data value found in the database in table {table} column {column} is not valid")]
    InvalidData {
        table: String,
        column: String,
        source: Option<Box<dyn Error + Send + Sync>>,
    },
    #[error("Failed to serialize data before database insert")]
    SerializationError(#[from] bincode::Error),
}

impl DatabaseError {
    fn invalid_data(
        table: &str,
        column: &str,
        source: Option<Box<dyn Error + Send + Sync>>,
    ) -> Self {
        Self::InvalidData {
            table: table.to_string(),
            column: column.to_string(),
            source,
        }
    }
}

impl From<InvalidBytesSyncInfo> for DatabaseError {
    fn from(value: InvalidBytesSyncInfo) -> Self {
        Self::InvalidData {
            column: "state".to_string(),
            table: "nodes".to_string(),
            source: Some(Box::new(value)),
        }
    }
}

impl Database {
    /// Creates a new empty database from the config
    pub async fn new(config: &DatabaseConfig) -> Result<Self, DatabaseCreationError> {
        let db_str = config.to_string_lossy().ok_or_else(|| {
            DatabaseCreationError::InvalidDbPath(
                config
                    .to_string_lossy()
                    .unwrap_or(String::from("*undefined*")),
            )
        })?;
        let manager = Manager::new(db_str, Runtime::Tokio1);

        // Pool size is set to 1 because most operations will be writes, and sqlite is not
        // well suited for concurrent accesses
        // Ok to unwrap because this cannot fail if a runtime is provided
        let pool = Pool::builder(manager)
            .post_create(Hook::async_fn(|conn, _| {
                Box::pin(async move {
                    conn.interact(|conn| {
                        // Enable foreign keys to allow cascading deletion
                        conn.batch_execute("PRAGMA foreign_keys = ON;")
                    })
                    .await
                    .unwrap() // This should never fail unless the inner closure panics
                    .map_err(|e| HookError::Backend(e.into()))
                })
            }))
            .max_size(1)
            .build()
            .unwrap();

        let conn = pool.get().await.map_err(|_| {
            DatabaseCreationError::InvalidDbPath(
                config
                    .to_string_lossy()
                    .unwrap_or(String::from("*undefined*")),
            )
        })?;
        conn.interact(|conn| conn.run_pending_migrations(MIGRATIONS).map(|_| ()))
            .await
            .unwrap() // This should never fail unless the inner closure panics
            .map_err(DatabaseCreationError::MigrationError)?;

        Ok(Self { pool })
    }

    /// Loads all the filesystems, regardless of the synchro they belong to
    #[cfg(test)]
    async fn load_all_filesystems(&self) -> Result<Vec<LoadedFileSystem>, DatabaseError> {
        use crate::schema::filesystems::dsl::*;

        let results = {
            let conn = self.pool.get().await?;
            conn.interact(|conn| filesystems.select(DbFileSystem::as_select()).load(conn))
                .await
                .unwrap() // This should never fail unless the inner closure panics
        }?;

        results
            .into_iter()
            .map(|db_fs| {
                Ok(LoadedFileSystem {
                    uuid: Uuid::from_slice(&db_fs.uuid).map_err(|e| {
                        DatabaseError::invalid_data("filesystems", "uuid", Some(Box::new(e)))
                    })?,
                    creation_info: bincode::deserialize(&db_fs.creation_info).map_err(|e| {
                        DatabaseError::invalid_data(
                            "filesystems",
                            "creation_info",
                            Some(Box::new(e)),
                        )
                    })?,
                })
            })
            .collect()
    }

    /// Loads a single filesystem from its id
    async fn load_filesystem_from_id(&self, fs_id: i32) -> Result<LoadedFileSystem, DatabaseError> {
        use crate::schema::filesystems::dsl::*;

        let db_fs = {
            let conn = self.pool.get().await?;
            conn.interact(move |conn| {
                filesystems
                    .filter(id.eq(fs_id))
                    .select(DbFileSystem::as_select())
                    .first(conn)
            })
            .await
            .unwrap() // This should never fail unless the inner closure panics
        }?;

        Ok(LoadedFileSystem {
            uuid: Uuid::from_slice(&db_fs.uuid)
                .map_err(|e| DatabaseError::invalid_data("filesystems", "uuid", Some(e.into())))?,
            creation_info: bincode::deserialize(&db_fs.creation_info)?,
        })
    }

    /// Inserts a new filesystem in the db
    pub async fn insert_new_filesystem(
        &self,
        fs_uuid: Uuid,
        fs: &AnyFsCreationInfo,
    ) -> Result<i32, DatabaseError> {
        use crate::schema::filesystems::dsl::*;

        let info = bincode::serialize(fs)?;

        let vfs_root = self.insert_vfs_root().await?;

        let conn = self.pool.get().await?;
        conn.interact(move |conn| {
            let new_fs = DbNewFileSystem {
                uuid: fs_uuid.as_bytes(),
                creation_info: &info,
                root_node: vfs_root,
            };

            diesel::insert_into(filesystems)
                .values(&new_fs)
                .returning(id)
                .get_result(conn)
        })
        .await
        .unwrap() // This should never fail unless the inner closure panics
        .map_err(|e| e.into())
    }

    /// Deletes a single filesystem from the db
    pub async fn delete_filesystem(&self, fs: &FileSystemMeta) -> Result<(), DatabaseError> {
        self.delete_filesystem_from_uuid(fs.id()).await
    }

    async fn delete_filesystem_from_uuid(&self, fs_id: Uuid) -> Result<(), DatabaseError> {
        use crate::schema::filesystems::dsl::*;

        let vfs_root = self.get_vfs_root_id(fs_id).await?;

        {
            let conn = self.pool.get().await?;
            conn.interact(move |conn| {
                diesel::delete(filesystems.filter(uuid.eq(fs_id.as_bytes()))).execute(conn)
            })
            .await
            // This should never fail unless the inner closure panics
            .unwrap()?;
        }

        self.delete_vfs(vfs_root).await
    }

    /// Updates the (status)[`SynchroStatus`] of a synchro
    pub async fn set_synchro_status(
        &self,
        synchro: SynchroId,
        synchro_status: SynchroStatus,
    ) -> Result<(), DatabaseError> {
        use crate::schema::synchros::dsl::*;

        let conn = self.pool.get().await?;
        conn.interact(move |conn| {
            diesel::update(synchros)
                .filter(uuid.eq(synchro.id().as_bytes()))
                .set(status.eq(format!("{synchro_status}")))
                .execute(conn)
        })
        .await
        .unwrap() // This should never fail unless the inner closure panics
        .map_err(|e| e.into())
        .map(|_| ())
    }

    /// Updates the [state](`SynchroState`) of a synchro
    pub async fn set_synchro_state(
        &self,
        synchro: SynchroId,
        synchro_state: SynchroState,
    ) -> Result<(), DatabaseError> {
        use crate::schema::synchros::dsl::*;

        let conn = self.pool.get().await?;
        conn.interact(move |conn| {
            diesel::update(synchros)
                .filter(uuid.eq(synchro.id().as_bytes()))
                .set(state.eq(format!("{synchro_state}")))
                .execute(conn)
        })
        .await
        .unwrap() // This should never fail unless the inner closure panics
        .map_err(|e| e.into())
        .map(|_| ())
    }

    /// Loads a single synchro into the [`SynchroList`]
    async fn load_synchro_to_list(
        &self,
        db_synchro: &DbSynchro,
        synchro_list: &mut SynchroList,
    ) -> Result<(), DatabaseError> {
        let local = self.load_filesystem_from_id(db_synchro.local_fs).await?;
        let local_vfs = self.load_vfs(local.uuid).await?;
        let remote = self.load_filesystem_from_id(db_synchro.remote_fs).await?;
        let remote_vfs = self.load_vfs(remote.uuid).await?;
        let synchro_info = NamedSynchroCreationInfo::new(
            local.creation_info.clone(),
            remote.creation_info.clone(),
            db_synchro.name.clone(),
        );

        let mut meta = SynchroMeta::new(local.into(), remote.into(), db_synchro.name.clone());
        meta.set_state(
            db_synchro
                .state
                .as_str()
                .try_into()
                .map_err(|_| DatabaseError::invalid_data("synchros", "state", None))?,
        );
        meta.set_status(
            db_synchro
                .status
                .as_str()
                .try_into()
                .map_err(|_| DatabaseError::invalid_data("synchros", "status", None))?,
        );

        let synchro_id = Uuid::from_slice(db_synchro.uuid.as_slice())
            .map_err(|e| DatabaseError::invalid_data("synchros", "uuid", Some(Box::new(e))))?;

        let mut synchro = synchro_info
            .instantiate()
            .map_err(|e| DatabaseError::invalid_data("nodes", "*", Some(Box::new(e))))?;

        synchro.set_local_vfs(local_vfs)?;
        synchro.set_remote_vfs(remote_vfs)?;

        synchro_list.insert_existing(synchro_id.into(), synchro, meta);
        Ok(())
    }

    /// Loads all the Synchros from the DB and create a new [`SynchroList`]
    pub async fn load_all_synchros(&self) -> Result<SynchroList, DatabaseError> {
        use crate::schema::synchros::dsl::*;

        let mut list = SynchroList::new();

        let db_synchros = {
            let conn = self.pool.get().await?;
            conn.interact(|conn| synchros.select(DbSynchro::as_select()).load(conn))
                .await
                .unwrap() // This should never fail unless the inner closure panics
        }?;

        for synchro in db_synchros {
            // TODO: check for situations where the loaded synchro might be invalid:
            // - SyncInProgress
            // - ?

            self.load_synchro_to_list(&synchro, &mut list).await?;
        }

        Ok(list)
    }

    /// Insert a new Synchro in the DB
    pub async fn insert_synchro(
        &self,
        created: CreatedSynchro,
        info: AnySynchroCreationInfo,
    ) -> Result<i32, DatabaseError> {
        use crate::schema::synchros::dsl::*;

        let (local_db_id, remote_db_id) = try_join(
            self.insert_new_filesystem(created.local_id(), info.local()),
            self.insert_new_filesystem(created.remote_id(), info.remote()),
        )
        .await?;

        let created_id = created.id();

        let conn = self.pool.get().await?;
        conn.interact(move |conn| {
            let new_synchro = DbNewSynchro {
                uuid: created_id.as_bytes(),
                name: created.name(),
                local_fs: local_db_id,
                remote_fs: remote_db_id,
                status: &SynchroStatus::default().to_string(),
                state: &SynchroState::default().to_string(),
            };

            diesel::insert_into(synchros)
                .values(&new_synchro)
                .returning(id)
                .get_result(conn)
        })
        .await
        .unwrap() // This should never fail unless the inner closure panics
        .map_err(|e| e.into())
    }

    /// Deletes a Synchro from the DB
    pub async fn delete_synchro(&self, synchro: SynchroId) -> Result<(), DatabaseError> {
        use crate::schema::synchros::dsl::*;

        let db_synchro = {
            let conn = self.pool.get().await?;
            conn.interact(move |conn| {
                let db_synchro = synchros
                    .filter(uuid.eq(synchro.as_bytes()))
                    .select(DbSynchro::as_select())
                    .get_result(conn)?;

                diesel::delete(synchros.filter(uuid.eq(synchro.as_bytes())))
                    .execute(conn)
                    .map(|_| db_synchro)
            })
            .await
            // This should never fail unless the inner closure panics
            .unwrap()?
        };

        let local = self.load_filesystem_from_id(db_synchro.local_fs).await?;
        self.delete_filesystem_from_uuid(local.uuid).await?;

        let remote = self.load_filesystem_from_id(db_synchro.remote_fs).await?;
        self.delete_filesystem_from_uuid(remote.uuid).await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use brume_daemon_proto::{AnyFsCreationInfo, LocalDirCreationInfo, NextcloudFsCreationInfo};

    use crate::synchro_list::SynchroList;

    use super::*;

    #[tokio::test]
    async fn test_db_filesystem() {
        let db = Database::new(&DatabaseConfig::InMemory).await.unwrap();

        db.load_all_filesystems().await.unwrap();

        let fs_info = AnyFsCreationInfo::LocalDir(LocalDirCreationInfo::new("/tmp/test"));
        let fs_ref = FileSystemMeta::from(fs_info.clone());
        db.insert_new_filesystem(fs_ref.id(), &fs_info)
            .await
            .unwrap();

        let fs_list = db.load_all_filesystems().await.unwrap();
        assert_eq!(fs_list.len(), 1);

        db.delete_filesystem(&fs_ref).await.unwrap();

        let fs_list = db.load_all_filesystems().await.unwrap();
        assert_eq!(fs_list.len(), 0);
    }

    #[tokio::test]
    async fn test_db_synchro() {
        let db = Database::new(&DatabaseConfig::InMemory).await.unwrap();

        let mut list = SynchroList::new();

        let loc_1 = LocalDirCreationInfo::new("/a");
        let rem_1 = NextcloudFsCreationInfo::new("http://localhost", "admin", "admin");
        let sync1 = AnySynchroCreationInfo::new(
            AnyFsCreationInfo::LocalDir(loc_1),
            AnyFsCreationInfo::Nextcloud(rem_1),
            None,
        );

        let created1 = list.insert(sync1.clone()).await.unwrap();

        db.insert_synchro(created1.clone(), sync1).await.unwrap();

        let fs_list = db.load_all_filesystems().await.unwrap();
        assert_eq!(fs_list.len(), 2);
        let sync_list = db.load_all_synchros().await.unwrap();
        assert_eq!(sync_list.len(), 1);

        let loc_2 = LocalDirCreationInfo::new("/b");
        let rem_2 = NextcloudFsCreationInfo::new("http://remote.dir", "admin", "admin");
        let sync2 = AnySynchroCreationInfo::new(
            AnyFsCreationInfo::LocalDir(loc_2),
            AnyFsCreationInfo::Nextcloud(rem_2),
            Some(String::from("2")),
        );

        let created2 = list.insert(sync2.clone()).await.unwrap();
        db.insert_synchro(created2, sync2).await.unwrap();

        let fs_list = db.load_all_filesystems().await.unwrap();
        assert_eq!(fs_list.len(), 4);
        let sync_list = db.load_all_synchros().await.unwrap();
        assert_eq!(sync_list.len(), 2);

        db.delete_synchro(created1.id()).await.unwrap();
        list.remove(created1.id()).unwrap();

        let fs_list = db.load_all_filesystems().await.unwrap();
        assert_eq!(fs_list.len(), 2);
        let sync_list = db.load_all_synchros().await.unwrap();
        assert_eq!(sync_list.len(), 1);
    }
}
