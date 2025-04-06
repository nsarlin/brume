//! Definition of the protocol needed to communicate with the daemon

use std::{collections::HashMap, fmt::Display};

use brume::concrete::{
    local::LocalDir, nextcloud::NextcloudFs, FSBackend, FsInstanceDescription, Named,
};

use brume::synchro::FullSyncStatus;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use brume::concrete::{local::LocalDirCreationInfo, nextcloud::NextcloudFsCreationInfo};
pub use brume::synchro::SynchroSide;
pub use brume::vfs::virtual_path::{VirtualPath, VirtualPathBuf};

/// Name of the socket where the clients should connect
pub const BRUME_SOCK_NAME: &str = "brume.socket";

/// An id that uniquely identify a pair of synchronized FS
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct SynchroId(Uuid);

impl Default for SynchroId {
    fn default() -> Self {
        Self::new()
    }
}

impl SynchroId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn id(&self) -> Uuid {
        self.0
    }

    pub fn short(&self) -> u32 {
        self.0.as_fields().0
    }
}

/// A reference to a filesystem in the SynchroList handled by the brume daemon.
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

    pub fn name(&self) -> &str {
        self.description.name()
    }

    pub fn id(&self) -> Uuid {
        self.id
    }
}

/// Computed status of the synchro, based on synchronization events
///
/// This status is mostly gotten from the [`FullSyncStatus`] returned by [`full_sync`]. When
/// [`full_sync`] is running, the status is set to [`Self::SyncInProgress`]
///
/// [`full_sync`]: brume::synchro::Synchro::full_sync
#[derive(Default, Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SynchroStatus {
    /// No node in any FS is in Conflict or Error state
    #[default]
    Ok,
    /// At least one node is in Conflict state, but no node is in Error state
    Conflict,
    /// At least one node is in Error state
    Error,
    /// There is some inconsistency in one of the Vfs, likely coming from a bug in brume.
    /// User should re-sync the faulty vfs from scratch
    Desync,
    /// A synchronization is in progress
    SyncInProgress,
}

impl Display for SynchroStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<FullSyncStatus> for SynchroStatus {
    fn from(value: FullSyncStatus) -> Self {
        match value {
            FullSyncStatus::Ok => Self::Ok,
            FullSyncStatus::Conflict => Self::Conflict,
            FullSyncStatus::Error => Self::Error,
            FullSyncStatus::Desync => Self::Desync,
        }
    }
}

impl SynchroStatus {
    /// Returns true if the status allows further synchronization
    pub fn is_synchronizable(self) -> bool {
        !matches!(self, SynchroStatus::Desync | SynchroStatus::SyncInProgress)
    }
}

/// User controlled state of the synchro
#[derive(Default, Copy, Clone, Debug, Serialize, Deserialize)]
pub enum SynchroState {
    #[default]
    Running,
    Paused,
}

impl Display for SynchroState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// A [`Synchro`] where the [`Backend`] filesystems are only known at runtime.
///
/// This type represents only an index and needs a valid SynchroList, to actually be used. It
/// must not be used after the Synchro it points to has been removed from the SynchroList.
///
/// [`Backend`]: FSBackend
/// [`Synchro`]: brume::synchro::Synchro
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
    pub fn new(local_ref: AnyFsRef, remote_ref: AnyFsRef, name: String) -> Self {
        AnySynchroRef {
            local: local_ref,
            remote: remote_ref,
            status: SynchroStatus::default(),
            state: SynchroState::default(),
            name,
        }
    }

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
    /// specific point in time and for a specific SynchroList, there should be no collision.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the status of this synchro
    pub fn status(&self) -> SynchroStatus {
        self.status
    }

    /// Updates the status of this synchro
    pub fn set_status(&mut self, status: SynchroStatus) {
        self.status = status
    }

    /// Returns the state of this synchro
    pub fn state(&self) -> SynchroState {
        self.state
    }

    /// Updates the state of this synchro
    pub fn set_state(&mut self, state: SynchroState) {
        self.state = state
    }
}

/// The information needed to create a FS that can be synchronized.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AnyFsCreationInfo {
    LocalDir(<LocalDir as FSBackend>::CreationInfo),
    Nextcloud(<NextcloudFs as FSBackend>::CreationInfo),
}

impl AnyFsCreationInfo {
    pub async fn validate(&self) -> Result<(), String> {
        match self {
            AnyFsCreationInfo::LocalDir(info) => LocalDir::validate(info)
                .await
                .map_err(|_| "Invalid directory for synchronization".to_string()),
            AnyFsCreationInfo::Nextcloud(info) => NextcloudFs::validate(info).await.map_err(|e| {
                let msg = if let Some(msg) = e.protocol_error_message() {
                    msg
                } else {
                    e.to_string()
                };
                format!("Failed to connect to Nextcloud server: {msg}")
            }),
        }
    }
}

/// The information needed to create a new synchro between filesystems
#[derive(Debug, Clone)]
pub struct AnySynchroCreationInfo {
    local: AnyFsCreationInfo,
    remote: AnyFsCreationInfo,
    name: Option<String>,
}

impl AnySynchroCreationInfo {
    pub fn new(local: AnyFsCreationInfo, remote: AnyFsCreationInfo, name: Option<String>) -> Self {
        Self {
            local,
            remote,
            name,
        }
    }

    pub fn local(&self) -> &AnyFsCreationInfo {
        &self.local
    }

    pub fn remote(&self) -> &AnyFsCreationInfo {
        &self.remote
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

/// The information needed to describe a FS that can be synchronized.
///
/// This is used for display and to avoid duplicate synchros.
#[derive(Clone, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum AnyFsDescription {
    LocalDir(<LocalDir as FSBackend>::Description),
    Nextcloud(<NextcloudFs as FSBackend>::Description),
}

impl AnyFsDescription {
    pub fn name(&self) -> &str {
        match self {
            AnyFsDescription::LocalDir(desc) => desc.name(),
            AnyFsDescription::Nextcloud(desc) => desc.name(),
        }
    }

    pub fn type_name(&self) -> &str {
        match self {
            AnyFsDescription::LocalDir(_) => LocalDir::TYPE_NAME,
            AnyFsDescription::Nextcloud(_) => NextcloudFs::TYPE_NAME,
        }
    }
}

impl Display for AnyFsDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyFsDescription::LocalDir(local) => local.fmt(f),
            AnyFsDescription::Nextcloud(nextcloud) => nextcloud.fmt(f),
        }
    }
}

impl From<AnyFsCreationInfo> for AnyFsDescription {
    fn from(value: AnyFsCreationInfo) -> Self {
        match value {
            AnyFsCreationInfo::LocalDir(dir) => Self::LocalDir(dir.into()),
            AnyFsCreationInfo::Nextcloud(nextcloud) => Self::Nextcloud(nextcloud.into()),
        }
    }
}

#[tarpc::service]
pub trait BrumeService {
    /// Creates a new synchronization between a "remote" and a "local" fs
    async fn new_synchro(
        local: AnyFsCreationInfo,
        remote: AnyFsCreationInfo,
        name: Option<String>,
    ) -> Result<(), String>;

    /// Lists all the existing synchronizations registered in the daemon
    async fn list_synchros() -> HashMap<SynchroId, AnySynchroRef>;

    /// Deletes a synchronization
    async fn delete_synchro(id: SynchroId) -> Result<(), String>;

    /// Pauses a synchronization
    async fn pause_synchro(id: SynchroId) -> Result<(), String>;

    /// Resumes a synchronization
    async fn resume_synchro(id: SynchroId) -> Result<(), String>;

    /// Resolves a conflict
    async fn resolve_conflict(
        id: SynchroId,
        path: VirtualPathBuf,
        side: SynchroSide,
    ) -> Result<(), String>;
}
