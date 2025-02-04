//! Definition of the protocol needed to communicate with the daemon

use std::{collections::HashMap, fmt::Display};

use brume::concrete::{
    local::LocalDir, nextcloud::NextcloudFs, FSBackend, FsInstanceDescription, Named,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use brume::concrete::{local::LocalDirCreationInfo, nextcloud::NextcloudFsCreationInfo};
pub use brume::synchro::SynchroSide;
pub use brume::vfs::virtual_path::{VirtualPath, VirtualPathBuf};

use crate::synchro_list::AnySynchroRef;

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
