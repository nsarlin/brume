//! Definition of the protocol needed to communicate with the daemon

use std::fmt::Display;

use brume::concrete::{local::LocalDir, nextcloud::NextcloudFs, ConcreteFS};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use brume::concrete::{local::LocalDirCreationInfo, nextcloud::NextcloudFsCreationInfo};

// TODO: make configurable
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
}

/// The information needed to create a FS that can be synchronized, remote or local
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AnyFsCreationInfo {
    LocalDir(<LocalDir as ConcreteFS>::CreationInfo),
    Nextcloud(<NextcloudFs as ConcreteFS>::CreationInfo),
}

/// The information needed to describe a FS that can be synchronized, remote or local
#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum AnyFsDescription {
    LocalDir(<LocalDir as ConcreteFS>::Description),
    Nextcloud(<NextcloudFs as ConcreteFS>::Description),
}

impl Display for AnyFsDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyFsDescription::LocalDir(local) => local.fmt(f),
            AnyFsDescription::Nextcloud(nextcloud) => nextcloud.fmt(f),
        }
    }
}

impl From<&AnyFsCreationInfo> for AnyFsDescription {
    fn from(value: &AnyFsCreationInfo) -> Self {
        match value {
            AnyFsCreationInfo::LocalDir(dir) => Self::LocalDir(dir.into()),
            AnyFsCreationInfo::Nextcloud(nextcloud) => Self::Nextcloud(nextcloud.into()),
        }
    }
}

#[tarpc::service]
pub trait BrumeService {
    /// Create a new synchronization between a "remote" and a "local" fs
    async fn new_synchro(
        local: AnyFsCreationInfo,
        remote: AnyFsCreationInfo,
    ) -> Result<SynchroId, String>;
}
