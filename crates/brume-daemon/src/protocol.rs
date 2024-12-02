//! Definition of the protocol needed to communicate with the daemon

use std::{fmt::Display, path::PathBuf};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

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

/// Required information for a Nextcloud connection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NextcloudLoginInfo {
    pub url: String,
    pub login: String,
    pub password: String,
}

/// The information needed to describe a FS that can be synchronized, remote or local
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FsDescription {
    LocalDir(PathBuf),
    Nextcloud(NextcloudLoginInfo),
}

impl Display for FsDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FsDescription::LocalDir(path_buf) => {
                write!(f, "local directory {}", path_buf.display())
            }
            FsDescription::Nextcloud(nextcloud_login_info) => {
                write!(f, "nextcloud server {}", nextcloud_login_info.url)
            }
        }
    }
}

#[tarpc::service]
pub trait BrumeService {
    /// Create a new synchronization between a "remote" and a "local" fs
    async fn new_synchro(local: FsDescription, remote: FsDescription) -> Result<SynchroId, String>;
}
