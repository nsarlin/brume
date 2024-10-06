//! Manipulation of a remote filesystem with WebDAV

use chrono::{DateTime, Utc};
use reqwest_dav::{Auth, Client, ClientBuilder, Depth};

mod dav;

use crate::{
    vfs::{IsModified, ModificationState, Vfs, VirtualPathError},
    NC_DAV_PATH_STR,
};

use dav::{dav_parse_vfs, TagError};

/// An error during synchronisation with the remote file system
#[derive(Debug)]
pub enum RemoteFsError {
    /// A path provided by the server is invalid
    InvalidPath(VirtualPathError),
    /// A tag provided by the server is invalid
    InvalidTag(TagError),
    /// The structure of the remote FS is not valid
    BadStructure,
    /// A dav protocol error occured during communication with the remote server
    ProtocolError(reqwest_dav::Error),
}

impl From<reqwest_dav::Error> for RemoteFsError {
    fn from(value: reqwest_dav::Error) -> Self {
        Self::ProtocolError(value)
    }
}

impl From<VirtualPathError> for RemoteFsError {
    fn from(value: VirtualPathError) -> Self {
        Self::InvalidPath(value)
    }
}

impl From<TagError> for RemoteFsError {
    fn from(value: TagError) -> Self {
        Self::InvalidTag(value)
    }
}

/// The remote FileSystem, accessed with the dav protocol
#[derive(Debug)]
pub struct RemoteFs {
    _client: Client,
    _vfs: Vfs<RemoteSyncInfo>,
}

impl RemoteFs {
    pub async fn new(url: &str, login: &str, password: &str) -> Result<Self, RemoteFsError> {
        let name = login.to_string();
        let client = ClientBuilder::new()
            .set_host(format!("{}{}{}/", url, NC_DAV_PATH_STR, &name))
            .set_auth(Auth::Basic(login.to_string(), password.to_string()))
            .build()?;

        let elements = client.list("", Depth::Infinity).await?;

        let vfs = dav_parse_vfs(elements, &name)?;

        Ok(Self {
            _client: client,
            _vfs: vfs,
        })
    }
}

/// Metadata used to detect modifications of a remote FS node
///
/// If possible, the nodes are compared using the nextcloud [etag] field, which is modified by the
/// server if a node or its content is modified. When comparing to a [`LocalSyncInfo`] which does
/// not have this tag, we resort to the modification time.
///
/// [etag]: https://docs.nextcloud.com/desktop/3.13/architecture.html#synchronization-by-time-versus-etag
/// [`LocalSyncInfo`]: crate::local::LocalSyncInfo
#[derive(Debug, Clone)]
pub struct RemoteSyncInfo {
    last_modified: DateTime<Utc>,
    tag: u128,
}

impl RemoteSyncInfo {
    pub fn new(last_modified: DateTime<Utc>, tag: u128) -> Self {
        Self { last_modified, tag }
    }

    pub fn last_modified(&self) -> DateTime<Utc> {
        self.last_modified
    }

    pub fn tag(&self) -> u128 {
        self.tag
    }
}

impl IsModified<Self> for RemoteSyncInfo {
    fn modification_state(&self, reference: &Self) -> ModificationState {
        if self.tag != reference.tag {
            ModificationState::Modified
        } else {
            ModificationState::RecursiveUnmodified
        }
    }
}
