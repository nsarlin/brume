//! Manipulation of a remote filesystem with WebDAV

use std::io::{self, Read};

use bytes::Buf;
use reqwest_dav::{re_exports::reqwest, Auth, Client, ClientBuilder, Depth};
use thiserror::Error;
use xxhash_rust::xxh3::xxh3_64;

mod dav;

use crate::{
    vfs::{IsModified, ModificationState, Vfs, VirtualPath, VirtualPathError},
    NC_DAV_PATH_STR,
};

use dav::{dav_parse_vfs, TagError};

use super::ConcreteFS;

/// An error during synchronisation with the remote file system
#[derive(Error, Debug)]
pub enum RemoteFsError {
    #[error("a path provided by the server is invalid")]
    InvalidPath(#[from] VirtualPathError),
    #[error("a tag provided by the server is invalid")]
    InvalidTag(#[from] TagError),
    #[error("the structure of the remote FS is not valid")]
    BadStructure,
    #[error("a dav protocol error occured during communication with the remote server")]
    ProtocolError(#[from] reqwest_dav::Error),
    #[error("io error while sending or receiving a file")]
    IoError(#[from] io::Error),
}

impl From<reqwest::Error> for RemoteFsError {
    fn from(value: reqwest::Error) -> Self {
        Self::ProtocolError(value.into())
    }
}

impl From<RemoteFsError> for crate::Error {
    fn from(value: RemoteFsError) -> Self {
        Self::ConcreteFsError(Box::new(value))
    }
}

/// The remote FileSystem, accessed with the dav protocol
#[derive(Debug)]
pub struct RemoteFs {
    client: Client,
    name: String,
}

impl RemoteFs {
    pub fn new(url: &str, login: &str, password: &str) -> Result<Self, RemoteFsError> {
        let name = login.to_string();
        let client = ClientBuilder::new()
            .set_host(format!("{}{}{}/", url, NC_DAV_PATH_STR, &name))
            .set_auth(Auth::Basic(login.to_string(), password.to_string()))
            .build()?;

        Ok(Self {
            client,
            name: name.to_string(),
        })
    }
}

impl ConcreteFS for RemoteFs {
    type SyncInfo = RemoteSyncInfo;

    type Error = RemoteFsError;

    async fn load_virtual(&self) -> Result<Vfs<Self::SyncInfo>, Self::Error> {
        let elements = self.client.list("", Depth::Infinity).await?;

        let vfs_root = dav_parse_vfs(elements, &self.name)?;

        Ok(Vfs::new(vfs_root))
    }

    async fn open(&self, path: &VirtualPath) -> Result<impl std::io::Read, Self::Error> {
        Ok(self.client.get(path.into()).await?.bytes().await?.reader())
    }

    async fn write<Stream: std::io::Read>(
        &self,
        path: &VirtualPath,
        data: &mut Stream,
    ) -> Result<(), Self::Error> {
        let mut buf = Vec::new();

        data.read_to_end(&mut buf).unwrap(); // TODO: handle io error
        self.client
            .put(path.into(), buf)
            .await
            .map_err(|e| e.into())
    }

    async fn mkdir(&self, path: &VirtualPath) -> Result<(), Self::Error> {
        self.client.mkcol(path.into()).await.map_err(|e| e.into())
    }

    async fn hash(&self, path: &VirtualPath) -> Result<u64, Self::Error> {
        let mut reader = self.open(path).await?;
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;

        Ok(xxh3_64(&data))
    }
}

/// Metadata used to detect modifications of a remote FS node
///
/// If possible, the nodes are compared using the nextcloud [etag] field, which is modified by the
/// server if a node or its content is modified. When comparing to a [`LocalSyncInfo`] which does
/// not have this tag, we resort to the modification time (*TODO*: not true anymore).
///
/// [etag]: https://docs.nextcloud.com/desktop/3.13/architecture.html#synchronization-by-time-versus-etag
/// [`LocalSyncInfo`]: crate::concrete::local::LocalSyncInfo
#[derive(Debug, Clone)]
pub struct RemoteSyncInfo {
    tag: u128,
}

impl RemoteSyncInfo {
    pub fn new(tag: u128) -> Self {
        Self { tag }
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

impl<'a> From<&'a RemoteSyncInfo> for RemoteSyncInfo {
    fn from(value: &'a RemoteSyncInfo) -> Self {
        value.to_owned()
    }
}

impl<'a> From<&'a RemoteSyncInfo> for () {
    fn from(_value: &'a RemoteSyncInfo) -> Self {}
}
