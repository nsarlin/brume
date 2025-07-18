//! Manipulation of a Nextcloud filesystem with WebDAV

use std::{
    error::Error,
    fmt::{Display, Formatter},
    io::{self},
    string::FromUtf8Error,
    sync::Arc,
};

use bytes::Bytes;
use futures::{Stream, TryStream, TryStreamExt, future::BoxFuture};
use reqwest::Body;
use reqwest_dav::{Auth, Client, ClientBuilder, Depth};
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod dav;

use crate::{
    update::{IsModified, ModificationState},
    vfs::{DirInfo, FileInfo, NodeInfo, Vfs, VirtualPath, VirtualPathError},
};

use dav::{TagError, dav_parse_entity_meta, dav_parse_vfs};

use super::{
    FSBackend, FsBackendError, FsInstanceDescription, InvalidBytesSyncInfo, Named, ToBytes,
    TryFromBytes,
};

const NC_DAV_PATH_STR: &str = "/remote.php/dav/files/";

/// An error during synchronisation with the nextcloud file system
#[derive(Error, Debug)]
pub enum NextcloudFsError {
    #[error("a path provided by the server is invalid")]
    InvalidPath(#[from] VirtualPathError),
    #[error("a tag provided by the server is invalid")]
    InvalidTag(#[from] TagError),
    #[error("the structure of the nextcloud FS is not valid")]
    BadStructure,
    #[error("failed to decode server provided url")]
    UrlDecode(#[from] FromUtf8Error),
    #[error("a dav protocol error occurred during communication with the nextcloud server")]
    ProtocolError(#[from] reqwest_dav::Error),
    #[error("io error while sending or receiving a file")]
    IoError(#[from] io::Error),
}

impl NextcloudFsError {
    /// Return the inner error message in case of protocol error
    pub fn protocol_error_message(&self) -> Option<String> {
        match self {
            NextcloudFsError::ProtocolError(reqwest_dav::Error::Reqwest(error)) => {
                if let Some(source) = error.source() {
                    if let Some(source2) = source.source() {
                        Some(source2.to_string())
                    } else {
                        Some(source.to_string())
                    }
                } else {
                    Some(error.to_string())
                }
            }
            _ => None,
        }
    }
}

impl From<reqwest::Error> for NextcloudFsError {
    fn from(value: reqwest::Error) -> Self {
        Self::ProtocolError(value.into())
    }
}

impl From<NextcloudFsError> for FsBackendError {
    fn from(value: NextcloudFsError) -> Self {
        Self(Arc::new(value))
    }
}

/// The nextcloud FileSystem, accessed with the dav protocol
#[derive(Debug)]
pub struct Nextcloud {
    client: Client,
    name: String,
}

impl Nextcloud {
    // TODO: handle folders that are not the user root folder
    pub fn new(url: &str, login: &str, password: &str) -> Result<Self, NextcloudFsError> {
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

impl FSBackend for Nextcloud {
    type SyncInfo = NextcloudSyncInfo;

    type IoError = NextcloudFsError;

    type CreationInfo = NextcloudFsCreationInfo;

    type Description = NextcloudFsDescription;

    fn validate(info: &Self::CreationInfo) -> BoxFuture<'_, Result<(), Self::IoError>> {
        Box::pin(async {
            // Try to create a nextcloud client instance and access the remote url
            let nextcloud: Self = info.clone().try_into()?;
            nextcloud
                .client
                .list("", Depth::Number(0))
                .await
                .map(|_| ())
                .map_err(|e| e.into())
        })
    }

    fn description(&self) -> Self::Description {
        NextcloudFsDescription {
            server_url: self
                .client
                .host
                .trim_end_matches('/')
                .trim_end_matches(&self.name)
                .trim_end_matches(&NC_DAV_PATH_STR)
                .to_string(),
            name: self.name.clone(),
        }
    }

    fn get_node_info<'a>(
        &'a self,
        path: &'a VirtualPath,
    ) -> BoxFuture<'a, Result<NodeInfo<Self::SyncInfo>, Self::IoError>> {
        Box::pin(async {
            let elements = self.client.list(path.into(), Depth::Number(0)).await?;

            let elem = elements.first().ok_or(NextcloudFsError::BadStructure)?;

            dav_parse_entity_meta(elem.clone())
        })
    }

    fn load_virtual(&self) -> BoxFuture<'_, Result<Vfs<Self::SyncInfo>, Self::IoError>> {
        Box::pin(async {
            let elements = self.client.list("", Depth::Infinity).await?;

            let vfs_root = dav_parse_vfs(elements, &self.name)?;

            Ok(Vfs::new(vfs_root))
        })
    }

    fn read_file<'a>(
        &'a self,
        path: &'a VirtualPath,
    ) -> BoxFuture<
        'a,
        Result<impl Stream<Item = Result<Bytes, Self::IoError>> + 'static, Self::IoError>,
    > {
        Box::pin(async {
            Ok(self
                .client
                .get(path.into())
                .await?
                .bytes_stream()
                .map_err(|e| e.into()))
        })
    }

    fn write_file<'a, Data: TryStream + Send + 'static>(
        &'a self,
        path: &'a VirtualPath,
        data: Data,
    ) -> BoxFuture<'a, Result<FileInfo<Self::SyncInfo>, Self::IoError>>
    where
        Data::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        Bytes: From<Data::Ok>,
    {
        Box::pin(async {
            let body = Body::wrap_stream(data);

            self.client.put(path.into(), body).await?;

            // Extract the tag of the created file
            let mut entities = self.client.list(path.into(), Depth::Number(0)).await?;
            entities
                .pop()
                .ok_or(NextcloudFsError::BadStructure)
                .and_then(dav_parse_entity_meta)
                .and_then(|info| info.into_file_info().ok_or(NextcloudFsError::BadStructure))
        })
    }

    fn rm<'a>(&'a self, path: &'a VirtualPath) -> BoxFuture<'a, Result<(), Self::IoError>> {
        Box::pin(async { self.client.delete(path.into()).await.map_err(|e| e.into()) })
    }

    fn mkdir<'a>(
        &'a self,
        path: &'a VirtualPath,
    ) -> BoxFuture<'a, Result<DirInfo<Self::SyncInfo>, Self::IoError>> {
        Box::pin(async {
            self.client.mkcol(path.into()).await?;

            // Extract the tag of the created dir
            let mut entities = self.client.list(path.into(), Depth::Number(0)).await?;
            entities
                .pop()
                .ok_or(NextcloudFsError::BadStructure)
                .and_then(dav_parse_entity_meta)
                .and_then(|info| info.into_dir_info().ok_or(NextcloudFsError::BadStructure))
        })
    }

    fn rmdir<'a>(&'a self, path: &'a VirtualPath) -> BoxFuture<'a, Result<(), Self::IoError>> {
        Box::pin(async { self.client.delete(path.into()).await.map_err(|e| e.into()) })
    }
}

/// Metadata used to detect modifications of a nextcloud FS node
///
/// The nodes are compared using the nextcloud [etag] field, which is modified by the
/// server if a node or its content is modified.
///
/// [etag]: https://docs.nextcloud.com/desktop/3.13/architecture.html#synchronization-by-time-versus-etag
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NextcloudSyncInfo {
    tag: u128,
}

impl NextcloudSyncInfo {
    pub fn new(tag: u128) -> Self {
        Self { tag }
    }
}

impl Named for NextcloudSyncInfo {
    const TYPE_NAME: &'static str = "Nextcloud";
}

impl IsModified for NextcloudSyncInfo {
    fn modification_state(&self, reference: &Self) -> ModificationState {
        if self.tag != reference.tag {
            ModificationState::Modified
        } else {
            ModificationState::RecursiveUnmodified
        }
    }
}

impl<'a> From<&'a NextcloudSyncInfo> for NextcloudSyncInfo {
    fn from(value: &'a NextcloudSyncInfo) -> Self {
        value.to_owned()
    }
}

impl<'a> From<&'a NextcloudSyncInfo> for () {
    fn from(_value: &'a NextcloudSyncInfo) -> Self {}
}

impl ToBytes for NextcloudSyncInfo {
    fn to_bytes(&self) -> Vec<u8> {
        self.tag.to_le_bytes().to_vec()
    }
}

impl TryFromBytes for NextcloudSyncInfo {
    fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, InvalidBytesSyncInfo> {
        let byte_array: [u8; 16] = bytes.try_into().map_err(|_| InvalidBytesSyncInfo)?;
        let tag = u128::from_le_bytes(byte_array);

        Ok(Self { tag })
    }
}

/// Description of a connection to a nextcloud instance
#[derive(Clone, Hash, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct NextcloudFsDescription {
    server_url: String,
    name: String,
}

impl FsInstanceDescription for NextcloudFsDescription {
    fn name(&self) -> &str {
        &self.name
    }
}

impl Display for NextcloudFsDescription {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "url: {}, folder: {}", self.server_url, self.name)
    }
}

/// Info needed to create a new connection to a nextcloud server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NextcloudFsCreationInfo {
    server_url: String,
    login: String,
    password: String,
}

impl NextcloudFsCreationInfo {
    pub fn new(server_url: &str, login: &str, password: &str) -> Self {
        Self {
            server_url: server_url.to_string(),
            login: login.to_string(),
            password: password.to_string(),
        }
    }
}

impl From<NextcloudFsCreationInfo> for NextcloudFsDescription {
    fn from(value: NextcloudFsCreationInfo) -> Self {
        Self {
            server_url: value.server_url,
            name: value.login,
        }
    }
}

impl TryFrom<NextcloudFsCreationInfo> for Nextcloud {
    type Error = <Nextcloud as FSBackend>::IoError;

    fn try_from(value: NextcloudFsCreationInfo) -> Result<Self, Self::Error> {
        Self::new(&value.server_url, &value.login, &value.password)
    }
}
