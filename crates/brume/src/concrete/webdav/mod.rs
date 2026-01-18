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
use reqwest::{Body, Url};
use reqwest_dav::{Auth, Client, ClientBuilder, Depth, re_exports::url::ParseError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod dav;

use crate::{
    update::{IsModified, ModificationState},
    vfs::{DirInfo, FileInfo, NodeInfo, Vfs, VirtualPath, VirtualPathBuf, VirtualPathError},
};

use dav::{TagError, dav_parse_entity_meta, dav_parse_vfs};

use super::{
    FSBackend, FsBackendError, FsInstanceDescription, InvalidBytesSyncInfo, Named, ToBytes,
    TryFromBytes,
};

/// An error during synchronisation with the nextcloud file system
#[derive(Error, Debug)]
pub enum WebDavError {
    #[error("user provided webdav config is invalid")]
    ConfigError(#[from] WebDavConfigError),
    #[error("a path provided by the server is invalid")]
    InvalidPath(#[from] VirtualPathError),
    #[error("a tag provided by the server is invalid")]
    InvalidTag(#[from] TagError),
    #[error("the structure of the webdav FS is not valid")]
    BadStructure,
    #[error("failed to decode server provided url")]
    UrlDecode(#[from] FromUtf8Error),
    #[error("a dav protocol error occurred during communication with the server")]
    ProtocolError(#[from] reqwest_dav::Error),
    #[error("io error while sending or receiving a file")]
    IoError(#[from] io::Error),
}

/// User provided webdav config is invalid
#[derive(Error, Debug)]
pub enum WebDavConfigError {
    #[error("the provided url is invalid")]
    InvalidUrl(#[from] ParseError),
    #[error("the path to the dav server at this url is invalid")]
    InvalidPath(#[from] VirtualPathError),
    #[error("invalid scheme for the server url: {0}")]
    InvalidUrlScheme(String),
}

impl WebDavError {
    /// Return the inner error message in case of protocol error
    pub fn protocol_error_message(&self) -> Option<String> {
        match self {
            WebDavError::ProtocolError(reqwest_dav::Error::Reqwest(error)) => {
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

impl From<reqwest::Error> for WebDavError {
    fn from(value: reqwest::Error) -> Self {
        Self::ProtocolError(value.into())
    }
}

impl From<WebDavError> for FsBackendError {
    fn from(value: WebDavError) -> Self {
        Self(Arc::new(value))
    }
}

/// A WebDav FileSystem
#[derive(Debug)]
pub struct WebDav {
    client: Client,
    /// Path to the dav server at the provided url
    path_to_dav: VirtualPathBuf,
    host: String,
    name: String,
}

impl WebDav {
    // TODO: handle folders that are not the user root folder
    pub fn new(url: &str, login: &str, password: &str) -> Result<Self, WebDavError> {
        let name = login.to_string();
        let url = Url::parse(url).map_err(WebDavConfigError::from)?;
        let full_url = url.join(&name).map_err(WebDavConfigError::from)?;
        let path_to_dav = VirtualPathBuf::new(full_url.path()).map_err(WebDavConfigError::from)?;
        let host = full_url
            .host_str()
            .ok_or(WebDavConfigError::InvalidUrlScheme(
                full_url.scheme().to_string(),
            ))?
            .to_string();

        let client = ClientBuilder::new()
            .set_host(full_url.to_string())
            // TODO: handle different auth types
            .set_auth(Auth::Basic(login.to_string(), password.to_string()))
            .build()?;

        Ok(Self {
            client,
            name,
            path_to_dav,
            host,
        })
    }

    pub fn path(&self) -> &VirtualPath {
        &self.path_to_dav
    }
}

impl Named for WebDav {
    const TYPE_NAME: &'static str = "WebDAV";
}

impl FSBackend for WebDav {
    type SyncInfo = WebDavSyncInfo;

    type IoError = WebDavError;

    type CreationInfo = WebDavFsCreationInfo;

    type Description = WebDavFsDescription;

    fn validate(info: &Self::CreationInfo) -> BoxFuture<'_, Result<(), Self::IoError>> {
        Box::pin(async {
            // Try to create a webdav client instance and access the remote url
            let dav: Self = info.clone().try_into()?;
            dav.client
                .list("", Depth::Number(0))
                .await
                .map(|_| ())
                .map_err(|e| {
                    println!("{e}");
                    e.into()
                })
        })
    }

    fn description(&self) -> Self::Description {
        WebDavFsDescription {
            server_url: self.host.clone(),
            name: self.name.clone(),
        }
    }

    fn get_node_info<'a>(
        &'a self,
        path: &'a VirtualPath,
    ) -> BoxFuture<'a, Result<NodeInfo<Self::SyncInfo>, Self::IoError>> {
        Box::pin(async {
            let elements = self.client.list(path.into(), Depth::Number(0)).await?;

            let elem = elements.first().ok_or(WebDavError::BadStructure)?;

            dav_parse_entity_meta(elem.clone())
        })
    }

    fn load_virtual(&self) -> BoxFuture<'_, Result<Vfs<Self::SyncInfo>, Self::IoError>> {
        Box::pin(async {
            let elements = self.client.list("", Depth::Infinity).await?;

            let vfs_root = dav_parse_vfs(elements, self.path())?;

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
                .ok_or(WebDavError::BadStructure)
                .and_then(dav_parse_entity_meta)
                .and_then(|info| info.into_file_info().ok_or(WebDavError::BadStructure))
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
                .ok_or(WebDavError::BadStructure)
                .and_then(dav_parse_entity_meta)
                .and_then(|info| info.into_dir_info().ok_or(WebDavError::BadStructure))
        })
    }

    fn rmdir<'a>(&'a self, path: &'a VirtualPath) -> BoxFuture<'a, Result<(), Self::IoError>> {
        Box::pin(async { self.client.delete(path.into()).await.map_err(|e| e.into()) })
    }
}

/// Metadata used to detect modifications of a webdav FS node
///
/// The nodes are compared using the response [etag] field, which is modified by the
/// server if a node or its content is modified.
///
/// [etag]: https://docs.nextcloud.com/desktop/3.13/architecture.html#synchronization-by-time-versus-etag
// TODO: handle servers that do not support etag
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebDavSyncInfo {
    tag: u128,
}

impl WebDavSyncInfo {
    pub fn new(tag: u128) -> Self {
        Self { tag }
    }
}

impl IsModified for WebDavSyncInfo {
    fn modification_state(&self, reference: &Self) -> ModificationState {
        if self.tag != reference.tag {
            ModificationState::Modified
        } else {
            ModificationState::RecursiveUnmodified
        }
    }
}

impl<'a> From<&'a WebDavSyncInfo> for WebDavSyncInfo {
    fn from(value: &'a WebDavSyncInfo) -> Self {
        value.to_owned()
    }
}

impl<'a> From<&'a WebDavSyncInfo> for () {
    fn from(_value: &'a WebDavSyncInfo) -> Self {}
}

impl ToBytes for WebDavSyncInfo {
    fn to_bytes(&self) -> Vec<u8> {
        self.tag.to_le_bytes().to_vec()
    }
}

impl TryFromBytes for WebDavSyncInfo {
    fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, InvalidBytesSyncInfo> {
        let byte_array: [u8; 16] = bytes.try_into().map_err(|_| InvalidBytesSyncInfo)?;
        let tag = u128::from_le_bytes(byte_array);

        Ok(Self { tag })
    }
}

/// Description of a connection to a webdav server
#[derive(Clone, Hash, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct WebDavFsDescription {
    server_url: String,
    name: String,
}

impl WebDavFsDescription {
    pub fn server_url(&self) -> &str {
        &self.server_url
    }
}

impl FsInstanceDescription for WebDavFsDescription {
    fn name(&self) -> &str {
        &self.name
    }
}

impl Display for WebDavFsDescription {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "url: {}, folder: {}", self.server_url, self.name)
    }
}

/// Info needed to create a new connection to a webdav server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebDavFsCreationInfo {
    server_url: String,
    login: String,
    password: String,
}

impl WebDavFsCreationInfo {
    pub fn new(server_url: &str, login: &str, password: &str) -> Self {
        Self {
            server_url: server_url.to_string(),
            login: login.to_string(),
            password: password.to_string(),
        }
    }
}

impl From<WebDavFsCreationInfo> for WebDavFsDescription {
    fn from(value: WebDavFsCreationInfo) -> Self {
        Self {
            server_url: value.server_url,
            name: value.login,
        }
    }
}

impl TryFrom<WebDavFsCreationInfo> for WebDav {
    type Error = <WebDav as FSBackend>::IoError;

    fn try_from(value: WebDavFsCreationInfo) -> Result<Self, Self::Error> {
        Self::new(&value.server_url, &value.login, &value.password)
    }
}
