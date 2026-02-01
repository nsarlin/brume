//! Manipulation of a Nextcloud filesystem with WebDAV

use std::fmt::{Display, Formatter};

use bytes::Bytes;
use futures::{Stream, TryStream, future::BoxFuture};
use serde::{Deserialize, Serialize};

use crate::{
    update::{IsModified, ModificationState},
    vfs::{DirInfo, FileInfo, NodeInfo, Vfs, VirtualPath},
};

use super::{
    FSBackend, FsInstanceDescription, InvalidBytesSyncInfo, Named, ToBytes, TryFromBytes,
    webdav::{WebDav, WebDavError, WebDavFsCreationInfo, WebDavFsDescription, WebDavSyncInfo},
};

const NC_DAV_PATH_STR: &str = "/remote.php/dav/files/";

/// The nextcloud FileSystem, accessed with the dav protocol
#[derive(Debug)]
pub struct Nextcloud {
    dav: WebDav,
}

impl Nextcloud {
    pub fn new(url: &str, login: &str, password: &str) -> Result<Self, WebDavError> {
        // TODO: handle folders that are not the user root folder
        let dav_url = format!("{}{}{}", url, NC_DAV_PATH_STR, login);
        let dav = WebDav::new(&dav_url, login, password)?;

        Ok(Self { dav })
    }
}

impl Named for Nextcloud {
    const TYPE_NAME: &'static str = "Nextcloud";
}

impl FSBackend for Nextcloud {
    type SyncInfo = WebDavSyncInfo;

    type IoError = WebDavError;

    type CreationInfo = NextcloudFsCreationInfo;

    type Description = NextcloudFsDescription;

    fn validate(info: &Self::CreationInfo) -> BoxFuture<'_, Result<(), Self::IoError>> {
        Box::pin(async {
            let dav_info = WebDavFsCreationInfo::from(info.clone());
            WebDav::validate(&dav_info).await
        })
    }

    fn description(&self) -> Self::Description {
        let dav_description = self.dav.description();
        dav_description.into()
    }

    fn get_node_info<'a>(
        &'a self,
        path: &'a VirtualPath,
    ) -> BoxFuture<'a, Result<NodeInfo<Self::SyncInfo>, Self::IoError>> {
        self.dav.get_node_info(path)
    }

    fn load_virtual(&self) -> BoxFuture<'_, Result<Vfs<Self::SyncInfo>, Self::IoError>> {
        self.dav.load_virtual()
    }

    fn read_file<'a>(
        &'a self,
        path: &'a VirtualPath,
    ) -> BoxFuture<
        'a,
        Result<impl Stream<Item = Result<Bytes, Self::IoError>> + 'static, Self::IoError>,
    > {
        self.dav.read_file(path)
    }

    fn write_file<'a, Data: TryStream + Send + Unpin + 'static>(
        &'a self,
        path: &'a VirtualPath,
        data: Data,
    ) -> BoxFuture<'a, Result<FileInfo<Self::SyncInfo>, Self::IoError>>
    where
        Data::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        Bytes: From<Data::Ok>,
    {
        self.dav.write_file(path, data)
    }

    fn rm<'a>(&'a self, path: &'a VirtualPath) -> BoxFuture<'a, Result<(), Self::IoError>> {
        self.dav.rm(path)
    }

    fn mkdir<'a>(
        &'a self,
        path: &'a VirtualPath,
    ) -> BoxFuture<'a, Result<DirInfo<Self::SyncInfo>, Self::IoError>> {
        self.dav.mkdir(path)
    }

    fn rmdir<'a>(&'a self, path: &'a VirtualPath) -> BoxFuture<'a, Result<(), Self::IoError>> {
        self.dav.rmdir(path)
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

impl From<WebDavFsDescription> for NextcloudFsDescription {
    fn from(value: WebDavFsDescription) -> Self {
        Self {
            server_url: value.server_url().to_string(),
            name: value.name().to_string(),
        }
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

impl From<NextcloudFsCreationInfo> for WebDavFsCreationInfo {
    fn from(value: NextcloudFsCreationInfo) -> Self {
        let dav_url = format!("{}{}", value.server_url, NC_DAV_PATH_STR);
        Self::new(&dav_url, &value.login, &value.password)
    }
}

impl TryFrom<NextcloudFsCreationInfo> for Nextcloud {
    type Error = <Nextcloud as FSBackend>::IoError;

    fn try_from(value: NextcloudFsCreationInfo) -> Result<Self, Self::Error> {
        Self::new(&value.server_url, &value.login, &value.password)
    }
}
