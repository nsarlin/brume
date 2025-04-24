//! FSBackend using the local filesystem

pub mod path;

use std::{
    fmt::{Debug, Display, Formatter},
    io::{self, ErrorKind},
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt, TryStream, TryStreamExt, future::BoxFuture};
use path::{LocalPath, build_vfs_subtree};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File},
    io::AsyncRead,
};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::{
    Error,
    update::{IsModified, ModificationState},
    vfs::{DirTree, FileMeta, Vfs, VfsNode, VirtualPath},
};

use super::{
    FSBackend, FsBackendError, FsInstanceDescription, InvalidByteSyncInfo, Named, ToBytes,
    TryFromBytes,
};

#[derive(Error, Debug)]
pub enum LocalDirError {
    #[error("io error with local fs, on path {path}")]
    IoError {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("could not parse path {0}")]
    InvalidPath(String),
}

impl LocalDirError {
    pub fn io<P: Debug>(path: P, source: io::Error) -> Self {
        Self::IoError {
            path: format!("{:?}", path),
            source,
        }
    }

    pub fn invalid_path<P: Debug>(path: &P) -> Self {
        Self::InvalidPath(format!("{:?}", path))
    }
}

impl From<LocalDirError> for FsBackendError {
    fn from(value: LocalDirError) -> Self {
        Self(Arc::new(value))
    }
}

/// [`ReaderStream`] adapter that report errors as [`LocalDirError`]
pub struct LocalFileStream<R: AsyncRead> {
    path: PathBuf,
    stream: ReaderStream<R>,
}

impl<R: AsyncRead> LocalFileStream<R> {
    fn stream_mut(self: Pin<&mut Self>) -> Pin<&mut ReaderStream<R>> {
        // SAFETY: This is okay because `stream` is pinned when `self` is.
        unsafe { self.map_unchecked_mut(|s| &mut s.stream) }
    }

    fn new(reader: R, path: &Path) -> Self {
        Self {
            path: path.to_owned(),
            stream: ReaderStream::new(reader),
        }
    }
}

impl<R: AsyncRead> Stream for LocalFileStream<R> {
    type Item = Result<Bytes, LocalDirError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // TODO: avoid this clone ?
        let path = self.path.to_owned();
        self.stream_mut()
            .poll_next(cx)
            .map_err(|e| LocalDirError::io(&path, e))
    }
}

/// Represent a local directory and its content
#[derive(Debug)]
pub struct LocalDir {
    path: PathBuf,
}

impl LocalDir {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, LocalDirError> {
        let path = path.as_ref();

        Ok(Self {
            path: path.to_path_buf(),
        })
    }

    pub fn full_path(&self, path: &VirtualPath) -> PathBuf {
        let path: &str = path.into();
        let trimmed = path.trim_start_matches('/');

        self.path.join(trimmed)
    }
}

impl FSBackend for LocalDir {
    type SyncInfo = LocalSyncInfo;

    type IoError = LocalDirError;

    type CreationInfo = LocalDirCreationInfo;

    type Description = LocalDirDescription;

    fn validate(info: &Self::CreationInfo) -> BoxFuture<'_, Result<(), Self::IoError>> {
        Box::pin(async {
            fs::metadata(&info.0)
                .await
                .map(|_| ())
                .map_err(|e| LocalDirError::io(&info.0, e))
        })
    }

    fn description(&self) -> Self::Description {
        LocalDirDescription::new(&self.path)
    }

    fn get_sync_info<'a>(
        &'a self,
        path: &'a VirtualPath,
    ) -> BoxFuture<'a, Result<Self::SyncInfo, Self::IoError>> {
        Box::pin(async {
            let full_path = self.full_path(path);
            LocalSyncInfo::load(full_path).await
        })
    }

    fn load_virtual(&self) -> BoxFuture<'_, Result<Vfs<Self::SyncInfo>, Self::IoError>> {
        Box::pin(async {
            let metadata = fs::metadata(&self.path)
                .await
                .map_err(|e| LocalDirError::io(&self.path, e))?;
            let sync_info = LocalSyncInfo::load(&self.path).await?;

            let root = if metadata.is_file() {
                VfsNode::File(FileMeta::new(
                    self.path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .ok_or(LocalDirError::invalid_path(&self.path))?,
                    metadata.len(),
                    sync_info,
                ))
            } else if metadata.is_dir() {
                let mut root = DirTree::new("", sync_info);
                let children = self
                    .path
                    .read_dir()
                    .await
                    .map_err(|e| LocalDirError::io(&self.path, e))?
                    .map(|entry| entry.unwrap())
                    .collect::<Vec<_>>()
                    .await;
                let vfs_children = build_vfs_subtree(&children).await?;
                vfs_children.into_iter().for_each(|n| {
                    root.insert_child(n);
                });

                VfsNode::Dir(root)
            } else {
                return Err(LocalDirError::invalid_path(&self.path));
            };

            Ok(Vfs::new(root))
        })
    }

    fn read_file<'a>(
        &'a self,
        path: &'a VirtualPath,
    ) -> BoxFuture<
        'a,
        Result<
            impl Stream<Item = Result<Bytes, Self::IoError>> + Send + Unpin + 'static,
            Self::IoError,
        >,
    > {
        Box::pin(async {
            let full_path = self.full_path(path);
            File::open(&full_path)
                .await
                .map(|reader| LocalFileStream::new(reader, &full_path))
                .map_err(|e| LocalDirError::io(&full_path, e))
        })
    }

    fn write_file<'a, Data: TryStream + Send + 'static + Unpin>(
        &'a self,
        path: &'a VirtualPath,
        data: Data,
    ) -> BoxFuture<'a, Result<Self::SyncInfo, Self::IoError>>
    where
        Data::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        Bytes: From<Data::Ok>,
    {
        Box::pin(async {
            let full_path = self.full_path(path);
            let mut f = File::create(&full_path)
                .await
                .map_err(|e| LocalDirError::io(&self.path, e))?;
            let mut reader = StreamReader::new(
                data.map_ok(Bytes::from)
                    .map_err(|e| io::Error::new(ErrorKind::Other, e)),
            );

            tokio::io::copy(&mut reader, &mut f)
                .await
                .map_err(|e| LocalDirError::io(&self.path, e))?;

            full_path
                .modification_time()
                .await
                .map(|time| LocalSyncInfo::new(time.into()))
                .map_err(|e| LocalDirError::io(&self.path, e))
        })
    }

    fn rm<'a>(&'a self, path: &'a VirtualPath) -> BoxFuture<'a, Result<(), Self::IoError>> {
        Box::pin(async {
            let full_path = self.full_path(path);
            fs::remove_file(&full_path)
                .await
                .map_err(|e| LocalDirError::io(&full_path, e))
        })
    }

    fn mkdir<'a>(
        &'a self,
        path: &'a VirtualPath,
    ) -> BoxFuture<'a, Result<Self::SyncInfo, Self::IoError>> {
        Box::pin(async {
            let full_path = self.full_path(path);
            fs::create_dir(&full_path)
                .await
                .map_err(|e| LocalDirError::io(&self.path, e))?;

            full_path
                .modification_time()
                .await
                .map(|time| LocalSyncInfo::new(time.into()))
                .map_err(|e| LocalDirError::io(&self.path, e))
        })
    }

    fn rmdir<'a>(&'a self, path: &'a VirtualPath) -> BoxFuture<'a, Result<(), Self::IoError>> {
        Box::pin(async {
            let full_path = self.full_path(path);
            fs::remove_dir_all(&full_path)
                .await
                .map_err(|e| LocalDirError::io(&full_path, e))
        })
    }
}

impl From<io::Error> for FsBackendError {
    fn from(value: io::Error) -> Self {
        Self(Arc::new(value))
    }
}

/// Metadata used to detect modifications of a local FS node
///
/// It is based on the modification time which is not recursive for directories, so we have to
/// handle the recursion ourselves.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalSyncInfo {
    last_modified: DateTime<Utc>,
}

impl LocalSyncInfo {
    pub fn new(last_modified: DateTime<Utc>) -> Self {
        Self { last_modified }
    }

    pub async fn load<P: AsRef<Path>>(path: P) -> Result<Self, LocalDirError> {
        let path = path.as_ref();
        let metadata = fs::metadata(path)
            .await
            .map_err(|e| LocalDirError::io(path, e))?;

        Ok(LocalSyncInfo::new(
            metadata
                .modified()
                .map_err(|e| LocalDirError::io(path, e))?
                .into(),
        ))
    }
}

impl IsModified for LocalSyncInfo {
    fn modification_state(&self, reference: &Self) -> ModificationState {
        if self.last_modified != reference.last_modified {
            ModificationState::Modified
        } else {
            ModificationState::ShallowUnmodified
        }
    }
}

impl Named for LocalSyncInfo {
    const TYPE_NAME: &'static str = "local FileSystem";
}

impl<'a> From<&'a LocalSyncInfo> for LocalSyncInfo {
    fn from(value: &'a LocalSyncInfo) -> Self {
        value.to_owned()
    }
}

impl<'a> From<&'a LocalSyncInfo> for () {
    fn from(_value: &'a LocalSyncInfo) -> Self {}
}

impl ToBytes for LocalSyncInfo {
    fn to_bytes(&self) -> Vec<u8> {
        let secs = self.last_modified.timestamp();
        let nanos = self.last_modified.timestamp_subsec_nanos();

        let mut bytes = Vec::with_capacity(12);
        bytes.extend_from_slice(&secs.to_le_bytes());
        bytes.extend_from_slice(&nanos.to_le_bytes());
        bytes
    }
}

impl TryFromBytes for LocalSyncInfo {
    fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, InvalidByteSyncInfo> {
        if bytes.len() != 12 {
            return Err(InvalidByteSyncInfo);
        }

        // Ok to unwrap because size has been checked
        let secs_bytes: [u8; 8] = bytes[0..8].try_into().unwrap();
        let nanos_bytes: [u8; 4] = bytes[8..12].try_into().unwrap();

        let secs = i64::from_le_bytes(secs_bytes);
        let nanos = u32::from_le_bytes(nanos_bytes);

        let last_modified = DateTime::from_timestamp(secs, nanos).ok_or(InvalidByteSyncInfo)?;

        Ok(Self { last_modified })
    }
}

/// Uniquely identify a path on the local filesystem
#[derive(Clone, Hash, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct LocalDirDescription {
    path: PathBuf,
    name: String,
}

impl LocalDirDescription {
    pub fn new<P: AsRef<Path>>(path: &P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            name: path
                .as_ref()
                .canonicalize()
                .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
                .unwrap_or("<anonymous>".to_string()),
        }
    }
}

impl FsInstanceDescription for LocalDirDescription {
    fn name(&self) -> &str {
        &self.name
    }
}

impl Display for LocalDirDescription {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.path.display())
    }
}

impl From<LocalDirCreationInfo> for LocalDirDescription {
    fn from(value: LocalDirCreationInfo) -> Self {
        Self::new(&value.0)
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct LocalDirCreationInfo(PathBuf);

impl LocalDirCreationInfo {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self(path.as_ref().to_path_buf())
    }
}

impl TryFrom<LocalDirCreationInfo> for LocalDir {
    type Error = <Self as FSBackend>::IoError;

    fn try_from(value: LocalDirCreationInfo) -> Result<Self, Self::Error> {
        Self::new(value.0)
    }
}
