//! Interactions with the local filesystem

pub mod path;

use std::{
    fmt::{Debug, Display, Formatter},
    io::{self, ErrorKind},
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, TryStream, TryStreamExt};
use path::{node_from_path_rec, LocalPath};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File},
    io::AsyncRead,
};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::{
    update::{IsModified, ModificationState},
    vfs::{DirTree, FileMeta, Vfs, VfsNode, VirtualPath},
    Error,
};

use super::{ConcreteFS, ConcreteFsError, Named};

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
    fn io<P: Debug>(path: &P, source: io::Error) -> Self {
        Self::IoError {
            path: format!("{:?}", path),
            source,
        }
    }

    fn invalid_path<P: Debug>(path: &P) -> Self {
        Self::InvalidPath(format!("{:?}", path))
    }
}

impl From<LocalDirError> for ConcreteFsError {
    fn from(value: LocalDirError) -> Self {
        Self(Box::new(value))
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

impl ConcreteFS for LocalDir {
    type SyncInfo = LocalSyncInfo;

    type IoError = LocalDirError;

    type CreationInfo = LocalDirCreationInfo;

    type Description = LocalDirDescription;

    fn description(&self) -> Self::Description {
        LocalDirDescription(self.path.clone())
    }

    async fn load_virtual(&self) -> Result<Vfs<Self::SyncInfo>, Self::IoError> {
        let sync = LocalSyncInfo::new(
            self.path
                .modification_time()
                .map_err(|e| LocalDirError::io(&self.path, e))?
                .into(),
        );
        let root = if self.path.is_file() {
            VfsNode::File(FileMeta::new(
                self.path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .ok_or(LocalDirError::invalid_path(&self.path))?,
                self.path
                    .file_size()
                    .map_err(|e| LocalDirError::io(&self.path, e))?,
                sync,
            ))
        } else if self.path.is_dir() {
            let mut root = DirTree::new("", sync);
            let children = self
                .path
                .read_dir()
                .map_err(|e| LocalDirError::io(&self.path, e))?
                .map(|entry| entry.unwrap())
                .collect::<Vec<_>>();
            node_from_path_rec(&mut root, &children)?;
            VfsNode::Dir(root)
        } else {
            return Err(LocalDirError::invalid_path(&self.path));
        };

        Ok(Vfs::new(root))
    }

    async fn read_file(
        &self,
        path: &VirtualPath,
    ) -> Result<impl Stream<Item = Result<Bytes, Self::IoError>> + 'static, Self::IoError> {
        let full_path = self.full_path(path);
        File::open(&full_path)
            .await
            .map(|reader| LocalFileStream::new(reader, &full_path))
            .map_err(|e| LocalDirError::io(&full_path, e))
    }

    async fn write_file<Data: TryStream + Send + 'static + Unpin>(
        &self,
        path: &VirtualPath,
        data: Data,
    ) -> Result<Self::SyncInfo, Self::IoError>
    where
        Data::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        Bytes: From<Data::Ok>,
    {
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
            .map(|time| LocalSyncInfo::new(time.into()))
            .map_err(|e| LocalDirError::io(&self.path, e))
    }

    async fn rm(&self, path: &VirtualPath) -> Result<(), Self::IoError> {
        let full_path = self.full_path(path);
        fs::remove_file(&full_path)
            .await
            .map_err(|e| LocalDirError::io(&self.path, e))
    }

    async fn mkdir(&self, path: &VirtualPath) -> Result<Self::SyncInfo, Self::IoError> {
        let full_path = self.full_path(path);
        fs::create_dir(&full_path)
            .await
            .map_err(|e| LocalDirError::io(&self.path, e))?;

        full_path
            .modification_time()
            .map(|time| LocalSyncInfo::new(time.into()))
            .map_err(|e| LocalDirError::io(&self.path, e))
    }

    async fn rmdir(&self, path: &VirtualPath) -> Result<(), Self::IoError> {
        let full_path = self.full_path(path);
        fs::remove_dir_all(&full_path)
            .await
            .map_err(|e| LocalDirError::io(&self.path, e))
    }
}

impl From<io::Error> for ConcreteFsError {
    fn from(value: io::Error) -> Self {
        Self(Box::new(value))
    }
}

/// Metadata used to detect modifications of a local FS node
///
/// It is based on the modification time which is not recursive for directories, so we have to
/// handle the recursion ourselves.
#[derive(Debug, Clone)]
pub struct LocalSyncInfo {
    last_modified: DateTime<Utc>,
}

impl LocalSyncInfo {
    pub fn new(last_modified: DateTime<Utc>) -> Self {
        Self { last_modified }
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

/// Uniquely identify a path on the local filesystem
#[derive(Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct LocalDirDescription(PathBuf);

impl Display for LocalDirDescription {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.display())
    }
}

impl From<&LocalDirCreationInfo> for LocalDirDescription {
    fn from(value: &LocalDirCreationInfo) -> Self {
        Self(value.0.clone())
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
pub struct LocalDirCreationInfo(PathBuf);

impl LocalDirCreationInfo {
    pub fn new(path: &Path) -> Self {
        Self(path.to_path_buf())
    }
}

impl TryFrom<LocalDirCreationInfo> for LocalDir {
    type Error = <Self as ConcreteFS>::IoError;

    fn try_from(value: LocalDirCreationInfo) -> Result<Self, Self::Error> {
        Self::new(value.0)
    }
}
