//! Interactions with the local filesystem

pub mod path;

use std::{
    io::{self, ErrorKind},
    path::{Path, PathBuf},
};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, TryStream, TryStreamExt};
use path::{node_from_path_rec, LocalPath};
use tokio::{
    fs::{self, File},
    io::AsyncReadExt,
};
use tokio_util::io::{ReaderStream, StreamReader};
use xxhash_rust::xxh3::xxh3_64;

use crate::{
    update::{IsModified, ModificationState},
    vfs::{DirTree, FileMeta, Vfs, VfsNode, VirtualPath},
    Error,
};

use super::{ConcreteFS, ConcreteFsError};

/// Represent a local directory and its content
#[derive(Debug)]
pub struct LocalDir {
    path: PathBuf,
}

impl LocalDir {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
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

    type Error = io::Error;

    async fn load_virtual(&self) -> Result<Vfs<Self::SyncInfo>, Self::Error> {
        let sync = LocalSyncInfo::new(self.path.modification_time()?.into());
        let root = if self.path.is_file() {
            VfsNode::File(FileMeta::new(
                self.path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .ok_or(self.path.invalid_path_error())?,
                self.path.file_size()?,
                sync,
            ))
        } else if self.path.is_dir() {
            let mut root = DirTree::new("", sync);
            let children = self
                .path
                .read_dir()?
                .map(|entry| entry.unwrap())
                .collect::<Vec<_>>();
            node_from_path_rec(&mut root, &children)?;
            VfsNode::Dir(root)
        } else {
            return Err(self.path.invalid_path_error());
        };

        Ok(Vfs::new(root))
    }

    async fn open(
        &self,
        path: &VirtualPath,
    ) -> Result<impl Stream<Item = Result<Bytes, Self::Error>> + 'static, Self::Error> {
        let full_path = self.full_path(path);
        File::open(&full_path).await.map(ReaderStream::new)
    }

    async fn write<Data: TryStream + Send + 'static + Unpin>(
        &self,
        path: &VirtualPath,
        data: Data,
    ) -> Result<Self::SyncInfo, Self::Error>
    where
        Data::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        Bytes: From<Data::Ok>,
    {
        let full_path = self.full_path(path);
        let mut f = File::create(&full_path).await?;
        let mut reader = StreamReader::new(
            data.map_ok(Bytes::from)
                .map_err(|e| io::Error::new(ErrorKind::Other, e)),
        );

        tokio::io::copy(&mut reader, &mut f).await?;

        full_path
            .modification_time()
            .map(|time| LocalSyncInfo::new(time.into()))
    }

    async fn rm(&self, path: &VirtualPath) -> Result<(), Self::Error> {
        let full_path = self.full_path(path);
        fs::remove_file(&full_path).await
    }

    async fn mkdir(&self, path: &VirtualPath) -> Result<Self::SyncInfo, Self::Error> {
        let full_path = self.full_path(path);
        fs::create_dir(&full_path).await?;

        full_path
            .modification_time()
            .map(|time| LocalSyncInfo::new(time.into()))
    }

    async fn rmdir(&self, path: &VirtualPath) -> Result<(), Self::Error> {
        let full_path = self.full_path(path);
        fs::remove_dir_all(&full_path).await
    }

    async fn hash(&self, path: &VirtualPath) -> Result<u64, Self::Error> {
        let stream = self.open(path).await?;
        let mut reader = StreamReader::new(stream.map_err(|e| io::Error::new(ErrorKind::Other, e)));

        let mut data = Vec::new();
        reader.read_to_end(&mut data).await?;

        Ok(xxh3_64(&data))
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

impl IsModified<Self> for LocalSyncInfo {
    fn modification_state(&self, reference: &Self) -> ModificationState {
        if self.last_modified != reference.last_modified {
            ModificationState::Modified
        } else {
            ModificationState::ShallowUnmodified
        }
    }
}

impl<'a> From<&'a LocalSyncInfo> for LocalSyncInfo {
    fn from(value: &'a LocalSyncInfo) -> Self {
        value.to_owned()
    }
}

impl<'a> From<&'a LocalSyncInfo> for () {
    fn from(_value: &'a LocalSyncInfo) -> Self {}
}
