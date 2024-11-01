//! Interactions with the local filesystem

pub mod path;

use std::{
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use path::{node_from_path_rec, LocalPath};
use xxhash_rust::xxh3::xxh3_64;

use crate::{
    vfs::{DirTree, FileInfo, IsModified, ModificationState, TreeNode, Vfs, VirtualPath},
    Error,
};

use super::ConcreteFS;

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
}

impl ConcreteFS for LocalDir {
    type SyncInfo = LocalSyncInfo;

    type Error = Error;

    async fn load_virtual(&self) -> Result<Vfs<Self::SyncInfo>, Self::Error> {
        let sync = LocalSyncInfo::new(self.path.modification_time()?.into());
        let root = if self.path.is_file() {
            TreeNode::File(FileInfo::new(
                self.path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .ok_or(self.path.invalid_path_error())?,
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
            TreeNode::Dir(root)
        } else {
            return Err(self.path.invalid_path_error().into());
        };

        Ok(Vfs::new(root))
    }

    async fn open(&self, path: &VirtualPath) -> Result<impl std::io::Read, Self::Error> {
        File::open(self.path.join(path)).map_err(|e| e.into())
    }

    // TODO: rewrite using tokio::fs
    async fn write<Stream: std::io::Read>(
        &self,
        path: &VirtualPath,
        data: &mut Stream,
    ) -> Result<(), Self::Error> {
        let mut f = File::create(self.path.join(path))?;
        let mut buf = Vec::new();

        data.read_to_end(&mut buf)?;

        f.write_all(&buf).map_err(|e| e.into())
    }

    async fn mkdir(&self, path: &VirtualPath) -> Result<(), Self::Error> {
        fs::create_dir(self.path.join(path)).map_err(|e| e.into())
    }

    async fn hash(&self, path: &VirtualPath) -> Result<u64, Self::Error> {
        let mut reader = self.open(path).await?;
        let mut data = Vec::new();
        reader.read_to_end(&mut data)?;

        Ok(xxh3_64(&data))
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
