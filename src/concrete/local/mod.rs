//! Interactions with the local filesystem

pub mod path;

use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use path::{node_from_path_rec, LocalPath};

use crate::{
    concrete::remote::RemoteSyncInfo,
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

    async fn load_virtual(self) -> Result<Vfs<Self>, Self::Error> {
        let sync = LocalSyncInfo::new(self.path.modification_time().map_err(|_| Error)?.into());
        let root = if self.path.is_file() {
            TreeNode::File(FileInfo::new(
                self.path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .ok_or(Error)?,
                sync,
            ))
        } else if self.path.is_dir() {
            let mut root = DirTree::new(
                self.path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .ok_or(Error)?,
                sync,
            );
            node_from_path_rec(
                &mut root,
                &self
                    .path
                    .read_dir()
                    .map_err(|_| Error)?
                    .map(|entry| entry.unwrap())
                    .collect::<Vec<_>>(),
            )?;
            TreeNode::Dir(root)
        } else {
            return Err(Error);
        };

        Ok(Vfs::new(self, root))
    }

    async fn open(&self, path: &VirtualPath) -> Result<impl std::io::Read, Self::Error> {
        File::open(self.path.join(path)).map_err(|_| Error)
    }

    // TODO: rewrite using tokio::fs
    async fn write<Stream: std::io::Read>(
        &self,
        path: &VirtualPath,
        data: &mut Stream,
    ) -> Result<(), Self::Error> {
        let mut f = File::create(self.path.join(path)).map_err(|_| Error)?;
        let mut buf = Vec::new();

        data.read_to_end(&mut buf).unwrap(); // TODO: handle io error

        f.write_all(&buf).map_err(|_| Error)
    }

    async fn mkdir(&self, path: &VirtualPath) -> Result<(), Self::Error> {
        fs::create_dir(self.path.join(path)).map_err(|_| Error)
    }
}

/// Metadata used to detect modifications of a local FS node
///
/// It is based on the modification time which is not recursive for directories, so we have to
/// handle the recursion ourselves.
#[derive(Debug, Clone)]
pub struct LocalSyncInfo {
    pub last_modified: DateTime<Utc>,
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

impl IsModified<RemoteSyncInfo> for LocalSyncInfo {
    fn modification_state(&self, reference: &RemoteSyncInfo) -> ModificationState {
        if self.last_modified != reference.last_modified() {
            ModificationState::Modified
        } else {
            ModificationState::ShallowUnmodified
        }
    }
}
