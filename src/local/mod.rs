//! Interactions with the local filesystem

pub mod path;

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use path::{node_from_path_rec, LocalPath};

use crate::{
    remote::RemoteSyncInfo,
    vfs::{DirTree, FileInfo, IsModified, ModificationState, TreeNode, Vfs},
    Error,
};

/// Represent a local directory and its content
#[derive(Debug)]
pub struct LocalDir {
    _path: PathBuf,
    _vfs: Vfs<LocalSyncInfo>,
}

impl LocalDir {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let path = path.as_ref();
        let sync = LocalSyncInfo::new(path.modification_time().map_err(|_| Error)?.into());
        let root = if path.is_file() {
            TreeNode::File(FileInfo::new(
                path.file_name().and_then(|s| s.to_str()).ok_or(Error)?,
                sync,
            ))
        } else if path.is_dir() {
            let mut root = DirTree::new(
                path.file_name().and_then(|s| s.to_str()).ok_or(Error)?,
                sync,
            );
            node_from_path_rec(
                &mut root,
                &path
                    .read_dir()
                    .map(|entry_iter| {
                        entry_iter
                            .into_iter()
                            .map(|entry_res| entry_res.map(|entry| entry.path()))
                    })
                    .map_err(|_| Error)?
                    .map(|entry| entry.unwrap())
                    .collect::<Vec<_>>(),
            )?;
            TreeNode::Dir(root)
        } else {
            return Err(Error);
        };

        Ok(Self {
            _path: path.to_path_buf(),
            _vfs: Vfs::new(
                path.file_name().and_then(|s| s.to_str()).ok_or(Error)?,
                root,
            ),
        })
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
