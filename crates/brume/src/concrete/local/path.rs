use std::{
    ffi::OsStr,
    fmt::Debug,
    fs,
    io::{self},
    path::{Path, PathBuf},
    time::SystemTime,
};

use crate::vfs::{DirTree, FileMeta, VfsNode};

use super::{LocalDirError, LocalSyncInfo};

/// A path mapped to a local file-system that can be queried.
pub(crate) trait LocalPath: Debug {
    type DirEntry: LocalPath;
    fn is_file(&self) -> bool;
    fn is_dir(&self) -> bool;
    fn file_name(&self) -> Option<&OsStr>;
    fn read_dir(&self) -> io::Result<impl Iterator<Item = io::Result<Self::DirEntry>>>;
    fn modification_time(&self) -> io::Result<SystemTime>;
    fn file_size(&self) -> io::Result<u64>;
}

impl LocalPath for Path {
    type DirEntry = PathBuf;
    fn is_file(&self) -> bool {
        self.is_file()
    }

    fn is_dir(&self) -> bool {
        self.is_dir()
    }

    fn file_name(&self) -> Option<&OsStr> {
        self.file_name()
    }

    fn read_dir(&self) -> io::Result<impl Iterator<Item = io::Result<Self::DirEntry>>> {
        fs::read_dir(self).map(|entry_iter| {
            entry_iter
                .into_iter()
                .map(|entry_res| entry_res.map(|entry| entry.path()))
        })
    }

    fn modification_time(&self) -> io::Result<SystemTime> {
        self.metadata()?.modified()
    }

    fn file_size(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }
}

impl LocalPath for PathBuf {
    type DirEntry = Self;
    fn is_file(&self) -> bool {
        LocalPath::is_file(self.as_path())
    }

    fn is_dir(&self) -> bool {
        LocalPath::is_dir(self.as_path())
    }

    fn file_name(&self) -> Option<&OsStr> {
        LocalPath::file_name(self.as_path())
    }

    fn read_dir(&self) -> io::Result<impl Iterator<Item = io::Result<Self::DirEntry>>> {
        LocalPath::read_dir(self.as_path())
    }

    fn modification_time(&self) -> io::Result<SystemTime> {
        LocalPath::modification_time(self.as_path())
    }

    fn file_size(&self) -> io::Result<u64> {
        LocalPath::file_size(self.as_path())
    }
}

/// Creates a VFS node by recursively scanning a list of path
pub(crate) fn node_from_path_rec<P: LocalPath>(
    parent: &mut DirTree<LocalSyncInfo>,
    children: &[P],
) -> Result<(), LocalDirError> {
    for path in children {
        let sync = match path.modification_time() {
            Ok(time) => LocalSyncInfo::new(time.into()),
            Err(err) => {
                // File might be an invalid symlink, so we just log and skip it
                println!("skipping file {path:?}: {err}");
                continue;
            }
        };

        if path.is_file() {
            let node = VfsNode::File(FileMeta::new(
                path.file_name()
                    .and_then(|s| s.to_str())
                    .ok_or(LocalDirError::invalid_path(path))?,
                path.file_size().map_err(|e| LocalDirError::io(path, e))?,
                sync,
            ));
            parent.insert_child(node);
        } else if path.is_dir() {
            let mut node = DirTree::new(
                path.file_name()
                    .and_then(|s| s.to_str())
                    .ok_or(LocalDirError::invalid_path(path))?,
                sync,
            );

            node_from_path_rec(
                &mut node,
                &path
                    .read_dir()
                    .map_err(|e| LocalDirError::io(path, e))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| LocalDirError::io(path, e))?,
            )?;

            parent.insert_child(VfsNode::Dir(node));
        } else {
            return Err(LocalDirError::invalid_path(path));
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use chrono::Utc;

    use super::*;

    use crate::vfs::{DirTree, VfsNode};

    #[test]
    fn test_parse_path() {
        use crate::test_utils::TestNode::{D, F};

        let base = vec![
            D("Doc", vec![F("f1.md"), F("f2.pdf")]),
            D("a", vec![D("b", vec![D("c", vec![])])]),
        ];

        let mut root = DirTree::new("", LocalSyncInfo::new(Utc::now()));
        node_from_path_rec(&mut root, &base).unwrap();

        let parsed = VfsNode::Dir(root);

        let reference = &D("", base).into_node();

        assert!(parsed.structural_eq(reference));
    }
}
