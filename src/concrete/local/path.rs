use std::{
    ffi::OsStr,
    fs::{self},
    io,
    path::{Path, PathBuf},
    time::SystemTime,
};

use crate::{
    vfs::{DirTree, FileInfo, TreeNode},
    Error,
};

use super::LocalSyncInfo;

/// A path mapped to a local file-system that can be queried.
pub(crate) trait LocalPath {
    type DirEntry: LocalPath;
    fn is_file(&self) -> bool;
    fn is_dir(&self) -> bool;
    fn file_name(&self) -> Option<&OsStr>;
    fn read_dir(&self) -> io::Result<impl Iterator<Item = io::Result<Self::DirEntry>>>;
    fn modification_time(&self) -> io::Result<SystemTime>;
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
}

/// Creates a VFS node by recursively scanning a path
pub(crate) fn node_from_path_rec<P: LocalPath>(
    parent: &mut DirTree<LocalSyncInfo>,
    children: &[P],
) -> Result<(), Error> {
    for path in children {
        let sync = LocalSyncInfo::new(path.modification_time().map_err(|_| Error)?.into());
        if path.is_file() {
            let node = TreeNode::File(FileInfo::new(
                path.file_name().and_then(|s| s.to_str()).ok_or(Error)?,
                sync,
            ));
            parent.insert_child(node);
        } else if path.is_dir() {
            let mut node = DirTree::new(
                path.file_name().and_then(|s| s.to_str()).ok_or(Error)?,
                sync,
            );

            node_from_path_rec(
                &mut node,
                &path
                    .read_dir()
                    .map_err(|_| Error)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| Error)?,
            )?;

            parent.insert_child(TreeNode::Dir(node));
        } else {
            return Err(Error);
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use chrono::Utc;

    use super::*;

    use crate::vfs::{DirTree, TreeNode};

    #[test]
    fn test_parse_path() {
        use crate::test_utils::TestNode::{D, F};

        let base = vec![
            D("Doc", vec![F("f1.md"), F("f2.pdf")]),
            D("a", vec![D("b", vec![D("c", vec![])])]),
        ];

        let mut root = DirTree::new("", LocalSyncInfo::new(Utc::now()));
        node_from_path_rec(&mut root, &base).unwrap();

        let parsed = TreeNode::Dir(root);

        let reference = &D("", base).into_node();

        assert!(parsed.structural_eq(reference));
    }
}
