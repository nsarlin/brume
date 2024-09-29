use std::{
    ffi::OsStr,
    fs::{self},
    io,
    path::{Path, PathBuf},
};

use crate::{
    vfs::{DirTree, FileInfo, TreeNode},
    Error,
};

/// Interface for the main operations done on Path, to be able to mock them
pub(crate) trait PathLike {
    type DirEntry: PathLike;
    fn is_file(&self) -> bool;
    fn is_dir(&self) -> bool;
    fn file_name(&self) -> Option<&OsStr>;
    fn read_dir(&self) -> io::Result<impl Iterator<Item = io::Result<Self::DirEntry>>>;
}

impl PathLike for Path {
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
}

impl PathLike for PathBuf {
    type DirEntry = Self;
    fn is_file(&self) -> bool {
        PathLike::is_file(self.as_path())
    }

    fn is_dir(&self) -> bool {
        PathLike::is_dir(self.as_path())
    }

    fn file_name(&self) -> Option<&OsStr> {
        PathLike::file_name(self.as_path())
    }

    fn read_dir(&self) -> io::Result<impl Iterator<Item = io::Result<Self::DirEntry>>> {
        PathLike::read_dir(self.as_path())
    }
}

/// Creates a VFS node by recursively scanning a path
pub(crate) fn node_from_path_rec<P: PathLike>(
    parent: &mut DirTree,
    children: &[P],
) -> Result<(), Error> {
    for path in children {
        if path.is_file() {
            let node = TreeNode::File(FileInfo::new(
                path.file_name().and_then(|s| s.to_str()).ok_or(Error)?,
            ));
            parent.insert_child(node);
        } else if path.is_dir() {
            let mut node = DirTree::new(path.file_name().and_then(|s| s.to_str()).ok_or(Error)?);

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
    use crate::{
        local::path::node_from_path_rec,
        vfs::{DirTree, TreeNode},
    };

    #[test]
    fn test_parse_path() {
        use crate::test_utils::TestNode::{D, F};

        let base = vec![
            D("Doc", vec![F("f1.md"), F("f2.pdf")]),
            D("a", vec![D("b", vec![D("c", vec![])])]),
        ];

        let mut root = DirTree::new("");
        node_from_path_rec(&mut root, &base).unwrap();

        let parsed = TreeNode::Dir(root);

        let reference = TreeNode::from(&D("", base));

        assert!(parsed.structural_eq(&reference));
    }
}
