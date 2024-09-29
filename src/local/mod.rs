pub mod path;

use std::path::{Path, PathBuf};

use path::node_from_path_rec;

use crate::{
    vfs::{DirTree, FileInfo, TreeNode, Vfs},
    Error,
};

#[derive(Debug)]
#[allow(unused)]
pub struct LocalDir {
    path: PathBuf,
    vfs: Vfs,
}

impl LocalDir {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let path = path.as_ref();
        let root = if path.is_file() {
            TreeNode::File(FileInfo::new(
                path.file_name().and_then(|s| s.to_str()).ok_or(Error)?,
            ))
        } else if path.is_dir() {
            let mut root = DirTree::new(path.file_name().and_then(|s| s.to_str()).ok_or(Error)?);
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
            path: path.to_path_buf(),
            vfs: Vfs::new(
                path.file_name().and_then(|s| s.to_str()).ok_or(Error)?,
                root,
            ),
        })
    }
}
