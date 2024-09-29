//! An in-memory representation of a local or remote FileSystem

pub mod dir_tree;

pub use dir_tree::*;

/// The virtual representation of a file system, local or remote
#[derive(Debug)]
#[allow(unused)]
pub struct Vfs {
    dir_name: String,
    root: TreeNode,
}

impl Vfs {
    pub fn root(&self) -> &TreeNode {
        &self.root
    }
}

impl Vfs {
    pub fn new(name: &str, root: TreeNode) -> Self {
        Self {
            dir_name: name.to_string(),
            root,
        }
    }
}
