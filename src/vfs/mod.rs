//! An in-memory representation of a local or remote FileSystem

pub mod dir_tree;

pub use dir_tree::*;

/// The virtual representation of a file system, local or remote
#[derive(Debug)]
pub struct Vfs(TreeNode);

impl Vfs {
    pub fn root(&self) -> &TreeNode {
        &self.0
    }
}

impl From<TreeNode> for Vfs {
    fn from(value: TreeNode) -> Self {
        Self(value)
    }
}
