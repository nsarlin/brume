//! An in-memory representation of a FileSystem using a tree structure

pub mod dir_tree;
pub mod update;
pub mod virtual_path;

pub use dir_tree::*;
pub use update::*;
pub use virtual_path::*;

/// The virtual representation of a file system, local or remote.
///
/// `SyncInfo` is a metadata type that will be stored with FS nodes and used for more efficient VFS
/// comparison, using their implementation of the [`IsModified`] trait.
#[derive(Debug)]
pub struct Vfs<SyncInfo> {
    root: TreeNode<SyncInfo>,
}

impl<SyncInfo> Vfs<SyncInfo> {
    /// Return the root node of the VFS
    pub fn root(&self) -> &TreeNode<SyncInfo> {
        &self.root
    }

    /// Structural comparison of two VFS, looking at the names of files and directories, but
    /// ignoring the content of files.
    pub fn structural_eq<OtherSyncInfo>(&self, other: &Vfs<OtherSyncInfo>) -> bool {
        self.root.structural_eq(other.root())
    }
}

impl<SyncInfo: IsModified<SyncInfo> + Clone> Vfs<SyncInfo> {
    /// Diff two VFS by comparing their nodes.
    ///
    /// This function returns a sorted list of [`VfsNodeUpdate`].
    /// The node comparison is based on the `SyncInfo` and might be recursive based on the result of
    /// [`modification_state`]. The result of the SyncInfo comparison on node is trusted.
    ///
    /// [`modification_state`]: IsModified::modification_state
    pub fn diff(&self, other: &Vfs<SyncInfo>) -> Result<SortedUpdateList, DiffError> {
        self.root.diff(other.root(), VirtualPath::root())
    }
}

impl<SyncInfo> Vfs<SyncInfo> {
    pub fn new(root: TreeNode<SyncInfo>) -> Self {
        Self { root }
    }

    pub fn empty() -> Self {
        Self {
            root: TreeNode::Dir(DirTree::new_invalid("")),
        }
    }
}
