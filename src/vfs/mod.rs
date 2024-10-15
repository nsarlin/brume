//! An in-memory representation of a FileSystem, local or remote

pub mod diff;
pub mod dir_tree;
pub mod virtual_path;

pub use diff::*;
pub use dir_tree::*;
pub use virtual_path::*;

use crate::concrete::ConcreteFS;

/// The virtual representation of a file system, local or remote.
///
/// `SyncInfo` is a metadata type that will be stored with FS nodes and used for more efficient VFS
/// comparison, using their implementation of the [`IsModified`] trait.
#[derive(Debug)]
pub struct Vfs<Concrete: ConcreteFS> {
    concrete: Concrete,
    root: TreeNode<Concrete::SyncInfo>,
}

impl<Concrete: ConcreteFS> Vfs<Concrete> {
    /// Return the root node of the VFS
    pub fn root(&self) -> &TreeNode<Concrete::SyncInfo> {
        &self.root
    }

    /// Structural comparison of two VFS, looking at the names of files and directories, but
    /// ignoring the content of files.
    pub fn structural_eq<OtherConcrete: ConcreteFS>(&self, other: &Vfs<OtherConcrete>) -> bool {
        self.root.structural_eq(other.root())
    }

    /// Diff two VFS by comparing their nodes.
    ///
    /// This function returns a list of differences.
    /// The node comparison is based on the `SyncInfo` and might be recursive based on the result of
    /// [`modification_state`]. The result of the SyncInfo comparison on node is trusted.
    ///
    /// [`modification_state`]: IsModified::modification_state
    pub fn diff<OtherConcrete: ConcreteFS>(
        &self,
        other: &Vfs<OtherConcrete>,
    ) -> Result<Vec<VfsNodeDiff<OtherConcrete::SyncInfo>>, DiffError>
    where
        Concrete::SyncInfo: IsModified<OtherConcrete::SyncInfo> + Clone,
        OtherConcrete::SyncInfo: Clone,
    {
        self.root.diff(other.root(), VirtualPath::root())
    }

    pub fn concrete(&self) -> &Concrete {
        &self.concrete
    }
}

impl<Concrete: ConcreteFS> Vfs<Concrete> {
    pub fn new(concrete: Concrete, root: TreeNode<Concrete::SyncInfo>) -> Self {
        Self { concrete, root }
    }
}
