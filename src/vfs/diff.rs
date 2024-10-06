//! Differences between two file system nodes

use super::{DirTree, VirtualPathBuf};

/// Error encountered during a diff
#[derive(Debug)]
pub enum DiffError {
    /// The sync info are not valid, for example wrongly indicating that a node is unmodified
    InvalidSyncInfo(VirtualPathBuf),
}

/// Result of a VFS node comparison.
pub enum ModificationState {
    /// The current node has not been modified, but this says nothing about its children
    ShallowUnmodified,
    /// The current node and its children have not been modified
    RecursiveUnmodified,
    /// The current node or its children are modified
    Modified,
}

/// Trait used for VFS node comparisons
///
/// This trait is implemented for the "SyncInfo" types, to
/// allow different node comparison stategies (for example based on timestamp or a revision id
/// value). See [`RemoteSyncInfo`] and [`LocalSyncInfo`] for examples.
pub trait IsModified<Ref> {
    /// Tell if a node have been modified, and if possible also recusively answers for its children
    fn modification_state(&self, reference: &Ref) -> ModificationState;

    /// Return a boolean telling if the node itself have been modified
    fn is_modified(&self, reference: &Ref) -> bool {
        match self.modification_state(reference) {
            ModificationState::ShallowUnmodified => false,
            ModificationState::RecursiveUnmodified => false,
            ModificationState::Modified => true,
        }
    }
}

/// A directory creation diff, with its full VFS tree
#[derive(Debug)]
pub struct DirCreatedDiff<SyncInfo> {
    path: VirtualPathBuf,
    created_dir: DirTree<SyncInfo>,
}

impl<SyncInfo, OtherSyncInfo> PartialEq<DirCreatedDiff<OtherSyncInfo>>
    for DirCreatedDiff<SyncInfo>
{
    fn eq(&self, other: &DirCreatedDiff<OtherSyncInfo>) -> bool {
        self.path == other.path && self.created_dir.structural_eq(&other.created_dir)
    }
}

impl<SyncInfo> Eq for DirCreatedDiff<SyncInfo> {}

impl<SyncInfo> DirCreatedDiff<SyncInfo> {
    pub fn new(path: VirtualPathBuf, created_dir: DirTree<SyncInfo>) -> Self {
        Self { path, created_dir }
    }
}

/// A single node diff
#[derive(Eq, PartialEq, Debug)]
pub enum VfsNodeDiff<SyncInfo> {
    DirCreated(DirCreatedDiff<SyncInfo>),
    DirRemoved(VirtualPathBuf),
    FileCreated(VirtualPathBuf),
    FileModified(VirtualPathBuf),
    FileRemoved(VirtualPathBuf),
}
