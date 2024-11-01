//! A patch represents a difference between two file system nodes, and can be applied to make them
//! equivalent.

use std::cmp::Ordering;

use crate::{
    concrete::{concrete_eq_file, ConcreteFS},
    sorted_list::{Sortable, SortedList},
    Error,
};

use super::{Vfs, VirtualPath, VirtualPathBuf};

/// Error encountered during a diff operation
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
///
/// [`LocalSyncInfo`]: crate::concrete::local::LocalSyncInfo
/// [`RemoteSyncInfo`]: crate::concrete::remote::RemoteSyncInfo
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

/// A single node patch
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum VfsNodePatch {
    DirCreated(VirtualPathBuf),
    DirRemoved(VirtualPathBuf),
    FileCreated(VirtualPathBuf),
    FileModified(VirtualPathBuf),
    FileRemoved(VirtualPathBuf),
    //TODO: detect moves
}

impl VfsNodePatch {
    pub fn path(&self) -> &VirtualPath {
        match self {
            VfsNodePatch::DirCreated(path) => path,
            VfsNodePatch::DirRemoved(path) => path,
            VfsNodePatch::FileCreated(path) => path,
            VfsNodePatch::FileModified(path) => path,
            VfsNodePatch::FileRemoved(path) => path,
        }
    }

    /// Patches from the local and remote VFS are "reconcilied" before being applied.
    ///
    /// The goal of this process is to:
    /// - remove duplicates (if the same modification has been done outside of the sync process on
    ///   both vfs)
    /// - detect conflicts (if different modifications have been done on the same node)
    pub async fn reconcile<Concrete: ConcreteFS, OtherConcrete: ConcreteFS>(
        &self,
        other: &VfsNodePatch,
        vfs_self: &Vfs<Concrete::SyncInfo>,
        vfs_other: &Vfs<OtherConcrete::SyncInfo>,
        concrete_self: &Concrete,
        concrete_other: &OtherConcrete,
    ) -> Result<SortedList<ReconciliedPatch>, Error>
    where
        Error: From<Concrete::Error>,
        Error: From<OtherConcrete::Error>,
    {
        // TODO: handle error
        assert_eq!(self.path(), other.path());

        let mut reconcilied = SortedList::new();

        match (self, other) {
            (VfsNodePatch::DirCreated(pself), VfsNodePatch::DirCreated(pother)) => {
                // If the same dir has been created on both sides, we need to check if they are
                // equivalent. If they are not, we generate patches that are only allowed to created
                // nodes.
                let dir_self = vfs_self.root().find_dir(pself)?;
                let dir_other = vfs_other.root().find_dir(pother)?;

                let reconcilied = dir_self
                    .concrete_diff(
                        dir_other,
                        concrete_self,
                        concrete_other,
                        self.path().parent().unwrap_or(VirtualPath::root()),
                    )
                    .await?
                    .iter()
                    .map(ReconciliedPatch::new_creation_only)
                    .collect();
                // Since we iterate on sorted patches, the result will be sorted too
                Ok(SortedList::unchecked_from_vec(reconcilied))
            }
            (VfsNodePatch::DirRemoved(_), VfsNodePatch::DirRemoved(_)) => Ok(reconcilied),
            (VfsNodePatch::FileModified(_), VfsNodePatch::FileModified(_))
            | (VfsNodePatch::FileCreated(_), VfsNodePatch::FileCreated(_)) => {
                if concrete_eq_file(concrete_self, concrete_other, self.path()).await? {
                    Ok(reconcilied)
                } else {
                    reconcilied.insert(ReconciliedPatch::Conflict(self.path().to_owned()));
                    Ok(reconcilied)
                }
            }
            (VfsNodePatch::FileRemoved(_), VfsNodePatch::FileRemoved(_)) => Ok(reconcilied),
            _ => {
                reconcilied.insert(ReconciliedPatch::Conflict(self.path().to_owned()));
                Ok(reconcilied)
            }
        }
    }
}

impl Sortable for VfsNodePatch {
    type Key = VirtualPath;

    fn key(&self) -> &Self::Key {
        self.path()
    }
}

pub type SortedPatchList = SortedList<VfsNodePatch>;

impl SortedPatchList {
    /// Merge two patch lists by calling `VfsPatchList::reconcile` on their elmements one by one
    async fn merge<Concrete: ConcreteFS, OtherConcrete: ConcreteFS>(
        &self,
        other: SortedPatchList,
        vfs_self: &Vfs<Concrete::SyncInfo>,
        vfs_other: &Vfs<OtherConcrete::SyncInfo>,
        concrete_self: &Concrete,
        concrete_other: &OtherConcrete,
    ) -> Result<SortedList<ReconciliedPatch>, Error>
    where
        Error: From<Concrete::Error>,
        Error: From<OtherConcrete::Error>,
    {
        // Here we cannot use `iter_zip_map` or an async variant of it because it does not seem
        // possible to express the lifetimes required by the async closures

        let mut ret = Vec::new();
        let mut self_iter = self.iter();
        let mut other_iter = other.iter();

        let mut self_item_opt = self_iter.next();
        let mut other_item_opt = other_iter.next();

        while let (Some(self_item), Some(other_item)) = (self_item_opt, other_item_opt) {
            match self_item.key().cmp(other_item.key()) {
                Ordering::Less => {
                    // Propagate the patch from self to other
                    ret.push(ReconciliedPatch::synchronize_other(self_item));
                    self_item_opt = self_iter.next()
                }

                Ordering::Equal => {
                    ret.extend(
                        Box::pin(self_item.reconcile(
                            other_item,
                            vfs_self,
                            vfs_other,
                            concrete_self,
                            concrete_other,
                        ))
                        .await?,
                    );
                    self_item_opt = self_iter.next();
                    other_item_opt = other_iter.next();
                }
                Ordering::Greater => {
                    // Propagate the patch from other to self
                    ret.push(ReconciliedPatch::synchronize_self(other_item));
                    other_item_opt = other_iter.next();
                }
            }
        }

        // Handle the remaining items that are present in an iterator and not the
        // other one
        while let Some(self_item) = self_item_opt {
            ret.push(ReconciliedPatch::synchronize_other(self_item));
            self_item_opt = self_iter.next();
        }

        while let Some(other_item) = other_item_opt {
            ret.push(ReconciliedPatch::synchronize_self(other_item));
            other_item_opt = other_iter.next();
        }

        // Ok to use unchecked since we iterate on ordered patches
        Ok(SortedList::unchecked_from_vec(ret))
    }

    /// Reconcile two patches lists.
    ///
    /// This is done in two steps:
    /// - First reconcile individual elements by calling [`Self::merge`]
    /// - Then find conflicts with a directory and one of its elements with
    ///   [`SortedList<ReconciliedPatch>::resolve_ancestor_conflicts`]
    pub async fn reconcile<Concrete: ConcreteFS, OtherConcrete: ConcreteFS>(
        &self,
        other: SortedPatchList,
        vfs_self: &Vfs<Concrete::SyncInfo>,
        vfs_other: &Vfs<OtherConcrete::SyncInfo>,
        concrete_self: &Concrete,
        concrete_other: &OtherConcrete,
    ) -> Result<SortedList<ReconciliedPatch>, Error>
    where
        Error: From<Concrete::Error>,
        Error: From<OtherConcrete::Error>,
    {
        let merged = self
            .merge(other, vfs_self, vfs_other, concrete_self, concrete_other)
            .await?;

        Ok(merged.resolve_ancestor_conflicts())
    }
}

/// The target of the patch, from the point of view of the FileSystem on which `reconcile` has been
/// called
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum PatchTarget {
    SelfFs,
    OtherFs,
}

/// A patch that is ready to be synchronized
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SynchronizablePatch {
    target: PatchTarget,
    patch: VfsNodePatch,
}

impl SynchronizablePatch {
    pub fn new(target: PatchTarget, patch: &VfsNodePatch) -> Self {
        Self {
            target,
            patch: patch.clone(),
        }
    }
}

/// Output of the patch reconciliation process. See [`VfsNodePatch::reconcile`]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconciliedPatch {
    Syncronize(SynchronizablePatch),
    Conflict(VirtualPathBuf),
}

impl ReconciliedPatch {
    pub fn path(&self) -> &VirtualPath {
        match self {
            ReconciliedPatch::Syncronize(patch) => patch.patch.path(),
            ReconciliedPatch::Conflict(path) => path.as_ref(),
        }
    }

    /// Created a new Reconcilied patch that will only be able to "create" nodes.
    ///
    /// This means that:
    /// - DirCreated/FileCreated will be applied on SelfFs
    /// - DirRemoved/FileRemoved will be converted into a "created" patch and applied to OtherFs
    /// - FileModified will generate a conflict
    pub fn new_creation_only(patch: &VfsNodePatch) -> Self {
        match patch {
            VfsNodePatch::DirCreated(_) | VfsNodePatch::FileCreated(_) => {
                Self::Syncronize(SynchronizablePatch::new(PatchTarget::SelfFs, patch))
            }
            VfsNodePatch::DirRemoved(path) => Self::Syncronize(SynchronizablePatch::new(
                PatchTarget::OtherFs,
                &VfsNodePatch::DirCreated(path.clone()),
            )),
            VfsNodePatch::FileRemoved(path) => Self::Syncronize(SynchronizablePatch::new(
                PatchTarget::OtherFs,
                &VfsNodePatch::FileCreated(path.clone()),
            )),
            VfsNodePatch::FileModified(path) => Self::Conflict(path.to_owned()),
        }
    }

    /// Create a new `Synchronize` patch with [`PatchTarget::SelfFs`]
    pub fn synchronize_self(patch: &VfsNodePatch) -> Self {
        ReconciliedPatch::Syncronize(SynchronizablePatch::new(PatchTarget::SelfFs, patch))
    }

    /// Create a new `Synchronize` patch with [`PatchTarget::OtherFs`]
    pub fn synchronize_other(patch: &VfsNodePatch) -> Self {
        ReconciliedPatch::Syncronize(SynchronizablePatch::new(PatchTarget::OtherFs, patch))
    }
}

impl Sortable for ReconciliedPatch {
    type Key = VirtualPath;

    fn key(&self) -> &Self::Key {
        self.path()
    }
}

impl SortedList<ReconciliedPatch> {
    /// Find conflicts between patches on an element and a directory on its path.
    ///
    /// For example, `DirRemoved("/a/b")` and `FileModified("/a/b/c/d")` will generate conflicts on
    /// both `/a/b` and `/a/b/c/d`.
    pub fn resolve_ancestor_conflicts(self) -> Self {
        let mut resolved = Vec::new();
        let mut iter = self.into_iter().peekable();

        while let Some(mut patch) = iter.next() {
            let mut conflict = false;
            let mut patches = Vec::new();
            while let Some(mut next_patch) =
                iter.next_if(|next_patch| next_patch.path().is_inside(patch.path()))
            {
                conflict = true;
                next_patch = ReconciliedPatch::Conflict(next_patch.path().to_owned());
                patches.push(next_patch);
            }

            if conflict {
                patch = ReconciliedPatch::Conflict(patch.path().to_owned())
            }

            resolved.push(patch);
            resolved.extend(patches);
        }

        SortedList::unchecked_from_vec(resolved)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::TestNode::{D, FF};

    /// Check that duplicate diffs are correctly removed
    #[tokio::test]
    async fn test_reconciliation_same_diffs() {
        let local_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );
        let local_vfs = Vfs::new(local_base.clone().into_node());

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );

        let remote_vfs = Vfs::new(remote_base.clone().into_node());

        let local_diff = SortedList::from([
            VfsNodePatch::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodePatch::DirRemoved(VirtualPathBuf::new("/a/b").unwrap()),
            VfsNodePatch::DirCreated(VirtualPathBuf::new("/e").unwrap()),
        ]);

        let remote_diff = local_diff.clone();

        let reconcilied = local_diff
            .reconcile(
                remote_diff,
                &local_vfs,
                &remote_vfs,
                &local_base,
                &remote_base,
            )
            .await
            .unwrap();

        assert!(reconcilied.is_empty());
    }

    /// Check that diffs only present on one side are all kept
    #[tokio::test]
    async fn test_reconciliation_missing() {
        let local_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );
        let local_vfs = Vfs::new(local_base.clone().into_node());

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );
        let remote_vfs = Vfs::new(remote_base.clone().into_node());

        let local_diff = SortedList::from([
            VfsNodePatch::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodePatch::DirCreated(VirtualPathBuf::new("/e").unwrap()),
        ]);

        let remote_diff = SortedList::from([VfsNodePatch::DirRemoved(
            VirtualPathBuf::new("/a/b").unwrap(),
        )]);

        let reconcilied = local_diff
            .reconcile(
                remote_diff,
                &local_vfs,
                &remote_vfs,
                &local_base,
                &remote_base,
            )
            .await
            .unwrap();

        let reconcilied_ref = SortedList::from([
            ReconciliedPatch::synchronize_other(&VfsNodePatch::FileModified(
                VirtualPathBuf::new("/Doc/f1.md").unwrap(),
            )),
            ReconciliedPatch::synchronize_self(&VfsNodePatch::DirRemoved(
                VirtualPathBuf::new("/a/b").unwrap(),
            )),
            ReconciliedPatch::synchronize_other(&VfsNodePatch::DirCreated(
                VirtualPathBuf::new("/e").unwrap(),
            )),
        ]);

        assert_eq!(reconcilied, reconcilied_ref);
    }

    /// Test conflict detection
    #[tokio::test]
    async fn test_reconciliation_conflict() {
        let local_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );
        let local_vfs = Vfs::new(local_base.clone().into_node());

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hell"), FF("f2.pdf", b"world")]),
                D(
                    "a",
                    vec![D("b", vec![D("c", vec![]), FF("test.log", b"value")])],
                ),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );
        let remote_vfs = Vfs::new(remote_base.clone().into_node());

        let local_diff = SortedList::from([
            VfsNodePatch::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodePatch::DirRemoved(VirtualPathBuf::new("/a/b").unwrap()),
        ]);

        let remote_diff = SortedList::from([
            VfsNodePatch::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodePatch::FileCreated(VirtualPathBuf::new("/a/b/test.log").unwrap()),
        ]);

        let reconcilied = local_diff
            .reconcile(
                remote_diff,
                &local_vfs,
                &remote_vfs,
                &local_base,
                &remote_base,
            )
            .await
            .unwrap();

        let reconcilied_ref = SortedList::from([
            ReconciliedPatch::Conflict(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            ReconciliedPatch::Conflict(VirtualPathBuf::new("/a/b").unwrap()),
            ReconciliedPatch::Conflict(VirtualPathBuf::new("/a/b/test.log").unwrap()),
        ]);

        assert_eq!(reconcilied, reconcilied_ref);
    }

    #[tokio::test]
    async fn test_reconciliation_created_dir() {
        let local_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );
        let local_vfs = Vfs::new(local_base.clone().into_node());

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hell"), FF("f2.pdf", b"world")]),
                D(
                    "a",
                    vec![D("b", vec![D("c", vec![]), FF("test.log", b"value")])],
                ),
                D("e", vec![]),
            ],
        );
        let remote_vfs = Vfs::new(remote_base.clone().into_node());

        let local_diff =
            SortedList::from([VfsNodePatch::DirCreated(VirtualPathBuf::new("/").unwrap())]);

        let remote_diff = local_diff.clone();

        let reconcilied = local_diff
            .reconcile(
                remote_diff,
                &local_vfs,
                &remote_vfs,
                &local_base,
                &remote_base,
            )
            .await
            .unwrap();

        let reconcilied_ref = SortedList::from([
            ReconciliedPatch::Conflict(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            ReconciliedPatch::synchronize_self(&VfsNodePatch::FileCreated(
                VirtualPathBuf::new("/a/b/test.log").unwrap(),
            )),
            ReconciliedPatch::synchronize_other(&VfsNodePatch::DirCreated(
                VirtualPathBuf::new("/e/g").unwrap(),
            )),
        ]);

        assert_eq!(reconcilied, reconcilied_ref);
    }
}
