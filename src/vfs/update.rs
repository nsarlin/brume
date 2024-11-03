//! An update represents a difference between two file system nodes, and can be applied to make them
//! equivalent.

use std::cmp::Ordering;

use crate::{
    concrete::{concrete_eq_file, ConcreteFS},
    filesystem::FileSystem,
    sorted_list::{Sortable, SortedList},
    Error,
};

use super::{VirtualPath, VirtualPathBuf};

/// Error encountered during a diff operation
#[derive(Error, Debug)]
pub enum DiffError {
    #[error("the sync info for the path {0:?} are not valid")]
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
/// value). See [`NextcloudSyncInfo`] and [`LocalSyncInfo`] for examples.
///
/// [`LocalSyncInfo`]: crate::concrete::local::LocalSyncInfo
/// [`NextcloudSyncInfo`]: crate::concrete::nextcloud::NextcloudSyncInfo
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

/// A single node update
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum VfsNodeUpdate {
    DirCreated(VirtualPathBuf),
    DirRemoved(VirtualPathBuf),
    FileCreated(VirtualPathBuf),
    FileModified(VirtualPathBuf),
    FileRemoved(VirtualPathBuf),
    //TODO: detect moves
}

impl VfsNodeUpdate {
    pub fn path(&self) -> &VirtualPath {
        match self {
            VfsNodeUpdate::DirCreated(path) => path,
            VfsNodeUpdate::DirRemoved(path) => path,
            VfsNodeUpdate::FileCreated(path) => path,
            VfsNodeUpdate::FileModified(path) => path,
            VfsNodeUpdate::FileRemoved(path) => path,
        }
    }

    /// Updates from two synchronized VFS are "reconcilied" before being applied.
    ///
    /// The goal of this process is to:
    /// - remove duplicates (if the same modification has been done outside of the sync process on
    ///   both vfs)
    /// - detect conflicts (if different modifications have been done on the same node)
    pub async fn reconcile<Concrete: ConcreteFS, OtherConcrete: ConcreteFS>(
        &self,
        other: &VfsNodeUpdate,
        fs_self: &FileSystem<Concrete>,
        fs_other: &FileSystem<OtherConcrete>,
    ) -> Result<SortedList<ReconciliedUpdate>, Error>
    where
        Error: From<Concrete::Error>,
        Error: From<OtherConcrete::Error>,
    {
        if self.path() != other.path() {
            return Err(Error::InvalidPath(self.path().to_owned()));
        }

        let mut reconcilied = SortedList::new();

        let concrete_self = fs_self.concrete();
        let concrete_other = fs_other.concrete();

        match (self, other) {
            (VfsNodeUpdate::DirCreated(pself), VfsNodeUpdate::DirCreated(pother)) => {
                // If the same dir has been created on both sides, we need to check if they are
                // equivalent. If they are not, we generate updates that are only allowed to created
                // nodes.
                let dir_self = fs_self.vfs().root().find_dir(pself)?;
                let dir_other = fs_other.vfs().root().find_dir(pother)?;

                let reconcilied = dir_self
                    .concrete_diff(
                        dir_other,
                        concrete_self,
                        concrete_other,
                        self.path().parent().unwrap_or(VirtualPath::root()),
                    )
                    .await?
                    .iter()
                    .map(ReconciliedUpdate::new_creation_only)
                    .collect();
                // Since we iterate on sorted updates, the result will be sorted too
                Ok(SortedList::unchecked_from_vec(reconcilied))
            }
            (VfsNodeUpdate::DirRemoved(_), VfsNodeUpdate::DirRemoved(_)) => Ok(reconcilied),
            (VfsNodeUpdate::FileModified(_), VfsNodeUpdate::FileModified(_))
            | (VfsNodeUpdate::FileCreated(_), VfsNodeUpdate::FileCreated(_)) => {
                if concrete_eq_file(concrete_self, concrete_other, self.path()).await? {
                    Ok(reconcilied)
                } else {
                    reconcilied.insert(ReconciliedUpdate::Conflict(self.path().to_owned()));
                    Ok(reconcilied)
                }
            }
            (VfsNodeUpdate::FileRemoved(_), VfsNodeUpdate::FileRemoved(_)) => Ok(reconcilied),
            _ => {
                reconcilied.insert(ReconciliedUpdate::Conflict(self.path().to_owned()));
                Ok(reconcilied)
            }
        }
    }
}

impl Sortable for VfsNodeUpdate {
    type Key = VirtualPath;

    fn key(&self) -> &Self::Key {
        self.path()
    }
}

pub type SortedUpdateList = SortedList<VfsNodeUpdate>;

impl SortedUpdateList {
    /// Merge two update lists by calling [`SortedUpdateList::reconcile`] on their elmements one by
    /// one
    async fn merge<Concrete: ConcreteFS, OtherConcrete: ConcreteFS>(
        &self,
        other: SortedUpdateList,
        fs_self: &FileSystem<Concrete>,
        fs_other: &FileSystem<OtherConcrete>,
    ) -> Result<SortedList<ReconciliedUpdate>, Error>
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
                    // Propagate the update from self to other
                    ret.push(ReconciliedUpdate::synchronize_other(self_item));
                    self_item_opt = self_iter.next()
                }

                Ordering::Equal => {
                    ret.extend(Box::pin(self_item.reconcile(other_item, fs_self, fs_other)).await?);
                    self_item_opt = self_iter.next();
                    other_item_opt = other_iter.next();
                }
                Ordering::Greater => {
                    // Propagate the update from other to self
                    ret.push(ReconciliedUpdate::synchronize_self(other_item));
                    other_item_opt = other_iter.next();
                }
            }
        }

        // Handle the remaining items that are present in an iterator and not the
        // other one
        while let Some(self_item) = self_item_opt {
            ret.push(ReconciliedUpdate::synchronize_other(self_item));
            self_item_opt = self_iter.next();
        }

        while let Some(other_item) = other_item_opt {
            ret.push(ReconciliedUpdate::synchronize_self(other_item));
            other_item_opt = other_iter.next();
        }

        // Ok to use unchecked since we iterate on ordered updates
        Ok(SortedList::unchecked_from_vec(ret))
    }

    /// Reconcile two updates lists.
    ///
    /// This is done in two steps:
    /// - First reconcile individual elements by calling [`Self::merge`]
    /// - Then find conflicts with a directory and one of its elements with
    ///   [`SortedList<ReconciliedUpdate>::resolve_ancestor_conflicts`]
    pub async fn reconcile<Concrete: ConcreteFS, OtherConcrete: ConcreteFS>(
        &self,
        other: SortedUpdateList,
        fs_self: &FileSystem<Concrete>,
        fs_other: &FileSystem<OtherConcrete>,
    ) -> Result<SortedList<ReconciliedUpdate>, Error>
    where
        Error: From<Concrete::Error>,
        Error: From<OtherConcrete::Error>,
    {
        let merged = self.merge(other, fs_self, fs_other).await?;

        Ok(merged.resolve_ancestor_conflicts())
    }
}

/// The target of the update, from the point of view of the FileSystem on which `reconcile` has been
/// called
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum UpdateTarget {
    SelfFs,
    OtherFs,
}

/// An update that is ready to be synchronized
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SynchronizableUpdate {
    target: UpdateTarget,
    update: VfsNodeUpdate,
}

impl SynchronizableUpdate {
    pub fn new(target: UpdateTarget, update: &VfsNodeUpdate) -> Self {
        Self {
            target,
            update: update.clone(),
        }
    }
}

/// Output of the update reconciliation process. See [`VfsNodeUpdate::reconcile`]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconciliedUpdate {
    Syncronize(SynchronizableUpdate),
    Conflict(VirtualPathBuf),
}

impl ReconciliedUpdate {
    pub fn path(&self) -> &VirtualPath {
        match self {
            ReconciliedUpdate::Syncronize(update) => update.update.path(),
            ReconciliedUpdate::Conflict(path) => path.as_ref(),
        }
    }

    /// Create a new Reconcilied update that will only be able to "create" nodes.
    ///
    /// This means that:
    /// - DirCreated/FileCreated will be applied on SelfFs
    /// - DirRemoved/FileRemoved will be converted into a "created" update and applied to OtherFs
    /// - FileModified will generate a conflict
    pub fn new_creation_only(update: &VfsNodeUpdate) -> Self {
        match update {
            VfsNodeUpdate::DirCreated(_) | VfsNodeUpdate::FileCreated(_) => {
                Self::Syncronize(SynchronizableUpdate::new(UpdateTarget::SelfFs, update))
            }
            VfsNodeUpdate::DirRemoved(path) => Self::Syncronize(SynchronizableUpdate::new(
                UpdateTarget::OtherFs,
                &VfsNodeUpdate::DirCreated(path.clone()),
            )),
            VfsNodeUpdate::FileRemoved(path) => Self::Syncronize(SynchronizableUpdate::new(
                UpdateTarget::OtherFs,
                &VfsNodeUpdate::FileCreated(path.clone()),
            )),
            VfsNodeUpdate::FileModified(path) => Self::Conflict(path.to_owned()),
        }
    }

    /// Create a new `Synchronize` update with [`UpdateTarget::SelfFs`]
    pub fn synchronize_self(update: &VfsNodeUpdate) -> Self {
        ReconciliedUpdate::Syncronize(SynchronizableUpdate::new(UpdateTarget::SelfFs, update))
    }

    /// Create a new `Synchronize` update with [`UpdateTarget::OtherFs`]
    pub fn synchronize_other(update: &VfsNodeUpdate) -> Self {
        ReconciliedUpdate::Syncronize(SynchronizableUpdate::new(UpdateTarget::OtherFs, update))
    }
}

impl Sortable for ReconciliedUpdate {
    type Key = VirtualPath;

    fn key(&self) -> &Self::Key {
        self.path()
    }
}

impl SortedList<ReconciliedUpdate> {
    /// Find conflicts between updates on an element and a directory on its path.
    ///
    /// For example, `DirRemoved("/a/b")` and `FileModified("/a/b/c/d")` will generate conflicts on
    /// both `/a/b` and `/a/b/c/d`.
    pub fn resolve_ancestor_conflicts(self) -> Self {
        let mut resolved = Vec::new();
        let mut iter = self.into_iter().peekable();

        while let Some(mut update) = iter.next() {
            let mut conflict = false;
            let mut updates = Vec::new();
            while let Some(mut next_update) =
                iter.next_if(|next_update| next_update.path().is_inside(update.path()))
            {
                conflict = true;
                next_update = ReconciliedUpdate::Conflict(next_update.path().to_owned());
                updates.push(next_update);
            }

            if conflict {
                update = ReconciliedUpdate::Conflict(update.path().to_owned())
            }

            resolved.push(update);
            resolved.extend(updates);
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
        let mut local_fs = FileSystem::new(local_base);
        local_fs.update_vfs().await.unwrap();

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );

        let mut remote_fs = FileSystem::new(remote_base);
        remote_fs.update_vfs().await.unwrap();

        let local_diff = SortedList::from([
            VfsNodeUpdate::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodeUpdate::DirRemoved(VirtualPathBuf::new("/a/b").unwrap()),
            VfsNodeUpdate::DirCreated(VirtualPathBuf::new("/e").unwrap()),
        ]);

        let remote_diff = local_diff.clone();

        let reconcilied = local_diff
            .reconcile(remote_diff, &local_fs, &remote_fs)
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
        let mut local_fs = FileSystem::new(local_base);
        local_fs.update_vfs().await.unwrap();

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );
        let mut remote_fs = FileSystem::new(remote_base);
        remote_fs.update_vfs().await.unwrap();

        let local_diff = SortedList::from([
            VfsNodeUpdate::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodeUpdate::DirCreated(VirtualPathBuf::new("/e").unwrap()),
        ]);

        let remote_diff = SortedList::from([VfsNodeUpdate::DirRemoved(
            VirtualPathBuf::new("/a/b").unwrap(),
        )]);

        let reconcilied = local_diff
            .reconcile(remote_diff, &local_fs, &remote_fs)
            .await
            .unwrap();

        let reconcilied_ref = SortedList::from([
            ReconciliedUpdate::synchronize_other(&VfsNodeUpdate::FileModified(
                VirtualPathBuf::new("/Doc/f1.md").unwrap(),
            )),
            ReconciliedUpdate::synchronize_self(&VfsNodeUpdate::DirRemoved(
                VirtualPathBuf::new("/a/b").unwrap(),
            )),
            ReconciliedUpdate::synchronize_other(&VfsNodeUpdate::DirCreated(
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
        let mut local_fs = FileSystem::new(local_base);
        local_fs.update_vfs().await.unwrap();

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
        let mut remote_fs = FileSystem::new(remote_base);
        remote_fs.update_vfs().await.unwrap();

        let local_diff = SortedList::from([
            VfsNodeUpdate::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodeUpdate::DirRemoved(VirtualPathBuf::new("/a/b").unwrap()),
        ]);

        let remote_diff = SortedList::from([
            VfsNodeUpdate::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodeUpdate::FileCreated(VirtualPathBuf::new("/a/b/test.log").unwrap()),
        ]);

        let reconcilied = local_diff
            .reconcile(remote_diff, &local_fs, &remote_fs)
            .await
            .unwrap();

        let reconcilied_ref = SortedList::from([
            ReconciliedUpdate::Conflict(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            ReconciliedUpdate::Conflict(VirtualPathBuf::new("/a/b").unwrap()),
            ReconciliedUpdate::Conflict(VirtualPathBuf::new("/a/b/test.log").unwrap()),
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
        let mut local_fs = FileSystem::new(local_base);
        local_fs.update_vfs().await.unwrap();

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
        let mut remote_fs = FileSystem::new(remote_base);
        remote_fs.update_vfs().await.unwrap();

        let local_diff =
            SortedList::from([VfsNodeUpdate::DirCreated(VirtualPathBuf::new("/").unwrap())]);

        let remote_diff = local_diff.clone();

        let reconcilied = local_diff
            .reconcile(remote_diff, &local_fs, &remote_fs)
            .await
            .unwrap();

        let reconcilied_ref = SortedList::from([
            ReconciliedUpdate::Conflict(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            ReconciliedUpdate::synchronize_self(&VfsNodeUpdate::FileCreated(
                VirtualPathBuf::new("/a/b/test.log").unwrap(),
            )),
            ReconciliedUpdate::synchronize_other(&VfsNodeUpdate::DirCreated(
                VirtualPathBuf::new("/e/g").unwrap(),
            )),
        ]);

        assert_eq!(reconcilied, reconcilied_ref);
    }
}
