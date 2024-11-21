//! An update represents a difference between two file system nodes.
//!
//! For 2 filesystems A and B that are kept in sync, the lifecycle of an update comes in 3 steps:
//! 1. **VFS diff**: The [`VFS`] of each filesystem is loaded from the [`ConcreteFS`] and compared
//!    with its previous version. This will create two separated [`VfsUpdateList`], one for A and
//!    one for B. The comparison does not access the concrete filesystems, only the metadata of the
//!    files are used.
//! 2. **Reconciliation**: The two [`VfsUpdateList`] are compared and merged, using the
//!    [`ConcreteFS`] of A and B when needed. This step will classify the updates in 3 categories:
//!      - Updates that should be applied (on A if they come from B and vice-versa):
//!        [`ApplicableUpdate`]
//!      - Updates that should be ignored, when the exact same modification has been performed on
//!        both filesystems outside of the synchronization process
//!      - Conflicts, when different modifications have been detected on the same node. Conflicts
//!        arise if the node or one of its descendant has been modified on both filesystem, but the
//!        associated updates do not match. The simplest conflict example is when files are modified
//!        differently on both sides. Another example is a directory that is removed on A, whereas a
//!        file inside this directory has been modified on B.
//! 3. **Application**: Updates are applied to both the [`ConcreteFS`] and the [`VFS`]. Applying the
//!    update to the `ConcreteFS` creates an [`AppliedUpdate`] that can be propagated to the `VFS`.
//!
//! [`VFS`]: crate::vfs::Vfs

use std::cmp::Ordering;

use futures::future::try_join_all;

use crate::{
    concrete::{concrete_eq_file, ConcreteFS, ConcreteFsError},
    filesystem::FileSystem,
    sorted_vec::{Sortable, SortedVec},
    vfs::{DeleteNodeError, DirTree, InvalidPathError, Vfs, VirtualPath, VirtualPathBuf},
    Error, NameMismatchError,
};

/// Error encountered during a diff operation
#[derive(Error, Debug)]
pub enum DiffError {
    #[error("the sync info for the path {0:?} are not valid")]
    InvalidSyncInfo(VirtualPathBuf),
    #[error("the nodes in 'self' and 'other' do not point to the same node and can't be diff")]
    NodeMismatch(#[from] NameMismatchError),
}

/// Error encountered while applying an update to a VFS
#[derive(Error, Debug)]
pub enum VfsUpdateApplicationError {
    #[error("the path provided in the update is invalid")]
    InvalidPath(#[from] InvalidPathError),
    #[error("the name of the created dir does not match the path of the update")]
    NameMismatch(#[from] NameMismatchError),
    #[error("the file at {0:?} already exists")]
    FileExists(VirtualPathBuf),
    #[error("the dir at {0:?} alread exists")]
    DirExists(VirtualPathBuf),
    #[error("cannot apply an update to the root dir itself")]
    PathIsRoot,
    #[error("failed to delete a node")]
    DeleteError(#[from] DeleteNodeError),
}

#[derive(Error, Debug)]
pub enum ReconciliationError {
    #[error("error from the concrete fs during reconciliation")]
    ConcreteFsError(#[from] ConcreteFsError),
    #[error(
        "the nodes in 'self' and 'other' do not point to the same node and can't be reconciled"
    )]
    NodeMismatch(#[from] NameMismatchError),
    #[error("invalid path provided for reconciliation")]
    InvalidPath(#[from] InvalidPathError),
    #[error("failed to diff vfs nodes")]
    DiffError(#[from] DiffError),
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

impl<SyncInfo: IsModified<SyncInfo>> IsModified<Self> for Option<SyncInfo> {
    fn modification_state(&self, reference: &Self) -> ModificationState {
        match (self, reference) {
            (Some(valid_self), Some(valid_other)) => valid_self.modification_state(valid_other),
            // If at least one of the node lacks SyncInfo, we need to re-check with the concrete FS.
            (Some(_), None) | (None, None) | (None, Some(_)) => ModificationState::Modified,
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

    /// Reconcile updates from two different VFS.
    ///
    /// This step is performed without access to the concrete filesystems. This means that only
    /// obvious conflicts are detected. Files that are modified on both fs will be flagged as
    /// `NeedConcreteCheck` so they can be resolved later with concrete fs access.
    fn reconcile<SyncInfo, OtherSyncInfo>(
        &self,
        other: &VfsNodeUpdate,
        vfs_self: &Vfs<SyncInfo>,
        vfs_other: &Vfs<OtherSyncInfo>,
    ) -> Result<SortedVec<VirtualReconciledUpdate>, ReconciliationError> {
        if self.path() != other.path() {
            return Err(NameMismatchError {
                found: self.path().name().to_string(),
                expected: other.path().name().to_string(),
            }
            .into());
        }

        let mut reconciled = SortedVec::new();

        match (self, other) {
            (VfsNodeUpdate::DirCreated(pself), VfsNodeUpdate::DirCreated(pother)) => {
                // If the same dir has been created on both sides, we need to check if they are
                // equivalent. If they are not, we generate updates that are only allowed to create
                // nodes.
                let dir_self = vfs_self.root().find_dir(pself)?;
                let dir_other = vfs_other.root().find_dir(pother)?;

                let reconciled = dir_self.reconciliation_diff(
                    dir_other,
                    self.path().parent().unwrap_or(VirtualPath::root()),
                )?;
                // Since we iterate on sorted updates, the result will be sorted too
                Ok(reconciled)
            }
            (VfsNodeUpdate::DirRemoved(_), VfsNodeUpdate::DirRemoved(_)) => Ok(reconciled),
            (VfsNodeUpdate::FileModified(pself), VfsNodeUpdate::FileModified(pother))
            | (VfsNodeUpdate::FileCreated(pself), VfsNodeUpdate::FileCreated(pother)) => {
                let file_self = vfs_self.root().find_file(pself)?;
                let file_other = vfs_other.root().find_file(pother)?;

                if file_self.size() == file_other.size() {
                    reconciled.insert(VirtualReconciledUpdate::NeedConcreteCheck(pself.to_owned()));
                } else {
                    reconciled.insert(VirtualReconciledUpdate::Conflict(self.path().to_owned()));
                }

                Ok(reconciled)
            }
            (VfsNodeUpdate::FileRemoved(_), VfsNodeUpdate::FileRemoved(_)) => Ok(reconciled),
            _ => {
                reconciled.insert(VirtualReconciledUpdate::Conflict(self.path().to_owned()));
                Ok(reconciled)
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

/// Sorted list of [`VfsNodeUpdate`]
pub type VfsUpdateList = SortedVec<VfsNodeUpdate>;

impl VfsUpdateList {
    /// Merge two update lists by calling [`VfsNodeUpdate::reconcile`] on their elements one by
    /// one
    fn merge<SyncInfo, OtherSyncInfo>(
        &self,
        other: VfsUpdateList,
        vfs_self: &Vfs<SyncInfo>,
        vfs_other: &Vfs<OtherSyncInfo>,
    ) -> Result<SortedVec<VirtualReconciledUpdate>, ReconciliationError> {
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
                    ret.push(VirtualReconciledUpdate::applicable_other(self_item));
                    self_item_opt = self_iter.next()
                }

                Ordering::Equal => {
                    ret.extend(self_item.reconcile(other_item, vfs_self, vfs_other)?);
                    self_item_opt = self_iter.next();
                    other_item_opt = other_iter.next();
                }
                Ordering::Greater => {
                    // Propagate the update from other to self
                    ret.push(VirtualReconciledUpdate::applicable_self(other_item));
                    other_item_opt = other_iter.next();
                }
            }
        }

        // Handle the remaining items that are present in an iterator and not the
        // other one
        while let Some(self_item) = self_item_opt {
            ret.push(VirtualReconciledUpdate::applicable_other(self_item));
            self_item_opt = self_iter.next();
        }

        while let Some(other_item) = other_item_opt {
            ret.push(VirtualReconciledUpdate::applicable_self(other_item));
            other_item_opt = other_iter.next();
        }

        // Ok to use unchecked since we iterate on ordered updates
        Ok(SortedVec::unchecked_from_vec(ret))
    }

    /// Reconcile two updates lists.
    ///
    /// This is done in two steps:
    /// - First reconcile individual elements by comparing them between one list and the other
    /// - Then find conflicts with a directory and one of its elements
    pub async fn reconcile<Concrete: ConcreteFS, OtherConcrete: ConcreteFS>(
        &self,
        other: VfsUpdateList,
        fs_self: &FileSystem<Concrete>,
        fs_other: &FileSystem<OtherConcrete>,
    ) -> Result<SortedVec<ReconciledUpdate>, ReconciliationError> {
        let merged = self.merge(other, fs_self.vfs(), fs_other.vfs())?;

        let concrete_merged = merged
            .resolve_concrete(fs_self.concrete(), fs_other.concrete())
            .await?;

        Ok(concrete_merged.resolve_ancestor_conflicts())
    }
}

/// The target of the update, from the point of view of the FileSystem on which `reconcile` has been
/// called
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum UpdateTarget {
    SelfFs,
    OtherFs,
}

/// An update that is ready to be applied
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicableUpdate {
    target: UpdateTarget,
    update: VfsNodeUpdate,
}

impl ApplicableUpdate {
    pub fn new(target: UpdateTarget, update: &VfsNodeUpdate) -> Self {
        Self {
            target,
            update: update.clone(),
        }
    }

    pub fn target(&self) -> UpdateTarget {
        self.target
    }
}

impl From<ApplicableUpdate> for VfsNodeUpdate {
    fn from(value: ApplicableUpdate) -> Self {
        value.update
    }
}

/// An update that has been reconciled based on VFS diff but needs a check against the concrete FS
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum VirtualReconciledUpdate {
    Applicable(ApplicableUpdate),
    /// Updates in this state cannot be directly applied and need to be checked with concrete file
    /// access. The outcome of the check can be:
    /// - Remove the update if both files are identical
    /// - Mark the update as a conflict if they are different
    NeedConcreteCheck(VirtualPathBuf),
    Conflict(VirtualPathBuf),
}

impl VirtualReconciledUpdate {
    fn path(&self) -> &VirtualPath {
        match self {
            Self::Applicable(update) => update.update.path(),
            Self::NeedConcreteCheck(path) => path.as_ref(),
            Self::Conflict(path) => path.as_ref(),
        }
    }

    /// Create a new `Applicable` update with [`UpdateTarget::SelfFs`]
    pub(crate) fn applicable_self(update: &VfsNodeUpdate) -> Self {
        Self::Applicable(ApplicableUpdate::new(UpdateTarget::SelfFs, update))
    }

    /// Create a new `Applicable` update with [`UpdateTarget::OtherFs`]
    pub(crate) fn applicable_other(update: &VfsNodeUpdate) -> Self {
        Self::Applicable(ApplicableUpdate::new(UpdateTarget::OtherFs, update))
    }

    /// Resolve an update with `NeedConcreteCheck` by comparing hashes of the file on both concrete
    /// FS
    async fn resolve_concrete<Concrete: ConcreteFS, OtherConcrete: ConcreteFS>(
        self,
        concrete_self: &Concrete,
        concrete_other: &OtherConcrete,
    ) -> Result<Option<ReconciledUpdate>, ReconciliationError> {
        match self {
            VirtualReconciledUpdate::Applicable(update) => {
                Ok(Some(ReconciledUpdate::Applicable(update)))
            }
            VirtualReconciledUpdate::NeedConcreteCheck(path) => {
                if concrete_eq_file(concrete_self, concrete_other, &path).await? {
                    Ok(None)
                } else {
                    Ok(Some(ReconciledUpdate::Conflict(path)))
                }
            }
            VirtualReconciledUpdate::Conflict(conflict) => {
                Ok(Some(ReconciledUpdate::Conflict(conflict)))
            }
        }
    }
}

impl Sortable for VirtualReconciledUpdate {
    type Key = VirtualPath;

    fn key(&self) -> &Self::Key {
        self.path()
    }
}

impl SortedVec<VirtualReconciledUpdate> {
    /// Convert a list of [`VirtualReconciledUpdate`] into a list of [`ReconciledUpdate`] by using
    /// the concrete FS to resolve all the `NeedConcreteCheck`
    async fn resolve_concrete<Concrete: ConcreteFS, OtherConcrete: ConcreteFS>(
        self,
        concrete_self: &Concrete,
        concrete_other: &OtherConcrete,
    ) -> Result<SortedVec<ReconciledUpdate>, ReconciliationError> {
        let updates = try_join_all(
            self.into_iter()
                .map(|update| update.resolve_concrete(concrete_self, concrete_other)),
        )
        .await?;

        let updates = updates.into_iter().flatten().collect();

        Ok(SortedVec::unchecked_from_vec(updates))
    }
}

/// Output of the update reconciliation process. See [`VfsUpdateList::reconcile`]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconciledUpdate {
    /// Updates in this state can be applied to the target concrete FS
    Applicable(ApplicableUpdate),
    /// There is a conflict that needs user input for resolution
    Conflict(VirtualPathBuf),
}

impl ReconciledUpdate {
    pub fn path(&self) -> &VirtualPath {
        match self {
            ReconciledUpdate::Applicable(update) => update.update.path(),
            ReconciledUpdate::Conflict(path) => path.as_ref(),
        }
    }

    /// Create a new `Applicable` update with [`UpdateTarget::SelfFs`]
    pub fn applicable_self(update: &VfsNodeUpdate) -> Self {
        Self::Applicable(ApplicableUpdate::new(UpdateTarget::SelfFs, update))
    }

    /// Create a new `Applicable` update with [`UpdateTarget::OtherFs`]
    pub fn applicable_other(update: &VfsNodeUpdate) -> Self {
        Self::Applicable(ApplicableUpdate::new(UpdateTarget::OtherFs, update))
    }
}

impl Sortable for ReconciledUpdate {
    type Key = VirtualPath;

    fn key(&self) -> &Self::Key {
        self.path()
    }
}

impl SortedVec<ReconciledUpdate> {
    /// Find conflicts between updates on an element and a directory on its path.
    ///
    /// For example, `DirRemoved("/a/b")` and `FileModified("/a/b/c/d")` will generate conflicts on
    /// both `/a/b` and `/a/b/c/d`.
    fn resolve_ancestor_conflicts(self) -> Self {
        let mut resolved = Vec::new();
        let mut iter = self.into_iter().peekable();

        while let Some(mut update) = iter.next() {
            let mut conflict = false;
            let mut updates = Vec::new();
            while let Some(mut next_update) =
                iter.next_if(|next_update| next_update.path().is_inside(update.path()))
            {
                conflict = true;
                next_update = ReconciledUpdate::Conflict(next_update.path().to_owned());
                updates.push(next_update);
            }

            if conflict {
                update = ReconciledUpdate::Conflict(update.path().to_owned())
            }

            resolved.push(update);
            resolved.extend(updates);
        }

        SortedVec::unchecked_from_vec(resolved)
    }
}

/// Represent a DirCreation update that has been successfully applied on the [`ConcreteFS`].
#[derive(Clone, Debug)]
pub struct AppliedDirCreation<SyncInfo> {
    path: VirtualPathBuf,
    dir: DirTree<SyncInfo>,
}

impl<SyncInfo> AppliedDirCreation<SyncInfo> {
    /// Create a new `AppliedDirCreation`.
    ///
    /// Return an error if the name of the dir does not match the last element of the path
    pub fn new(path: &VirtualPath, dir: DirTree<SyncInfo>) -> Result<Self, NameMismatchError> {
        if path.name() != dir.name() {
            return Err(NameMismatchError {
                found: path.name().to_string(),
                expected: dir.name().to_string(),
            });
        }
        Ok(Self {
            path: path.to_owned(),
            dir,
        })
    }

    pub fn path(&self) -> &VirtualPath {
        &self.path
    }
}

impl<SyncInfo> From<AppliedDirCreation<SyncInfo>> for DirTree<SyncInfo> {
    fn from(value: AppliedDirCreation<SyncInfo>) -> Self {
        value.dir
    }
}

/// Represent a File creation or modification update that has been successfully applied on the
/// [`ConcreteFS`].
#[derive(Clone, Debug)]
pub struct AppliedFileUpdate<SyncInfo> {
    path: VirtualPathBuf,
    file_info: SyncInfo,
    file_size: u64,
}

impl<SyncInfo> AppliedFileUpdate<SyncInfo> {
    pub fn new(path: &VirtualPath, file_size: u64, file_info: SyncInfo) -> Self {
        Self {
            path: path.to_owned(),
            file_size,
            file_info,
        }
    }

    pub fn path(&self) -> &VirtualPath {
        &self.path
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn into_sync_info(self) -> SyncInfo {
        self.file_info
    }
}

/// An update that has been applied on a [`ConcreteFS`], and will be propagated to the [`Vfs`].
///
/// For node creation or modification, this types holds the SyncInfo needed to apply the update on
/// the `Vfs`. These SyncInfo must be fetched from the `ConcreteFS` immediatly after the update
/// application for correctness.
///
/// [`Vfs`]: crate::vfs::Vfs
#[derive(Clone, Debug)]
pub enum AppliedUpdate<SyncInfo> {
    DirCreated(AppliedDirCreation<SyncInfo>),
    DirRemoved(VirtualPathBuf),
    FileCreated(AppliedFileUpdate<SyncInfo>),
    FileModified(AppliedFileUpdate<SyncInfo>),
    FileRemoved(VirtualPathBuf),
    //TODO: detect moves
}

impl<SyncInfo> AppliedUpdate<SyncInfo> {
    pub fn path(&self) -> &VirtualPath {
        match self {
            AppliedUpdate::DirCreated(update) => update.path(),
            AppliedUpdate::FileCreated(update) | AppliedUpdate::FileModified(update) => {
                update.path()
            }
            AppliedUpdate::DirRemoved(path) | AppliedUpdate::FileRemoved(path) => path,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::{
        TestFileSystem,
        TestNode::{D, FF},
    };

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
        let mut local_fs = TestFileSystem::new(local_base.into());
        local_fs.update_vfs().await.unwrap();

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );

        let mut remote_fs = TestFileSystem::new(remote_base.into());
        remote_fs.update_vfs().await.unwrap();

        let local_diff = SortedVec::from([
            VfsNodeUpdate::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodeUpdate::DirRemoved(VirtualPathBuf::new("/a/b").unwrap()),
            VfsNodeUpdate::DirCreated(VirtualPathBuf::new("/e").unwrap()),
        ]);

        let remote_diff = local_diff.clone();

        let reconciled = local_diff
            .reconcile(remote_diff, &local_fs, &remote_fs)
            .await
            .unwrap();

        assert!(reconciled.is_empty());
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
        let mut local_fs = TestFileSystem::new(local_base.into());
        local_fs.update_vfs().await.unwrap();

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );
        let mut remote_fs = TestFileSystem::new(remote_base.into());
        remote_fs.update_vfs().await.unwrap();

        let local_diff = SortedVec::from([
            VfsNodeUpdate::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodeUpdate::DirCreated(VirtualPathBuf::new("/e").unwrap()),
        ]);

        let remote_diff = SortedVec::from([VfsNodeUpdate::DirRemoved(
            VirtualPathBuf::new("/a/b").unwrap(),
        )]);

        let reconciled = local_diff
            .reconcile(remote_diff, &local_fs, &remote_fs)
            .await
            .unwrap();

        let reconciled_ref = SortedVec::from([
            ReconciledUpdate::applicable_other(&VfsNodeUpdate::FileModified(
                VirtualPathBuf::new("/Doc/f1.md").unwrap(),
            )),
            ReconciledUpdate::applicable_self(&VfsNodeUpdate::DirRemoved(
                VirtualPathBuf::new("/a/b").unwrap(),
            )),
            ReconciledUpdate::applicable_other(&VfsNodeUpdate::DirCreated(
                VirtualPathBuf::new("/e").unwrap(),
            )),
        ]);

        assert_eq!(reconciled, reconciled_ref);
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
        let mut local_fs = TestFileSystem::new(local_base.into());
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
        let mut remote_fs = TestFileSystem::new(remote_base.into());
        remote_fs.update_vfs().await.unwrap();

        let local_diff = SortedVec::from([
            VfsNodeUpdate::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodeUpdate::DirRemoved(VirtualPathBuf::new("/a/b").unwrap()),
        ]);

        let remote_diff = SortedVec::from([
            VfsNodeUpdate::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodeUpdate::FileCreated(VirtualPathBuf::new("/a/b/test.log").unwrap()),
        ]);

        let reconciled = local_diff
            .reconcile(remote_diff, &local_fs, &remote_fs)
            .await
            .unwrap();

        let reconciled_ref = SortedVec::from([
            ReconciledUpdate::Conflict(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            ReconciledUpdate::Conflict(VirtualPathBuf::new("/a/b").unwrap()),
            ReconciledUpdate::Conflict(VirtualPathBuf::new("/a/b/test.log").unwrap()),
        ]);

        assert_eq!(reconciled, reconciled_ref);
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
        let mut local_fs = TestFileSystem::new(local_base.into());
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
        let mut remote_fs = TestFileSystem::new(remote_base.into());
        remote_fs.update_vfs().await.unwrap();

        let local_diff =
            SortedVec::from([VfsNodeUpdate::DirCreated(VirtualPathBuf::new("/").unwrap())]);

        let remote_diff = local_diff.clone();

        let reconciled = local_diff
            .reconcile(remote_diff, &local_fs, &remote_fs)
            .await
            .unwrap();

        let reconciled_ref = SortedVec::from([
            ReconciledUpdate::Conflict(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            ReconciledUpdate::applicable_self(&VfsNodeUpdate::FileCreated(
                VirtualPathBuf::new("/a/b/test.log").unwrap(),
            )),
            ReconciledUpdate::applicable_other(&VfsNodeUpdate::DirCreated(
                VirtualPathBuf::new("/e/g").unwrap(),
            )),
        ]);

        assert_eq!(reconciled, reconciled_ref);
    }
}
