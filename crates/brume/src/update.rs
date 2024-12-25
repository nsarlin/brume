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
    concrete::{concrete_eq_file, ConcreteFS, ConcreteFsError, Named},
    sorted_vec::{Sortable, SortedVec},
    vfs::{
        DeleteNodeError, DirTree, InvalidPathError, NodeState, Vfs, VirtualPath, VirtualPathBuf,
    },
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
    #[error("error from {fs_name} during reconciliation")]
    ConcreteFsError {
        fs_name: String,
        source: ConcreteFsError,
    },
    #[error(
        "the nodes in 'self' and 'other' do not point to the same node and can't be reconciled"
    )]
    NodeMismatch(#[from] NameMismatchError),
    #[error("invalid path on {fs_name} provided for reconciliation")]
    InvalidPath {
        fs_name: String,
        source: InvalidPathError,
    },
    #[error("failed to diff vfs nodes")]
    DiffError(#[from] DiffError),
}

impl ReconciliationError {
    pub fn concrete<E: Into<ConcreteFsError>>(fs_name: &str, source: E) -> Self {
        Self::ConcreteFsError {
            fs_name: fs_name.to_string(),
            source: source.into(),
        }
    }

    pub fn invalid_path<E: Into<InvalidPathError>>(fs_name: &str, source: E) -> Self {
        Self::InvalidPath {
            fs_name: fs_name.to_string(),
            source: source.into(),
        }
    }
}

/// Result of a VFS node comparison.
#[derive(Debug)]
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
pub trait IsModified {
    /// Tell if a node have been modified, and if possible also recusively answers for its children
    fn modification_state(&self, reference: &Self) -> ModificationState;

    /// Return a boolean telling if the node itself have been modified
    fn is_modified(&self, reference: &Self) -> bool {
        match self.modification_state(reference) {
            ModificationState::ShallowUnmodified => false,
            ModificationState::RecursiveUnmodified => false,
            ModificationState::Modified => true,
        }
    }
}

impl<SyncInfo: IsModified> IsModified for NodeState<SyncInfo> {
    fn modification_state(&self, reference: &Self) -> ModificationState {
        match (self, reference) {
            (NodeState::Ok(self_sync), NodeState::Ok(other_sync)) => {
                self_sync.modification_state(other_sync)
            }
            // Skip nodes in conflict until the conflicts are resolved
            (NodeState::Conflict, _) | (_, NodeState::Conflict) => {
                ModificationState::ShallowUnmodified
            }
            // If at least one node is in error or wants a resync, we return modified
            _ => ModificationState::Modified,
        }
    }
}

/// The kinds of fs modifications that can be represented in an update
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum UpdateKind {
    DirCreated,
    DirRemoved,
    FileCreated,
    FileModified,
    FileRemoved,
    //TODO: detect moves
}

impl UpdateKind {
    /// The update removes a node (dir or file)
    pub fn is_removal(self) -> bool {
        matches!(self, Self::DirRemoved | Self::FileRemoved)
    }

    /// The update creates a node (dir or file)
    pub fn is_creation(self) -> bool {
        matches!(self, Self::DirCreated | Self::FileCreated)
    }
}

/// A single node update
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub struct VfsNodeUpdate {
    path: VirtualPathBuf,
    kind: UpdateKind,
}

impl VfsNodeUpdate {
    pub fn dir_created(path: VirtualPathBuf) -> Self {
        Self {
            path,
            kind: UpdateKind::DirCreated,
        }
    }

    pub fn dir_removed(path: VirtualPathBuf) -> Self {
        Self {
            path,
            kind: UpdateKind::DirRemoved,
        }
    }

    pub fn file_created(path: VirtualPathBuf) -> Self {
        Self {
            path,
            kind: UpdateKind::FileCreated,
        }
    }

    pub fn file_modified(path: VirtualPathBuf) -> Self {
        Self {
            path,
            kind: UpdateKind::FileModified,
        }
    }

    pub fn file_removed(path: VirtualPathBuf) -> Self {
        Self {
            path,
            kind: UpdateKind::FileRemoved,
        }
    }

    pub fn path(&self) -> &VirtualPath {
        &self.path
    }

    pub fn kind(&self) -> UpdateKind {
        self.kind
    }

    /// Reconcile updates from two different VFS.
    ///
    /// This step is performed without access to the concrete filesystems. This means that only
    /// obvious conflicts are detected. Files that are modified on both fs will be flagged as
    /// `NeedConcreteCheck` so they can be resolved later with concrete fs access.
    fn reconcile<SyncInfo: Named, OtherSyncInfo: Named>(
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

        match (self.kind, other.kind) {
            (UpdateKind::DirCreated, UpdateKind::DirCreated) => {
                // If the same dir has been created on both sides, we need to check if they are
                // equivalent. If they are not, we generate updates that are only allowed to create
                // nodes.
                let dir_self = vfs_self
                    .root()
                    .find_dir(&self.path)
                    .map_err(|e| ReconciliationError::invalid_path(SyncInfo::TYPE_NAME, e))?;
                let dir_other = vfs_other
                    .root()
                    .find_dir(&other.path)
                    .map_err(|e| ReconciliationError::invalid_path(OtherSyncInfo::TYPE_NAME, e))?;

                let reconciled = dir_self.reconciliation_diff(
                    dir_other,
                    self.path().parent().unwrap_or(VirtualPath::root()),
                )?;
                // Since we iterate on sorted updates, the result will be sorted too
                Ok(reconciled)
            }
            (UpdateKind::DirRemoved, UpdateKind::DirRemoved) => Ok(reconciled),
            (UpdateKind::FileModified, UpdateKind::FileModified)
            | (UpdateKind::FileCreated, UpdateKind::FileCreated) => {
                let file_self = vfs_self
                    .root()
                    .find_file(&self.path)
                    .map_err(|e| ReconciliationError::invalid_path(OtherSyncInfo::TYPE_NAME, e))?;
                let file_other = vfs_other
                    .root()
                    .find_file(&other.path)
                    .map_err(|e| ReconciliationError::invalid_path(OtherSyncInfo::TYPE_NAME, e))?;

                let (self_update, other_update) = if file_self.size() == file_other.size() {
                    VirtualReconciledUpdate::concrete_check_both(self)
                } else {
                    VirtualReconciledUpdate::conflict_both(self)
                };

                reconciled.insert(self_update);
                reconciled.insert(other_update);

                Ok(reconciled)
            }
            (UpdateKind::FileRemoved, UpdateKind::FileRemoved) => Ok(reconciled),
            (_, _) => {
                reconciled.insert(VirtualReconciledUpdate::conflict_self(self));
                reconciled.insert(VirtualReconciledUpdate::conflict_other(other));
                Ok(reconciled)
            }
        }
    }

    /// The update removes a node (dir or file)
    pub fn is_removal(&self) -> bool {
        self.kind.is_removal()
    }

    /// The update creates a node (dir or file)
    pub fn is_creation(&self) -> bool {
        self.kind.is_creation()
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
    pub(crate) fn merge<SyncInfo: Named, OtherSyncInfo: Named>(
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
}

/// The target of the update, from the point of view of the FileSystem on which `reconcile` has been
/// called
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum UpdateTarget {
    SelfFs,
    OtherFs,
}

/// An update that is ready to be applied
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ApplicableUpdate {
    update: VfsNodeUpdate, // Fields order is important for the ord implementation
    target: UpdateTarget,
}

impl ApplicableUpdate {
    pub fn new(target: UpdateTarget, update: &VfsNodeUpdate) -> Self {
        Self {
            target,
            update: update.clone(),
        }
    }

    /// The Filesystem targeted by the update "self" or "other based on the Filesystem where the
    /// diff has been performed
    pub fn target(&self) -> UpdateTarget {
        self.target
    }

    pub fn update(&self) -> &VfsNodeUpdate {
        &self.update
    }

    pub fn path(&self) -> &VirtualPath {
        self.update.path()
    }

    pub fn is_removal(&self) -> bool {
        self.update.is_removal()
    }

    /// Return a new update with inverted [`UpdateTarget`]
    pub fn inverse_target(&self) -> Self {
        Self {
            update: self.update.clone(),
            target: match self.target {
                UpdateTarget::SelfFs => UpdateTarget::OtherFs,
                UpdateTarget::OtherFs => UpdateTarget::SelfFs,
            },
        }
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
    NeedConcreteCheck(ApplicableUpdate),
    Conflict(ApplicableUpdate),
}

impl VirtualReconciledUpdate {
    fn update(&self) -> &ApplicableUpdate {
        match self {
            VirtualReconciledUpdate::Applicable(update) => update,
            VirtualReconciledUpdate::NeedConcreteCheck(update) => update,
            VirtualReconciledUpdate::Conflict(update) => update,
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

    /// Create a new `Conflict` update with [`UpdateTarget::SelfFs`]
    pub(crate) fn conflict_self(update: &VfsNodeUpdate) -> Self {
        Self::Conflict(ApplicableUpdate::new(UpdateTarget::SelfFs, update))
    }

    /// Create a new `Conflict` update with [`UpdateTarget::OtherFs`]
    pub(crate) fn conflict_other(update: &VfsNodeUpdate) -> Self {
        Self::Conflict(ApplicableUpdate::new(UpdateTarget::OtherFs, update))
    }

    /// Create two new `Conflict` updates with [`UpdateTarget::SelfFs`] and
    /// [`UpdateTarget::OtherFs`]
    pub(crate) fn conflict_both(update: &VfsNodeUpdate) -> (Self, Self) {
        (Self::conflict_self(update), Self::conflict_other(update))
    }

    /// Create a new `NeedConcreteCheck` update with [`UpdateTarget::SelfFs`]
    pub(crate) fn concrete_check_self(update: &VfsNodeUpdate) -> Self {
        Self::NeedConcreteCheck(ApplicableUpdate::new(UpdateTarget::SelfFs, update))
    }

    /// Create a new `NeedConcreteCheck` update with [`UpdateTarget::OtherFs`]
    pub(crate) fn concrete_check_other(update: &VfsNodeUpdate) -> Self {
        Self::NeedConcreteCheck(ApplicableUpdate::new(UpdateTarget::OtherFs, update))
    }

    /// Create two new `NeedConcreteCheck` updates with [`UpdateTarget::SelfFs`] and
    /// [`UpdateTarget::OtherFs`]
    pub(crate) fn concrete_check_both(update: &VfsNodeUpdate) -> (Self, Self) {
        (
            Self::concrete_check_self(update),
            Self::concrete_check_other(update),
        )
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
            VirtualReconciledUpdate::NeedConcreteCheck(update) => {
                if concrete_eq_file(concrete_self, concrete_other, update.path())
                    .await
                    .map_err(|(e, name)| ReconciliationError::concrete(name, e))?
                {
                    Ok(None)
                } else {
                    Ok(Some(ReconciledUpdate::Conflict(update)))
                }
            }
            VirtualReconciledUpdate::Conflict(conflict) => {
                Ok(Some(ReconciledUpdate::Conflict(conflict)))
            }
        }
    }
}

impl Sortable for VirtualReconciledUpdate {
    type Key = ApplicableUpdate;

    fn key(&self) -> &Self::Key {
        self.update()
    }
}

impl SortedVec<VirtualReconciledUpdate> {
    /// Convert a list of [`VirtualReconciledUpdate`] into a list of [`ReconciledUpdate`] by using
    /// the concrete FS to resolve all the `NeedConcreteCheck`
    pub(crate) async fn resolve_concrete<Concrete: ConcreteFS, OtherConcrete: ConcreteFS>(
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

/// Output of the update reconciliation process. See [`reconcile`]
///
/// [`reconcile`]: crate::synchro::Synchro::reconcile
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconciledUpdate {
    /// Updates in this state can be applied to the target concrete FS
    Applicable(ApplicableUpdate),
    /// There is a conflict that needs user input for resolution before the update can be applied
    Conflict(ApplicableUpdate),
}

impl ReconciledUpdate {
    pub fn path(&self) -> &VirtualPath {
        match self {
            ReconciledUpdate::Applicable(update) => update.path(),
            ReconciledUpdate::Conflict(update) => update.path(),
        }
    }

    pub fn update(&self) -> &ApplicableUpdate {
        match self {
            ReconciledUpdate::Applicable(update) => update,
            ReconciledUpdate::Conflict(update) => update,
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

    /// Create a new `Conflict` update with [`UpdateTarget::SelfFs`]
    pub fn conflict_self(update: &VfsNodeUpdate) -> Self {
        Self::Conflict(ApplicableUpdate::new(UpdateTarget::SelfFs, update))
    }

    /// Create a new `Conflict` update with [`UpdateTarget::OtherFs`]
    pub fn conflict_other(update: &VfsNodeUpdate) -> Self {
        Self::Conflict(ApplicableUpdate::new(UpdateTarget::OtherFs, update))
    }

    /// Create two new `Conflict` updates with [`UpdateTarget::SelfFs`] and
    /// [`UpdateTarget::OtherFs`]
    pub fn conflict_both(update: &VfsNodeUpdate) -> (Self, Self) {
        (Self::conflict_self(update), Self::conflict_other(update))
    }
}

impl Sortable for ReconciledUpdate {
    type Key = ApplicableUpdate;

    fn key(&self) -> &Self::Key {
        self.update()
    }
}

impl SortedVec<ReconciledUpdate> {
    /// Find conflicts between updates on an element and a directory on its path.
    ///
    /// For example, `DirRemoved("/a/b")` and `FileModified("/a/b/c/d")` will generate conflicts on
    /// both `/a/b` and `/a/b/c/d`.
    pub(crate) fn resolve_ancestor_conflicts(self) -> Self {
        let mut resolved = Vec::new();
        let mut iter = self.into_iter().peekable();

        while let Some(mut parent_update) = iter.next() {
            // Skip if the path is already a conflict
            if matches!(parent_update, ReconciledUpdate::Conflict(_)) {
                resolved.push(parent_update);
                continue;
            }
            let mut conflict = false;
            let mut updates = Vec::new();
            while let Some(mut next_update) =
                iter.next_if(|next_update| next_update.path().is_inside(parent_update.path()))
            {
                conflict = true;
                // Swap the targets because we store the conflicts in the source filesystem
                next_update = ReconciledUpdate::Conflict(next_update.update().inverse_target());
                updates.push(next_update);
            }

            if conflict {
                parent_update = ReconciledUpdate::Conflict(parent_update.update().inverse_target())
            }

            resolved.push(parent_update);
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
    /// `parent_path` is the path to the parent of the created dir. Since an update cannot create a
    /// root dir, it always exist.
    pub fn new(parent_path: &VirtualPath, dir: DirTree<SyncInfo>) -> Self {
        let name = dir.name();
        let mut path = parent_path.to_owned();
        path.push(name);

        Self { path, dir }
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
    FailedApplication(FailedUpdateApplication),
    Conflict(VfsNodeUpdate),
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
            AppliedUpdate::FailedApplication(failed_update) => failed_update.path(),
            AppliedUpdate::Conflict(update) => update.path(),
        }
    }

    pub fn is_err(&self) -> bool {
        matches!(self, Self::FailedApplication(_))
    }
}

/// An update that failed because an error occured on one of the concrete fs
#[derive(Clone, Debug)]
pub struct FailedUpdateApplication {
    error: ConcreteFsError,
    update: VfsNodeUpdate,
}

impl FailedUpdateApplication {
    pub fn new(update: VfsNodeUpdate, error: ConcreteFsError) -> Self {
        Self { update, error }
    }

    pub fn path(&self) -> &VirtualPath {
        self.update.path()
    }

    pub fn update(&self) -> &VfsNodeUpdate {
        &self.update
    }

    pub fn error(&self) -> &ConcreteFsError {
        &self.error
    }
}
