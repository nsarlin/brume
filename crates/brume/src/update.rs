//! An update represents a difference between two file system nodes.
//!
//! For 2 filesystems A and B that are kept in sync, the lifecycle of an update comes in 3 steps:
//! 1. **VFS diff**: The [`VFS`] of each filesystem is loaded from the [`FSBackend`] and compared
//!    with its previous version. This will create two separated [`VfsDiffList`], one for A and one
//!    for B. The comparison does not access the concrete filesystems, only the metadata of the
//!    files are used.
//! 2. **Reconciliation**: The two [`VfsDiffList`] are compared and merged, using the [`FSBackend`]
//!    of A and B when needed. This step will classify the updates in 3 categories:
//!      - Updates that should be applied (on A if they come from B and vice-versa):
//!        [`ApplicableUpdate`]
//!      - Updates that should be ignored, when the exact same modification has been performed on
//!        both filesystems outside of the synchronization process
//!      - Conflicts, when different modifications have been detected on the same node. Conflicts
//!        arise if the node or one of its descendant has been modified on both filesystem, but the
//!        associated updates do not match. The simplest conflict example is when files are modified
//!        differently on both sides. Another example is a directory that is removed on A, whereas a
//!        file inside this directory has been modified on B.
//! 3. **Application**: Updates are applied to both the [`FSBackend`] and the [`VFS`]. Applying the
//!    update to the `FSBackend` creates an [`AppliedUpdate`] that can be propagated to the `VFS`.
//!
//! [`VFS`]: crate::vfs::Vfs
//! [`FSBackend`]: crate::concrete::FSBackend

use std::fmt::Display;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    Error, NameMismatchError,
    concrete::{ConcreteFileCloneResult, FsBackendError, Named, ToBytes},
    sorted_vec::{Sortable, SortedVec},
    vfs::{
        DeleteNodeError, FileInfo, InvalidPathError, StatefulDirTree, Vfs, VirtualPath,
        VirtualPathBuf,
    },
};

/// Error encountered during a diff operation
#[derive(Error, Debug)]
pub enum DiffError {
    #[error("the sync info for the path {0:?} are not valid")]
    InvalidSyncInfo(VirtualPathBuf),
    #[error("the path in 'local' and 'remote' FS do not point to the same node and can't be diff")]
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
    #[error("the dir at {0:?} already exists")]
    DirExists(VirtualPathBuf),
    #[error("cannot apply an update to the root dir itself")]
    PathIsRoot,
    #[error("failed to delete a node")]
    DeleteError(#[from] DeleteNodeError),
    #[error("the reference VFS is missing from the synchro")]
    MissingReferenceVfs,
}

#[derive(Error, Debug)]
pub enum ReconciliationError {
    #[error("error from {fs_name} during reconciliation")]
    FsBackendError {
        fs_name: String,
        source: FsBackendError,
    },
    #[error(
        "the nodes in 'local' and 'remote' FS do not point to the same node and can't be reconciled"
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
    pub fn concrete<E: Into<FsBackendError>>(fs_name: &str, source: E) -> Self {
        Self::FsBackendError {
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
/// allow different node comparison strategies (for example based on timestamp or a revision id
/// value). See [`NextcloudSyncInfo`] and [`LocalSyncInfo`] for examples.
///
/// [`LocalSyncInfo`]: crate::concrete::local::LocalSyncInfo
/// [`NextcloudSyncInfo`]: crate::concrete::nextcloud::NextcloudSyncInfo
pub trait IsModified {
    /// Tell if a node have been modified, and if possible also recursively answers for its children
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

/// The kinds of fs modifications that can be represented in an update
#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum UpdateKind {
    DirCreated,
    DirRemoved,
    FileCreated,
    FileRemoved,
    FileModified,
    //TODO: add DirModified to update the syncinfo of a dir
    //TODO: detect moves
}

impl Display for UpdateKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateKind::DirCreated => write!(f, "Directory created"),
            UpdateKind::DirRemoved => write!(f, "Directory removed"),
            UpdateKind::FileCreated => write!(f, "File created"),
            UpdateKind::FileRemoved => write!(f, "File removed"),
            UpdateKind::FileModified => write!(f, "File modified"),
        }
    }
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

    /// Return the "opposite" operation. For example, the inverse of a creation is a removal.
    pub fn inverse(self) -> Self {
        match self {
            UpdateKind::DirCreated => UpdateKind::DirRemoved,
            UpdateKind::DirRemoved => UpdateKind::DirCreated,
            UpdateKind::FileCreated => UpdateKind::FileRemoved,
            UpdateKind::FileModified => UpdateKind::FileModified,
            UpdateKind::FileRemoved => UpdateKind::FileCreated,
        }
    }
}

/// The target filesystem of an update, as defined in the synchro
#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum UpdateTarget {
    Local,
    Remote,
    Both,
}

impl UpdateTarget {
    pub fn invert(self) -> Self {
        match self {
            UpdateTarget::Local => UpdateTarget::Remote,
            UpdateTarget::Remote => UpdateTarget::Local,
            UpdateTarget::Both => UpdateTarget::Both,
        }
    }
}

/// A single diff on a vfs node, returned by [`Vfs::diff`]
///
/// [`Vfs::diff`]: crate::vfs::Vfs::diff
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VfsDiff {
    path: VirtualPathBuf, // Field order is important for the "Ord" impl
    kind: UpdateKind,
}

impl VfsDiff {
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

    /// Merge updates from two different VFS.
    ///
    /// This step is performed without access to the concrete filesystems. This means that only
    /// obvious conflicts are detected. Files that are modified on both fs will be flagged as
    /// `NeedBackendCheck` so they can be resolved later with concrete fs access.
    fn merge<LocalSyncInfo: Named, RemoteSyncInfo: Named>(
        &self,
        remote_update: &VfsDiff,
        vfs_local: &Vfs<LocalSyncInfo>,
        vfs_remote: &Vfs<RemoteSyncInfo>,
    ) -> Result<SortedVec<VirtualReconciledUpdate>, ReconciliationError> {
        if self.path() != remote_update.path() {
            return Err(NameMismatchError {
                found: self.path().name().to_string(),
                expected: remote_update.path().name().to_string(),
            }
            .into());
        }

        let mut reconciled = SortedVec::new();

        match (self.kind, remote_update.kind) {
            (UpdateKind::DirCreated, UpdateKind::DirCreated) => {
                // If the same dir has been created on both sides, we need to check if they are
                // equivalent. If they are not, we generate updates that are only allowed to create
                // nodes.
                let dir_local = vfs_local
                    .find_dir(&self.path)
                    .map_err(|e| ReconciliationError::invalid_path(LocalSyncInfo::TYPE_NAME, e))?;
                let dir_remote = vfs_remote
                    .find_dir(&remote_update.path)
                    .map_err(|e| ReconciliationError::invalid_path(RemoteSyncInfo::TYPE_NAME, e))?;

                let reconciled = dir_local.reconciliation_diff(
                    dir_remote,
                    self.path().parent().unwrap_or(VirtualPath::root()),
                );
                // Since we iterate on sorted updates, the result will be sorted too
                Ok(reconciled)
            }
            (UpdateKind::DirRemoved, UpdateKind::DirRemoved) => Ok(reconciled),
            (UpdateKind::FileModified, UpdateKind::FileModified)
            | (UpdateKind::FileCreated, UpdateKind::FileCreated) => {
                let file_local = vfs_local
                    .find_file(&self.path)
                    .map_err(|e| ReconciliationError::invalid_path(RemoteSyncInfo::TYPE_NAME, e))?;
                let file_remote = vfs_remote
                    .find_file(&remote_update.path)
                    .map_err(|e| ReconciliationError::invalid_path(RemoteSyncInfo::TYPE_NAME, e))?;

                let update = if file_local.size() == file_remote.size() {
                    VirtualReconciledUpdate::backend_check_both(self)
                } else {
                    VirtualReconciledUpdate::conflict_both(self)
                };

                reconciled.insert(update);

                Ok(reconciled)
            }
            (UpdateKind::FileRemoved, UpdateKind::FileRemoved) => Ok(reconciled),
            (_, _) => {
                reconciled.insert(VirtualReconciledUpdate::conflict_local(self));
                reconciled.insert(VirtualReconciledUpdate::conflict_remote(remote_update));
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

    pub fn invert(self) -> Self {
        let kind = self.kind.inverse();

        Self {
            path: self.path,
            kind,
        }
    }
}

impl Sortable for VfsDiff {
    type Key = VirtualPath;

    fn key(&self) -> &Self::Key {
        self.path()
    }
}

/// Sorted list of [`VfsDiff`]
pub type VfsDiffList = SortedVec<VfsDiff>;

impl VfsDiffList {
    /// Merge two update lists by calling [`VfsDiff::merge`] on their elements one by
    /// one
    pub(crate) fn merge<LocalSyncInfo: Named, RemoteSyncInfo: Named>(
        &self,
        remote_updates: VfsDiffList,
        local_vfs: &Vfs<LocalSyncInfo>,
        remote_vfs: &Vfs<RemoteSyncInfo>,
    ) -> Result<SortedVec<VirtualReconciledUpdate>, ReconciliationError> {
        let res = self
            .iter_zip_map(
                &remote_updates,
                |local_item| {
                    Ok(SortedVec::from_vec(vec![
                        VirtualReconciledUpdate::applicable_remote(local_item),
                    ]))
                },
                |local_item, remote_item| local_item.merge(remote_item, local_vfs, remote_vfs),
                |remote_item| {
                    Ok(SortedVec::from_vec(vec![
                        VirtualReconciledUpdate::applicable_local(remote_item),
                    ]))
                },
            )
            .collect::<Result<Vec<_>, _>>()?;

        // Updates that are only present on one target will be sorted because `iter_zip_map`
        // iterates in order. Updates present on both targets will also be sorted relatively because
        // reconcile returns a `SortedVec`
        Ok(SortedVec::unchecked_flatten(res))
    }
}

/// An update that is ready to be applied
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct ApplicableUpdate {
    update: VfsDiff, // Fields order is important for the ord implementation
    target: UpdateTarget,
}

impl ApplicableUpdate {
    pub fn new(target: UpdateTarget, update: &VfsDiff) -> Self {
        Self {
            target,
            update: update.clone(),
        }
    }

    /// The Filesystem targeted by the update, local or remote
    pub fn target(&self) -> UpdateTarget {
        self.target
    }

    pub fn update(&self) -> &VfsDiff {
        &self.update
    }

    pub fn path(&self) -> &VirtualPath {
        self.update.path()
    }

    pub fn is_removal(&self) -> bool {
        self.update.is_removal()
    }

    /// Return a new update with inverted [`UpdateTarget`]
    pub fn invert_target(&self) -> Self {
        Self {
            update: self.update.clone(),
            target: self.target.invert(),
        }
    }

    /// Return a new update that is the opposite of the current one.
    ///
    /// # Example
    /// ```
    /// use brume::update::*;
    /// use brume::vfs::VirtualPathBuf;
    ///
    /// let update = VfsDiff::dir_removed(VirtualPathBuf::new("/a/b").unwrap());
    /// let applicable = ApplicableUpdate::new(UpdateTarget::Local, &update);
    ///
    /// let inverse_up = VfsDiff::dir_created(VirtualPathBuf::new("/a/b").unwrap());
    /// let inverse_app = ApplicableUpdate::new(UpdateTarget::Remote, &inverse_up);
    ///
    /// assert_eq!(applicable.invert(), inverse_app);
    /// ```
    pub fn invert(self) -> Self {
        Self {
            update: self.update.invert(),
            target: self.target.invert(),
        }
    }
}

impl From<ApplicableUpdate> for VfsDiff {
    fn from(value: ApplicableUpdate) -> Self {
        value.update
    }
}

impl Sortable for ApplicableUpdate {
    type Key = Self;

    fn key(&self) -> &Self::Key {
        self
    }
}

impl SortedVec<ApplicableUpdate> {
    /// Split the list in two, with the updates for the local FS on one side and the updates for the
    /// remote FS on the other side.
    ///
    /// Updates with [`UpdateTarget::Both`] are duplicated in both lists.
    ///
    /// # Example
    /// ```
    /// use brume::sorted_vec::SortedVec;
    /// use brume::update::*;
    /// use brume::vfs::VirtualPathBuf;
    ///
    /// let mut list = SortedVec::new();
    ///
    /// let update_a = VfsDiff::dir_removed(VirtualPathBuf::new("/dir/a").unwrap());
    /// let applicable_a = ApplicableUpdate::new(UpdateTarget::Local, &update_a);
    /// list.insert(applicable_a);
    ///
    /// let update_b = VfsDiff::file_created(VirtualPathBuf::new("/dir/b").unwrap());
    /// let applicable_b = ApplicableUpdate::new(UpdateTarget::Remote, &update_b);
    /// list.insert(applicable_b);
    ///
    /// let update_c = VfsDiff::file_modified(VirtualPathBuf::new("/dir/c").unwrap());
    /// let applicable_c = ApplicableUpdate::new(UpdateTarget::Both, &update_c);
    /// list.insert(applicable_c);
    ///
    /// let (local, remote) = list.split_local_remote();
    /// assert_eq!(local.len(), 2);
    /// assert_eq!(remote.len(), 2);
    /// ```
    pub fn split_local_remote(self) -> (Vec<VfsDiff>, Vec<VfsDiff>) {
        let mut local = Vec::new();
        let mut remote = Vec::new();

        for update in self.into_iter() {
            match update.target() {
                UpdateTarget::Local => local.push(update.update),
                UpdateTarget::Remote => remote.push(update.update),
                UpdateTarget::Both => {
                    local.push(update.update.clone());
                    remote.push(update.update)
                }
            }
        }

        (local, remote)
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
    NeedBackendCheck(ApplicableUpdate),
    Conflict(ApplicableUpdate),
}

impl VirtualReconciledUpdate {
    fn update(&self) -> &ApplicableUpdate {
        match self {
            VirtualReconciledUpdate::Applicable(update) => update,
            VirtualReconciledUpdate::NeedBackendCheck(update) => update,
            VirtualReconciledUpdate::Conflict(update) => update,
        }
    }

    /// Create a new `Applicable` update with [`UpdateTarget::SelfFs`]
    pub(crate) fn applicable_local(update: &VfsDiff) -> Self {
        Self::Applicable(ApplicableUpdate::new(UpdateTarget::Local, update))
    }

    /// Create a new `Applicable` update with [`UpdateTarget::Remote`]
    pub(crate) fn applicable_remote(update: &VfsDiff) -> Self {
        Self::Applicable(ApplicableUpdate::new(UpdateTarget::Remote, update))
    }

    /// Create a new `Conflict` update with [`UpdateTarget::Local`]
    pub(crate) fn conflict_local(update: &VfsDiff) -> Self {
        Self::Conflict(ApplicableUpdate::new(UpdateTarget::Local, update))
    }

    /// Create a new `Conflict` update with [`UpdateTarget::Remote`]
    pub(crate) fn conflict_remote(update: &VfsDiff) -> Self {
        Self::Conflict(ApplicableUpdate::new(UpdateTarget::Remote, update))
    }

    /// Create a new `Conflict` updates with [`UpdateTarget::Both`]
    pub(crate) fn conflict_both(update: &VfsDiff) -> Self {
        Self::Conflict(ApplicableUpdate::new(UpdateTarget::Both, update))
    }

    /// Create a new `NeedBackendCheck` updates with [`UpdateTarget::Both`]
    pub(crate) fn backend_check_both(update: &VfsDiff) -> Self {
        Self::NeedBackendCheck(ApplicableUpdate::new(UpdateTarget::Both, update))
    }
}

impl Sortable for VirtualReconciledUpdate {
    type Key = ApplicableUpdate;

    fn key(&self) -> &Self::Key {
        self.update()
    }
}

/// A conflict between two updates
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UpdateConflict {
    /// The update that was supposed to be applied
    update: ApplicableUpdate,
    /// The path which caused the conflict on the other fs in the syncro
    otherfs_conflict_path: Vec<VirtualPathBuf>,
}

impl Sortable for UpdateConflict {
    type Key = ApplicableUpdate;

    fn key(&self) -> &Self::Key {
        self.update()
    }
}

impl UpdateConflict {
    pub fn new(update: ApplicableUpdate, otherfs_conflict_path: &[VirtualPathBuf]) -> Self {
        Self {
            update,
            otherfs_conflict_path: otherfs_conflict_path.to_vec(),
        }
    }

    /// Create a conflict that is caused by the node at the same path in the other filesystem
    pub fn new_same_path(update: ApplicableUpdate) -> Self {
        let otherfs_conflict_path = vec![update.path().to_owned()];
        Self {
            update,
            otherfs_conflict_path,
        }
    }

    pub fn path(&self) -> &VirtualPath {
        self.update.path()
    }

    pub fn update(&self) -> &ApplicableUpdate {
        &self.update
    }

    pub fn otherfs_conflict_path(&self) -> &[VirtualPathBuf] {
        &self.otherfs_conflict_path
    }

    /// The update removes a node (dir or file)
    pub fn is_removal(&self) -> bool {
        self.update.is_removal()
    }

    pub fn invert(self) -> Self {
        let update = self.update.invert();

        Self { update, ..self }
    }
}

impl SortedVec<UpdateConflict> {
    /// Split the list in two, with the updates for the local FS on one side and the updates for the
    /// remote FS on the other side.
    ///
    /// Updates with [`UpdateTarget::Both`] are duplicated in both lists.
    pub fn split_local_remote(self) -> (Vec<VfsConflict>, Vec<VfsConflict>) {
        let mut local = Vec::new();
        let mut remote = Vec::new();

        for conflict in self.into_iter() {
            match conflict.update().target() {
                UpdateTarget::Local => local.push(conflict.into()),
                UpdateTarget::Remote => remote.push(conflict.into()),
                UpdateTarget::Both => {
                    local.push(conflict.clone().into());
                    remote.push(conflict.into())
                }
            }
        }

        (local, remote)
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
    Conflict(UpdateConflict),
}

impl ReconciledUpdate {
    pub fn path(&self) -> &VirtualPath {
        match self {
            ReconciledUpdate::Applicable(update) => update.path(),
            ReconciledUpdate::Conflict(conflict) => conflict.path(),
        }
    }

    pub fn update(&self) -> &ApplicableUpdate {
        match self {
            ReconciledUpdate::Applicable(update) => update,
            ReconciledUpdate::Conflict(conflict) => conflict.update(),
        }
    }

    /// Create a new `Applicable` update with [`UpdateTarget::Local`]
    pub fn applicable_local(update: &VfsDiff) -> Self {
        Self::Applicable(ApplicableUpdate::new(UpdateTarget::Local, update))
    }

    /// Create a new `Applicable` update with [`UpdateTarget::Remote`]
    pub fn applicable_remote(update: &VfsDiff) -> Self {
        Self::Applicable(ApplicableUpdate::new(UpdateTarget::Remote, update))
    }

    /// Create a new `Conflict` update with [`UpdateTarget::Local`]
    pub fn conflict_local(update: &VfsDiff, otherfs_conflict_path: &[VirtualPathBuf]) -> Self {
        Self::Conflict(UpdateConflict::new(
            ApplicableUpdate::new(UpdateTarget::Local, update),
            otherfs_conflict_path,
        ))
    }

    /// Create a new `Conflict` update with [`UpdateTarget::Remote`]
    pub fn conflict_remote(update: &VfsDiff, otherfs_conflict_path: &[VirtualPathBuf]) -> Self {
        Self::Conflict(UpdateConflict::new(
            ApplicableUpdate::new(UpdateTarget::Remote, update),
            otherfs_conflict_path,
        ))
    }

    /// Create two new `Conflict` updates with [`UpdateTarget::Local`] and
    /// [`UpdateTarget::Remote`]
    pub fn conflict_both(update: &VfsDiff) -> Self {
        Self::Conflict(UpdateConflict::new_same_path(ApplicableUpdate::new(
            UpdateTarget::Both,
            update,
        )))
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
            match &parent_update {
                // If the update is a dir removal, check if one of its children is modified too
                ReconciledUpdate::Applicable(update) if update.is_removal() => {
                    let mut conflict = false;
                    let mut updates = Vec::new();

                    while let Some(mut next_update) = iter
                        .next_if(|next_update| next_update.path().is_inside(parent_update.path()))
                    {
                        conflict = true;
                        // Swap the targets because we store the conflicts in the source filesystem
                        next_update = ReconciledUpdate::Conflict(UpdateConflict::new(
                            next_update.update().invert_target(),
                            &[parent_update.path().to_owned()],
                        ));
                        updates.push(next_update);
                    }

                    if conflict {
                        parent_update = ReconciledUpdate::Conflict(UpdateConflict::new(
                            parent_update.update().invert_target(),
                            &updates
                                .iter()
                                .map(|update| update.path().to_owned())
                                .collect::<Vec<_>>(),
                        ))
                    }

                    resolved.push(parent_update);
                    resolved.extend(updates);
                }
                _ =>
                // Skip if the path is already a conflict or not a removal
                {
                    resolved.push(parent_update);
                    continue;
                }
            }
        }

        SortedVec::unchecked_from_vec(resolved)
    }
}

/// Represent a DirCreation update that has been successfully applied on the [`FSBackend`].
///
/// [`FSBackend`]: crate::concrete::FSBackend
#[derive(Clone, Debug)]
pub struct AppliedDirCreation<SrcSyncInfo, DstSyncInfo> {
    path: VirtualPathBuf,
    src_dir: StatefulDirTree<SrcSyncInfo>,
    dst_dir: StatefulDirTree<DstSyncInfo>,
}

impl<SrcSyncInfo, DstSyncInfo> AppliedDirCreation<SrcSyncInfo, DstSyncInfo> {
    /// Create a new `AppliedDirCreation`.
    ///
    /// `parent_path` is the path to the parent of the created dir. Since an update cannot create a
    /// root dir, it always exist.
    pub fn new(
        parent_path: &VirtualPath,
        src_dir: StatefulDirTree<SrcSyncInfo>,
        dst_dir: StatefulDirTree<DstSyncInfo>,
    ) -> Self {
        let name = src_dir.name();
        let mut path = parent_path.to_owned();
        path.push(name);

        Self {
            path,
            src_dir,
            dst_dir,
        }
    }
}

impl<SrcSyncInfo, DstSyncInfo> From<AppliedDirCreation<SrcSyncInfo, DstSyncInfo>>
    for (VfsDirCreation<SrcSyncInfo>, VfsDirCreation<DstSyncInfo>)
{
    fn from(value: AppliedDirCreation<SrcSyncInfo, DstSyncInfo>) -> Self {
        let src_update = VfsDirCreation {
            path: value.path.clone(),
            dir: value.src_dir,
        };

        let dst_update = VfsDirCreation {
            path: value.path,
            dir: value.dst_dir,
        };

        (src_update, dst_update)
    }
}

/// Represents a File creation or modification update that has been successfully applied on the
/// [`FSBackend`].
///
/// [`FSBackend`]: crate::concrete::FSBackend
#[derive(Clone, Debug)]
pub struct AppliedFileUpdate<SrcSyncInfo, DstSyncInfo> {
    path: VirtualPathBuf,
    src_file_info: FileInfo<SrcSyncInfo>,
    dst_file_info: FileInfo<DstSyncInfo>,
}

impl<SrcSyncInfo, DstSyncInfo> AppliedFileUpdate<SrcSyncInfo, DstSyncInfo> {
    pub fn new(
        path: &VirtualPath,
        src_file_info: FileInfo<SrcSyncInfo>,
        dst_file_info: FileInfo<DstSyncInfo>,
    ) -> Self {
        Self {
            path: path.to_owned(),

            src_file_info,
            dst_file_info,
        }
    }

    pub fn from_clone_result(
        path: &VirtualPath,
        clone_result: ConcreteFileCloneResult<SrcSyncInfo, DstSyncInfo>,
    ) -> Self {
        let (src_file_info, dst_file_info) = clone_result.into();
        Self::new(path, src_file_info, dst_file_info)
    }
}

impl<SrcSyncInfo, DstSyncInfo> From<AppliedFileUpdate<SrcSyncInfo, DstSyncInfo>>
    for (VfsFileUpdate<SrcSyncInfo>, VfsFileUpdate<DstSyncInfo>)
{
    fn from(value: AppliedFileUpdate<SrcSyncInfo, DstSyncInfo>) -> Self {
        let src_size = value.src_file_info.size();
        let src_modified = value.src_file_info.last_modified();
        let src_update = VfsFileUpdate {
            path: value.path.clone(),
            file_info: value.src_file_info.into_metadata(),
            file_size: src_size,
            last_modified: src_modified,
        };

        let dst_size = value.dst_file_info.size();
        let dst_modified = value.dst_file_info.last_modified();
        let dst_update = VfsFileUpdate {
            path: value.path.clone(),
            file_info: value.dst_file_info.into_metadata(),
            file_size: dst_size,
            last_modified: dst_modified,
        };

        (src_update, dst_update)
    }
}

/// An update that has been applied on a [`FSBackend`], and will be propagated to the [`Vfs`].
///
/// For node creation or modification, this types holds the SyncInfo needed to apply the update on
/// both `Vfs` the src and the dst one. The src Vfs is the one where the update originated as a
/// [`VfsDiff`] object. The dst Vfs is the one where the update has been propagated. These SyncInfo
/// must be fetched from the `FSBackend` immediately after the update application for correctness.
///
/// [`Vfs`]: crate::vfs::Vfs
/// [`FSBackend`]: crate::concrete::FSBackend
#[derive(Clone, Debug)]
pub enum AppliedUpdate<SrcSyncInfo, DstSyncInfo> {
    DirCreated(AppliedDirCreation<SrcSyncInfo, DstSyncInfo>),
    DirRemoved(VirtualPathBuf),
    /// A dir was to be removed, but the application has been skipped because it did not exist on
    /// dest
    DirRemovedSkipped(VirtualPathBuf),
    FileCreated(AppliedFileUpdate<SrcSyncInfo, DstSyncInfo>),
    FileModified(AppliedFileUpdate<SrcSyncInfo, DstSyncInfo>),
    FileRemoved(VirtualPathBuf),
    /// A file was to be removed, but the application has been skipped because it did not exist on
    /// dest
    FileRemovedSkipped(VirtualPathBuf),
    FailedApplication(FailedUpdateApplication),
    //TODO: detect moves
}

impl<SrcSyncInfo, DstSyncInfo> From<AppliedUpdate<SrcSyncInfo, DstSyncInfo>>
    for (VfsUpdate<SrcSyncInfo>, Option<VfsUpdate<DstSyncInfo>>)
{
    fn from(value: AppliedUpdate<SrcSyncInfo, DstSyncInfo>) -> Self {
        match value {
            AppliedUpdate::DirCreated(dir_creation) => {
                let (src_update, dst_update) = dir_creation.into();
                (
                    VfsUpdate::DirCreated(src_update),
                    Some(VfsUpdate::DirCreated(dst_update)),
                )
            }
            AppliedUpdate::DirRemoved(path) => (
                VfsUpdate::DirRemoved(path.clone()),
                Some(VfsUpdate::DirRemoved(path)),
            ),
            AppliedUpdate::DirRemovedSkipped(path) => (VfsUpdate::DirRemoved(path.clone()), None),
            AppliedUpdate::FileCreated(file_update) => {
                let (src_update, dst_update) = file_update.into();
                (
                    VfsUpdate::FileCreated(src_update),
                    Some(VfsUpdate::FileCreated(dst_update)),
                )
            }
            AppliedUpdate::FileModified(file_update) => {
                let (src_update, dst_update) = file_update.into();
                (
                    VfsUpdate::FileModified(src_update),
                    Some(VfsUpdate::FileModified(dst_update)),
                )
            }
            AppliedUpdate::FileRemoved(path) => (
                VfsUpdate::FileRemoved(path.clone()),
                Some(VfsUpdate::FileRemoved(path)),
            ),
            AppliedUpdate::FileRemovedSkipped(path) => (VfsUpdate::FileRemoved(path.clone()), None),
            AppliedUpdate::FailedApplication(failed_update) => {
                // Errors are applied on the source Vfs to make them trigger a resync, and not on
                // the dest Vfs because the node might not even exist
                (VfsUpdate::FailedApplication(failed_update.clone()), None)
            }
        }
    }
}

impl<SrcSyncInfo, DstSyncInfo> AppliedUpdate<SrcSyncInfo, DstSyncInfo> {
    pub fn path(&self) -> &VirtualPath {
        match self {
            AppliedUpdate::DirCreated(dir_creation) => &dir_creation.path,
            AppliedUpdate::DirRemoved(path) => path,
            AppliedUpdate::DirRemovedSkipped(path) => path,
            AppliedUpdate::FileCreated(file_update) => &file_update.path,
            AppliedUpdate::FileModified(file_update) => &file_update.path,
            AppliedUpdate::FileRemoved(path) => path,
            AppliedUpdate::FileRemovedSkipped(path) => path,
            AppliedUpdate::FailedApplication(failed_update) => failed_update.path(),
        }
    }
}

/// An update that failed because an error occurred on one of the concrete fs
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FailedUpdateApplication {
    error: String,
    update: VfsDiff,
}

impl FailedUpdateApplication {
    pub fn new<E: Display>(update: VfsDiff, error: E) -> Self {
        Self {
            update,
            error: format!("{error}"),
        }
    }

    pub fn path(&self) -> &VirtualPath {
        self.update.path()
    }

    pub fn update(&self) -> &VfsDiff {
        &self.update
    }

    pub fn error(&self) -> &str {
        &self.error
    }
}

/// Represents a DirCreation update that has been successfully applied on the [`FSBackend`] and can
/// be applied to the Vfs.
///
/// [`FSBackend`]: crate::concrete::FSBackend
#[derive(Clone, Debug)]
pub struct VfsDirCreation<SyncInfo> {
    path: VirtualPathBuf,
    dir: StatefulDirTree<SyncInfo>,
}

impl<SyncInfo> VfsDirCreation<SyncInfo> {
    /// Creates a new `VfsDirCreation`.
    ///
    /// `parent_path` is the path to the parent of the created dir. Since an update cannot create a
    /// root dir, it always exist.
    pub fn new(parent_path: &VirtualPath, dir: StatefulDirTree<SyncInfo>) -> Self {
        let name = dir.name();
        let mut path = parent_path.to_owned();
        path.push(name);

        Self {
            path: path.to_owned(),
            dir,
        }
    }

    pub fn path(&self) -> &VirtualPath {
        &self.path
    }

    pub fn dir(&self) -> &StatefulDirTree<SyncInfo> {
        &self.dir
    }
}

impl<SyncInfo> From<VfsDirCreation<SyncInfo>> for StatefulDirTree<SyncInfo> {
    fn from(value: VfsDirCreation<SyncInfo>) -> Self {
        value.dir
    }
}

impl<SyncInfo: ToBytes> From<VfsDirCreation<SyncInfo>> for VfsDirCreation<Vec<u8>> {
    fn from(value: VfsDirCreation<SyncInfo>) -> Self {
        Self {
            path: value.path,
            dir: (&value.dir).into(),
        }
    }
}

/// Represents a File creation or modification update that has been successfully applied on the
/// [`FSBackend`] and can be applied to a VFS.
///
/// [`FSBackend`]: crate::concrete::FSBackend
#[derive(Clone, Debug)]
pub struct VfsFileUpdate<SyncInfo> {
    path: VirtualPathBuf,
    file_info: SyncInfo,
    last_modified: DateTime<Utc>,
    file_size: u64,
}

impl<SyncInfo> VfsFileUpdate<SyncInfo> {
    pub fn new(
        path: &VirtualPath,
        file_size: u64,
        last_modified: DateTime<Utc>,
        file_info: SyncInfo,
    ) -> Self {
        Self {
            path: path.to_owned(),
            file_info,
            last_modified,
            file_size,
        }
    }

    pub fn path(&self) -> &VirtualPath {
        &self.path
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn last_modified(&self) -> DateTime<Utc> {
        self.last_modified
    }

    pub fn sync_info(&self) -> &SyncInfo {
        &self.file_info
    }
}

impl<SyncInfo: ToBytes> From<VfsFileUpdate<SyncInfo>> for VfsFileUpdate<Vec<u8>> {
    fn from(value: VfsFileUpdate<SyncInfo>) -> Self {
        Self {
            path: value.path,
            file_size: value.file_size,
            last_modified: value.last_modified,
            file_info: value.file_info.to_bytes(),
        }
    }
}

/// A conflict between nodes that will be stored in the Vfs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VfsConflict {
    update: VfsDiff,
    otherside_conflict_paths: Vec<VirtualPathBuf>,
}

impl From<UpdateConflict> for VfsConflict {
    fn from(value: UpdateConflict) -> Self {
        Self {
            update: value.update.update,
            otherside_conflict_paths: value.otherfs_conflict_path,
        }
    }
}

impl VfsConflict {
    pub fn new(update: VfsDiff, otherside_conflict_paths: &[VirtualPathBuf]) -> Self {
        Self {
            update,
            otherside_conflict_paths: otherside_conflict_paths.to_vec(),
        }
    }

    /// The updates that caused the conflict
    pub fn update(&self) -> &VfsDiff {
        &self.update
    }

    /// The path, in this filesystem, of the update that caused the conflict
    pub fn path(&self) -> &VirtualPath {
        self.update.path()
    }

    /// The path on the other side of the synchro that caused the conflict.
    ///
    /// Most of the time, this will be the same path as the one in the update. It may be different
    /// when a directory has been removed and some of its children have been modified.
    pub fn otherside_conflict_paths(&self) -> &[VirtualPathBuf] {
        &self.otherside_conflict_paths
    }

    /// Returns the list of directory that should be re-created for the conflict resolution
    ///
    /// # Example
    /// Conflict is cause by a dir that has been removed remotely and a file modified locally
    ///
    /// ```
    /// # use brume::update::{VfsConflict, VfsDiff};
    /// # use brume::vfs::virtual_path::VirtualPathBuf;
    ///
    /// let diff = VfsDiff::file_modified(VirtualPathBuf::new("/a/b/c/d/e").unwrap());
    /// let conflict = VfsConflict::new(diff, &[VirtualPathBuf::new("/a/b").unwrap()]);
    /// let to_recreate = vec![
    ///     VfsDiff::dir_created(VirtualPathBuf::new("/a/b").unwrap()),
    ///     VfsDiff::dir_created(VirtualPathBuf::new("/a/b/c").unwrap()),
    ///     VfsDiff::dir_created(VirtualPathBuf::new("/a/b/c/d").unwrap()),
    /// ];
    /// assert_eq!(conflict.otherside_dir_to_recreate(), to_recreate);
    /// ```
    pub fn otherside_dir_to_recreate(&self) -> Vec<VfsDiff> {
        // Get the paths from the other side update that are parents of this update
        let missing_paths = self
            .otherside_conflict_paths
            .iter()
            .filter_map(|path| {
                let diff = self.path().chroot(path).ok()?.parent()?;

                let mut missing = vec![path.to_owned()];
                let mut base = path.to_owned();
                for item in diff.iter() {
                    base.push(item);
                    missing.push(base.clone());
                }
                Some(missing)
            })
            .flatten()
            .collect();

        let dedup_paths = SortedVec::from_vec(missing_paths);

        dedup_paths.into_iter().map(VfsDiff::dir_created).collect()
    }
}

/// An update that can be applied to the [`Vfs`] at the end of the update lifecycle.
///
/// [`Vfs`]: crate::vfs::Vfs
#[derive(Debug)]
pub enum VfsUpdate<SyncInfo> {
    DirCreated(VfsDirCreation<SyncInfo>),
    DirRemoved(VirtualPathBuf),
    FileCreated(VfsFileUpdate<SyncInfo>),
    FileModified(VfsFileUpdate<SyncInfo>),
    FileRemoved(VirtualPathBuf),
    FailedApplication(FailedUpdateApplication),
    Conflict(VfsConflict),
    //TODO: detect moves
}

impl<SyncInfo> VfsUpdate<SyncInfo> {
    pub fn path(&self) -> &VirtualPath {
        match self {
            Self::DirCreated(update) => update.path(),
            Self::FileCreated(update) | Self::FileModified(update) => update.path(),
            Self::DirRemoved(path) | Self::FileRemoved(path) => path,
            Self::FailedApplication(failed_update) => failed_update.path(),
            Self::Conflict(update) => update.path(),
        }
    }

    pub fn is_err(&self) -> bool {
        matches!(self, Self::FailedApplication(_))
    }

    pub fn is_removal(&self) -> bool {
        matches!(self, Self::DirRemoved(_) | Self::FileRemoved(_))
    }

    pub fn is_creation(&self) -> bool {
        matches!(self, Self::DirCreated(_) | Self::FileCreated(_))
    }
}

impl<SyncInfo: ToBytes> From<VfsUpdate<SyncInfo>> for VfsUpdate<Vec<u8>> {
    fn from(value: VfsUpdate<SyncInfo>) -> Self {
        match value {
            VfsUpdate::DirCreated(update) => VfsUpdate::DirCreated(update.into()),
            VfsUpdate::DirRemoved(path) => VfsUpdate::DirRemoved(path),
            VfsUpdate::FileCreated(update) => VfsUpdate::FileCreated(update.into()),
            VfsUpdate::FileModified(update) => VfsUpdate::FileModified(update.into()),
            VfsUpdate::FileRemoved(path) => VfsUpdate::FileRemoved(path),
            VfsUpdate::FailedApplication(failed_update) => {
                VfsUpdate::FailedApplication(failed_update)
            }
            VfsUpdate::Conflict(vfs_diff) => VfsUpdate::Conflict(vfs_diff),
        }
    }
}

impl<SyncInfo> Sortable for VfsUpdate<SyncInfo> {
    // Sort by path, since reconciliation guarantees that there will be only one update for each
    // path
    type Key = VirtualPath;

    fn key(&self) -> &Self::Key {
        self.path()
    }
}
