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

use std::cmp::Ordering;

use futures::future::try_join_all;

use crate::{
    concrete::{ConcreteFS, FSBackend, FsBackendError, Named},
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
            (NodeState::Conflict(_), _) | (_, NodeState::Conflict(_)) => {
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
    FileRemoved,
    FileModified,
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
#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
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
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
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

    /// Reconcile updates from two different VFS.
    ///
    /// This step is performed without access to the concrete filesystems. This means that only
    /// obvious conflicts are detected. Files that are modified on both fs will be flagged as
    /// `NeedBackendCheck` so they can be resolved later with concrete fs access.
    fn reconcile<LocalSyncInfo: Named, RemoteSyncInfo: Named>(
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
                )?;
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
    /// Merge two update lists by calling [`VfsDiff::reconcile`] on their elements one by
    /// one
    pub(crate) fn merge<SyncInfo: Named, RemoteSyncInfo: Named>(
        &self,
        remote_updates: VfsDiffList,
        local_vfs: &Vfs<SyncInfo>,
        remote_vfs: &Vfs<RemoteSyncInfo>,
    ) -> Result<SortedVec<VirtualReconciledUpdate>, ReconciliationError> {
        // Here we cannot use `iter_zip_map` or an async variant of it because it does not seem
        // possible to express the lifetimes required by the async closures

        let mut ret = Vec::new();
        let mut local_iter = self.iter();
        let mut remote_iter = remote_updates.iter();

        let mut local_item_opt = local_iter.next();
        let mut remote_item_opt = remote_iter.next();

        while let (Some(local_item), Some(remote_item)) = (local_item_opt, remote_item_opt) {
            match local_item.key().cmp(remote_item.key()) {
                Ordering::Less => {
                    // Propagate the update from local to remote
                    ret.push(VirtualReconciledUpdate::applicable_remote(local_item));
                    local_item_opt = local_iter.next()
                }

                Ordering::Equal => {
                    ret.extend(local_item.reconcile(remote_item, local_vfs, remote_vfs)?);
                    local_item_opt = local_iter.next();
                    remote_item_opt = remote_iter.next();
                }
                Ordering::Greater => {
                    // Propagate the update from remote to local
                    ret.push(VirtualReconciledUpdate::applicable_local(remote_item));
                    remote_item_opt = remote_iter.next();
                }
            }
        }

        // Handle the remaining items that are present in an iterator and not the
        // other one
        while let Some(local_item) = local_item_opt {
            ret.push(VirtualReconciledUpdate::applicable_remote(local_item));
            local_item_opt = local_iter.next();
        }

        while let Some(remote_item) = remote_item_opt {
            ret.push(VirtualReconciledUpdate::applicable_local(remote_item));
            remote_item_opt = remote_iter.next();
        }

        // Ok to use unchecked since we iterate on ordered updates
        Ok(SortedVec::unchecked_from_vec(ret))
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

    /// Remove duplicate updates that target the same nodes.
    ///
    /// Only the first update will be kept for a given path. This means that the priority will be
    /// based on the order of the [`UpdateKind`] enum. In practice, file creation will be favored
    /// over file modifications.
    ///
    /// # Example
    /// ```
    /// use brume::sorted_vec::SortedVec;
    /// use brume::update::*;
    /// use brume::vfs::VirtualPathBuf;
    ///
    /// let mut list = SortedVec::new();
    ///
    /// let update_a = VfsDiff::file_modified(VirtualPathBuf::new("/dir/a").unwrap());
    /// let applicable_a = ApplicableUpdate::new(UpdateTarget::Local, &update_a);
    /// list.insert(applicable_a);
    ///
    /// let update_b = VfsDiff::file_created(VirtualPathBuf::new("/dir/a").unwrap());
    /// let applicable_b = ApplicableUpdate::new(UpdateTarget::Remote, &update_b);
    /// list.insert(applicable_b.clone());
    ///
    /// let mut simplified = list.remove_duplicates();
    /// assert_eq!(simplified.len(), 1);
    /// let first = simplified.pop().unwrap();
    /// assert_eq!(first, applicable_b);
    /// ```
    pub fn remove_duplicates(self) -> Self {
        let mut path = VirtualPathBuf::root();
        let mut result = Vec::new();

        for update in self.into_iter() {
            if update.path() != path {
                path = update.path().to_owned();
                result.push(update);
            }
        }

        // Ok because if the input is sorted, the output will be too
        SortedVec::unchecked_from_vec(result)
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

    /// Resolve an update with `NeedBackendCheck` by comparing hashes of the file on both concrete
    /// FS
    async fn resolve_concrete<LocalBackend: FSBackend, RemoteBackend: FSBackend>(
        self,
        local_concrete: &ConcreteFS<LocalBackend>,
        remote_concrete: &ConcreteFS<RemoteBackend>,
    ) -> Result<Option<ReconciledUpdate>, ReconciliationError> {
        match self {
            VirtualReconciledUpdate::Applicable(update) => {
                Ok(Some(ReconciledUpdate::Applicable(update)))
            }
            VirtualReconciledUpdate::NeedBackendCheck(update) => {
                if local_concrete
                    .eq_file(remote_concrete, update.path())
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
    /// the concrete FS to resolve all the `NeedBackendCheck`
    pub(crate) async fn resolve_concrete<Backend: FSBackend, OtherBackend: FSBackend>(
        self,
        concrete_self: &ConcreteFS<Backend>,
        concrete_other: &ConcreteFS<OtherBackend>,
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

    /// Create a new `Applicable` update with [`UpdateTarget::Local`]
    pub fn applicable_local(update: &VfsDiff) -> Self {
        Self::Applicable(ApplicableUpdate::new(UpdateTarget::Local, update))
    }

    /// Create a new `Applicable` update with [`UpdateTarget::Remote`]
    pub fn applicable_remote(update: &VfsDiff) -> Self {
        Self::Applicable(ApplicableUpdate::new(UpdateTarget::Remote, update))
    }

    /// Create a new `Conflict` update with [`UpdateTarget::Local`]
    pub fn conflict_local(update: &VfsDiff) -> Self {
        Self::Conflict(ApplicableUpdate::new(UpdateTarget::Local, update))
    }

    /// Create a new `Conflict` update with [`UpdateTarget::Remote`]
    pub fn conflict_remote(update: &VfsDiff) -> Self {
        Self::Conflict(ApplicableUpdate::new(UpdateTarget::Remote, update))
    }

    /// Create two new `Conflict` updates with [`UpdateTarget::Local`] and
    /// [`UpdateTarget::Remote`]
    pub fn conflict_both(update: &VfsDiff) -> Self {
        Self::Conflict(ApplicableUpdate::new(UpdateTarget::Both, update))
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
                        next_update =
                            ReconciledUpdate::Conflict(next_update.update().invert_target());
                        updates.push(next_update);
                    }

                    if conflict {
                        parent_update =
                            ReconciledUpdate::Conflict(parent_update.update().invert_target())
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
/// [`FSBackend`].
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

/// An update that has been applied on a [`FSBackend`], and will be propagated to the [`Vfs`].
///
/// For node creation or modification, this types holds the SyncInfo needed to apply the update on
/// the `Vfs`. These SyncInfo must be fetched from the `FSBackend` immediatly after the update
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
    Conflict(VfsDiff),
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
    error: FsBackendError,
    update: VfsDiff,
}

impl FailedUpdateApplication {
    pub fn new(update: VfsDiff, error: FsBackendError) -> Self {
        Self { update, error }
    }

    pub fn path(&self) -> &VirtualPath {
        self.update.path()
    }

    pub fn update(&self) -> &VfsDiff {
        &self.update
    }

    pub fn error(&self) -> &FsBackendError {
        &self.error
    }
}
