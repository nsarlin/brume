//! Link two [`FileSystem`] for bidirectional synchronization

use std::fmt::Display;

use futures::{future::try_join_all, TryFutureExt};
use log::warn;
use serde::{Deserialize, Serialize};
use tokio::try_join;

use crate::{
    concrete::{ConcreteUpdateApplicationError, FSBackend},
    filesystem::FileSystem,
    sorted_vec::SortedVec,
    update::{AppliedUpdate, ReconciledUpdate, VfsNodeUpdate, VfsUpdateList},
    vfs::{NodeState, VirtualPath},
    Error,
};

/// State in which the [`Synchro`] can be after a call to [`full_sync`]
///
/// In case of successful sync, this is returned by `full_sync`. In that case, "Error" means
/// that some individual nodes failed to synchronize but the VFS was successfully updated.
/// This status can also be created from an [`Error`] returned by `full_sync`. In that case, nothing
/// was synchronized at all. "Error" status means that the Concrete backend is likely down,
/// and "Desync" means that a logic error has been encountered during the diff.
///
/// [`full_sync`]: Synchro::full_sync
/// [`NodeState`]: crate::vfs::dir_tree::NodeState
/// [`Error`]: enum@crate::Error
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default, Serialize, Deserialize)]
pub enum SynchroStatus {
    /// No node in any FS is in Conflict or Error state
    #[default]
    Ok,
    /// At least one node is in Conflict state, but no node is in Error state
    Conflict,
    /// At least one node is in Error state
    Error,
    /// There is some inconsistency in one of the Vfs, likely coming from a bug in brume.
    /// User should re-sync the faulty vfs from scratch
    Desync,
}

impl From<&Error> for SynchroStatus {
    fn from(value: &Error) -> Self {
        if value.is_concrete() {
            Self::Error
        } else {
            Self::Desync
        }
    }
}

impl Display for SynchroStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Copy, Clone)]
pub enum SynchroSide {
    Local,
    Remote,
}

impl SynchroSide {
    pub fn invert(self) -> Self {
        match self {
            SynchroSide::Local => Self::Remote,
            SynchroSide::Remote => Self::Local,
        }
    }
}

/// A link between 2 [`FileSystem`] that are synchronized.
///
/// Since synchronization is bidirectional, there is almost no difference between how the `local`
/// and `remote` filesystems are handled. The only difference is that conflict files will only be
/// created on the local side.
pub struct Synchro<'local, 'remote, LocalBackend: FSBackend, RemoteBackend: FSBackend> {
    local: &'local mut FileSystem<LocalBackend>,
    remote: &'remote mut FileSystem<RemoteBackend>,
}

impl<'local, 'remote, LocalBackend: FSBackend, RemoteBackend: FSBackend>
    Synchro<'local, 'remote, LocalBackend, RemoteBackend>
{
    pub fn new(
        local: &'local mut FileSystem<LocalBackend>,
        remote: &'remote mut FileSystem<RemoteBackend>,
    ) -> Self {
        Self { local, remote }
    }

    pub fn local(&self) -> &FileSystem<LocalBackend> {
        self.local
    }

    pub fn remote(&self) -> &FileSystem<RemoteBackend> {
        self.remote
    }

    /// Fully synchronizes both filesystems.
    ///
    /// This is done in three steps:
    /// - Vfs updates: both vfs are reloaded from their concrete FS. Updates on both filesystems are
    ///   detected.
    /// - Reconciliations: updates from both filesystems are merged and deduplicated. Conflicts are
    ///   detected.
    /// - Applications: Updates from one filesystem are applied to the other. Vfs are also updated
    ///   accordingly.
    ///
    /// See [`crate::update`] for more information about the update process.
    pub async fn full_sync(&mut self) -> Result<SynchroStatus, Error> {
        let (local_diff, remote_diff) = self.update_vfs().await?;

        let reconciled = self.reconcile(local_diff, remote_diff).await?;

        let (local_applied, remote_applied) = self
            .apply_updates_list_concrete(reconciled)
            .await
            .map_err(|(e, name)| Error::concrete_application(name, e))?;

        self.local
            .vfs_mut()
            .apply_updates_list(local_applied)
            .map_err(|e| Error::vfs_update_application(LocalBackend::TYPE_NAME, e))?;
        self.remote
            .vfs_mut()
            .apply_updates_list(remote_applied)
            .map_err(|e| Error::vfs_update_application(RemoteBackend::TYPE_NAME, e))?;

        Ok(self.get_status())
    }

    pub fn get_status(&self) -> SynchroStatus {
        if !self.local.vfs().get_errors().is_empty() || !self.remote.vfs().get_errors().is_empty() {
            SynchroStatus::Error
        } else if !self.local.vfs().get_conflicts().is_empty()
            || !self.remote.vfs().get_conflicts().is_empty()
        {
            SynchroStatus::Conflict
        } else {
            SynchroStatus::Ok
        }
    }

    /// Reconciles updates lists from both filesystems by removing duplicates and detecting
    /// conflicts.
    ///
    /// This is done in two steps:
    /// - First reconcile individual elements by comparing them between one list and the other
    /// - Then find conflicts with a directory and one of its elements
    pub async fn reconcile(
        &self,
        local_updates: VfsUpdateList,
        remote_updates: VfsUpdateList,
    ) -> Result<SortedVec<ReconciledUpdate>, Error> {
        let merged = local_updates.merge(remote_updates, self.local.vfs(), self.remote.vfs())?;

        let concrete_merged = merged
            .resolve_concrete(self.local.concrete(), self.remote.concrete())
            .await?;

        Ok(concrete_merged.resolve_ancestor_conflicts())
    }

    /// Applies a list of updates on the two FS on both ends of the synchro
    ///
    /// The target of the update will be chosen based on the value of the [`UpdateTarget`] of the
    /// [`ApplicableUpdate`].
    ///
    /// [`ApplicableUpdate`]: crate::update::ApplicableUpdate
    /// [`UpdateTarget`]: crate::update::UpdateTarget
    pub async fn apply_updates_list_concrete(
        &self,
        updates: SortedVec<ReconciledUpdate>,
    ) -> Result<
        (
            Vec<AppliedUpdate<LocalBackend::SyncInfo>>,
            Vec<AppliedUpdate<RemoteBackend::SyncInfo>>,
        ),
        (ConcreteUpdateApplicationError, &'static str),
    > {
        let mut applicables = SortedVec::new();
        let mut conflicts = SortedVec::new();
        for update in updates {
            match update {
                ReconciledUpdate::Applicable(applicable) => {
                    applicables.insert(applicable);
                }
                ReconciledUpdate::Conflict(update) => {
                    warn!("conflict on {update:?}");
                    if update.is_removal() {
                        // Don't push removal updates since the node will not exist anymore in the
                        // source Vfs. Instead, store a "reverse" update in the destination
                        // directory. For example, if the update was a removed dir in src, instead
                        // we store a created dir in dest.
                        conflicts.insert(update.invert());
                    } else {
                        conflicts.insert(update);
                    }
                }
            }
        }
        let conflicts = conflicts.remove_duplicates();

        let (local_updates, remote_updates) = applicables.split_local_remote();
        let (local_conflicts, remote_conflicts) = conflicts.split_local_remote();

        // Apply the updates
        let local_futures = try_join_all(
            local_updates
                .into_iter()
                .map(|update| self.local.apply_update_concrete(self.remote, update)),
        );

        let remote_futures = try_join_all(
            remote_updates
                .into_iter()
                .map(|update| self.remote.apply_update_concrete(self.local, update)),
        );

        let (local_applied, remote_applied) = try_join!(
            local_futures.map_err(|e| (e, LocalBackend::TYPE_NAME)),
            remote_futures.map_err(|e| (e, RemoteBackend::TYPE_NAME))
        )?;

        // Errors are applied on the source Vfs to make them trigger a resync, but they are detected
        // on the target side. So we need to move them from one fs to the other.
        let mut local_res = Vec::new();
        let mut local_failed = Vec::new();
        for applied in local_applied.into_iter().flatten() {
            match applied {
                AppliedUpdate::FailedApplication(failed_update) => {
                    local_failed.push(AppliedUpdate::FailedApplication(failed_update))
                }
                _ => local_res.push(applied),
            }
        }

        let mut remote_res = Vec::new();
        let mut remote_failed = Vec::new();
        for applied in remote_applied.into_iter().flatten() {
            match applied {
                AppliedUpdate::FailedApplication(failed_update) => {
                    remote_failed.push(AppliedUpdate::FailedApplication(failed_update))
                }
                _ => remote_res.push(applied),
            }
        }

        local_res.extend(remote_failed);
        local_res.extend(
            local_conflicts
                .clone()
                .into_iter()
                .map(|update| AppliedUpdate::Conflict(update.clone())),
        );
        remote_res.extend(local_failed);
        remote_res.extend(
            remote_conflicts
                .clone()
                .into_iter()
                .map(|update| AppliedUpdate::Conflict(update.clone())),
        );

        Ok((local_res, remote_res))
    }

    /// Applies an update to the concrete FS and update the VFS accordingly
    pub async fn apply_update(
        &mut self,
        side: SynchroSide,
        update: VfsNodeUpdate,
    ) -> Result<(), Error> {
        match side {
            SynchroSide::Local => {
                let applied = self
                    .local
                    .apply_update_concrete(self.remote, update)
                    .await
                    .map_err(|e| Error::concrete_application(LocalBackend::TYPE_NAME, e))?;

                for update in applied {
                    // Errors are applied on the source Vfs to make them trigger a resync, but they
                    // are detected on the target side. So we need to move them
                    // from one fs to the other.
                    if let AppliedUpdate::FailedApplication(failed_update) = update {
                        self.remote
                            .vfs_mut()
                            .apply_update(AppliedUpdate::FailedApplication(failed_update))
                    } else {
                        self.local.vfs_mut().apply_update(update)
                    }
                    .map_err(|e| Error::vfs_update_application(RemoteBackend::TYPE_NAME, e))?;
                }
                Ok(())
            }
            SynchroSide::Remote => {
                let applied = self
                    .remote
                    .apply_update_concrete(self.local, update)
                    .await
                    .unwrap();

                for update in applied {
                    // Errors are applied on the source Vfs to make them trigger a resync, but they
                    // are detected on the target side. So we need to move them
                    // from one fs to the other.
                    if let AppliedUpdate::FailedApplication(failed_update) = update {
                        self.local
                            .vfs_mut()
                            .apply_update(AppliedUpdate::FailedApplication(failed_update))
                    } else {
                        self.remote.vfs_mut().apply_update(update)
                    }
                    .map_err(|e| Error::vfs_update_application(RemoteBackend::TYPE_NAME, e))?;
                }

                Ok(())
            }
        }
    }

    /// Updates both [`Vfs`] by querying and parsing their respective concrete FS.
    ///
    /// Returns the updates on both fs, relative to the previously loaded Vfs.
    ///
    /// [`Vfs`]: crate::vfs::Vfs
    pub async fn update_vfs(&mut self) -> Result<(VfsUpdateList, VfsUpdateList), Error> {
        try_join!(
            self.local
                .update_vfs()
                .map_err(|e| Error::vfs_reload(LocalBackend::TYPE_NAME, e)),
            self.remote
                .update_vfs()
                .map_err(|e| Error::vfs_reload(RemoteBackend::TYPE_NAME, e))
        )
    }

    /// Resolves a conflict by selecting a [`SynchroSide`] an applying its update on the other side
    pub async fn resolve_conflict(
        &mut self,
        path: &VirtualPath,
        side: SynchroSide,
    ) -> Result<SynchroStatus, Error> {
        if let Some(update) = self.get_conflict_update(path, side) {
            self.apply_update(side.invert(), update).await?;
        } else {
            // If no conflict is found, it probably means that it has been resolved in the meantime,
            // so we just log it
            warn!("conflict not found on node {path:?}");
        }

        self.mark_conflict_resolved(path, side).await?;
        Ok(self.get_status())
    }

    fn get_conflict_update(&self, path: &VirtualPath, side: SynchroSide) -> Option<VfsNodeUpdate> {
        // First try to find the conflict at the given path. If nothing is found, it might be
        // because the update was a deletion and the node does not exist on the VFS anymore. In that
        // case, the conflict update must have been stored on the other side, reversed (ie: as a
        // creation)
        match side {
            SynchroSide::Local => self.local.vfs().find_conflict(path).cloned().or_else(|| {
                self.remote
                    .vfs()
                    .find_conflict(path)
                    .map(|update| update.clone().invert())
            }),
            SynchroSide::Remote => self.remote.vfs().find_conflict(path).cloned().or_else(|| {
                self.local
                    .vfs()
                    .find_conflict(path)
                    .map(|update| update.clone().invert())
            }),
        }
    }

    /// Removes the "Conflict" state of a node. Sets the state to Ok with updated SyncInfo.
    async fn mark_conflict_resolved(
        &mut self,
        path: &VirtualPath,
        side: SynchroSide,
    ) -> Result<(), Error> {
        match side {
            SynchroSide::Local => {
                // Skip if the node does not exist, meaning it was a removal
                if self.local.vfs().find_node(path).is_some() {
                    let state = match self.local.concrete().backend().get_sync_info(path).await {
                        Ok(info) => NodeState::Ok(info),
                        // If we can reach the concrete FS, we delay until the next full_sync
                        Err(_) => NodeState::NeedResync,
                    };
                    self.local
                        .vfs_mut()
                        .update_node_state(path, state)
                        .map_err(|e| Error::vfs_update_application(LocalBackend::TYPE_NAME, e))
                } else {
                    Ok(())
                }
            }
            SynchroSide::Remote => {
                if self.remote.vfs().find_node(path).is_some() {
                    let state = match self.remote.concrete().backend().get_sync_info(path).await {
                        Ok(info) => NodeState::Ok(info),
                        Err(_) => NodeState::NeedResync,
                    };
                    self.remote
                        .vfs_mut()
                        .update_node_state(path, state)
                        .map_err(|e| Error::vfs_update_application(LocalBackend::TYPE_NAME, e))
                } else {
                    Ok(())
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        test_utils::{
            ConcreteTestNode,
            TestNode::{D, FE, FF},
        },
        update::VfsNodeUpdate,
        vfs::{Vfs, VirtualPathBuf},
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
        let mut local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );
        let mut remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base));

        let mut synchro = Synchro::new(&mut local_fs, &mut remote_fs);
        synchro.update_vfs().await.unwrap();

        let local_diff = SortedVec::from([
            VfsNodeUpdate::file_modified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodeUpdate::dir_removed(VirtualPathBuf::new("/a/b").unwrap()),
            VfsNodeUpdate::dir_created(VirtualPathBuf::new("/e").unwrap()),
        ]);

        let remote_diff = local_diff.clone();

        let reconciled = synchro.reconcile(local_diff, remote_diff).await.unwrap();

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
        let mut local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );

        let mut remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(&mut local_fs, &mut remote_fs);
        synchro.update_vfs().await.unwrap();

        let local_diff = SortedVec::from([
            VfsNodeUpdate::file_modified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodeUpdate::dir_created(VirtualPathBuf::new("/e").unwrap()),
        ]);

        let remote_diff = SortedVec::from([VfsNodeUpdate::dir_removed(
            VirtualPathBuf::new("/a/b").unwrap(),
        )]);

        let reconciled = synchro.reconcile(local_diff, remote_diff).await.unwrap();

        let reconciled_ref = SortedVec::from([
            ReconciledUpdate::applicable_remote(&VfsNodeUpdate::file_modified(
                VirtualPathBuf::new("/Doc/f1.md").unwrap(),
            )),
            ReconciledUpdate::applicable_local(&VfsNodeUpdate::dir_removed(
                VirtualPathBuf::new("/a/b").unwrap(),
            )),
            ReconciledUpdate::applicable_remote(&VfsNodeUpdate::dir_created(
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
        let mut local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

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
        let mut remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(&mut local_fs, &mut remote_fs);
        synchro.update_vfs().await.unwrap();

        let local_diff = SortedVec::from([
            VfsNodeUpdate::file_modified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodeUpdate::dir_removed(VirtualPathBuf::new("/a/b").unwrap()),
        ]);

        let remote_diff = SortedVec::from([
            VfsNodeUpdate::file_modified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodeUpdate::file_created(VirtualPathBuf::new("/a/b/test.log").unwrap()),
        ]);

        let reconciled = synchro.reconcile(local_diff, remote_diff).await.unwrap();

        let modif_update = VfsNodeUpdate::file_modified(VirtualPathBuf::new("/Doc/f1.md").unwrap());

        let reconciled_ref = SortedVec::from([
            ReconciledUpdate::conflict_both(&modif_update),
            ReconciledUpdate::conflict_local(&VfsNodeUpdate::dir_removed(
                VirtualPathBuf::new("/a/b").unwrap(),
            )),
            ReconciledUpdate::conflict_remote(&VfsNodeUpdate::file_created(
                VirtualPathBuf::new("/a/b/test.log").unwrap(),
            )),
        ]);

        assert_eq!(reconciled, reconciled_ref);
    }

    /// Test reconciliation when directories with the same name but different content have been
    /// created on both sides
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
        let mut local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

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
        let mut remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(&mut local_fs, &mut remote_fs);
        synchro.update_vfs().await.unwrap();

        let local_diff = SortedVec::from([VfsNodeUpdate::dir_created(
            VirtualPathBuf::new("/").unwrap(),
        )]);

        let remote_diff = local_diff.clone();

        let reconciled = synchro.reconcile(local_diff, remote_diff).await.unwrap();

        let reconciled_ref = SortedVec::from([
            ReconciledUpdate::conflict_both(&VfsNodeUpdate::file_created(
                VirtualPathBuf::new("/Doc/f1.md").unwrap(),
            )),
            ReconciledUpdate::applicable_local(&VfsNodeUpdate::file_created(
                VirtualPathBuf::new("/a/b/test.log").unwrap(),
            )),
            ReconciledUpdate::applicable_remote(&VfsNodeUpdate::dir_created(
                VirtualPathBuf::new("/e/g").unwrap(),
            )),
        ]);

        assert_eq!(reconciled, reconciled_ref);
    }

    #[tokio::test]
    async fn test_full_sync() {
        let local_base = D("", vec![]);
        let mut local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        let mut remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(&mut local_fs, &mut remote_fs);

        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Ok);

        assert!(synchro
            .local
            .vfs()
            .diff(synchro.remote.vfs())
            .unwrap()
            .is_empty());
    }

    /// Test conflict handling in synchro
    #[tokio::test]
    async fn test_full_sync_conflict() {
        // First synchronize the folders cleanly
        let local_base = D("", vec![]);
        let mut local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        let mut remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(&mut local_fs, &mut remote_fs);

        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Ok);

        // Then do a modification on both sides
        let local_modif = D(
            "",
            vec![
                D(
                    "Doc",
                    vec![FF("f1.md", b"hello"), FF("f2.pdf", b"cruel world")],
                ),
                FF("file.doc", b"content"),
            ],
        );

        synchro
            .local
            .set_backend(ConcreteTestNode::from(local_modif));

        let remote_modif = D(
            "",
            vec![
                D(
                    "Doc",
                    vec![FF("f1.md", b"hello"), FF("f2.pdf", b"brave new world")],
                ),
                FF("file.doc", b"content"),
            ],
        );

        synchro
            .remote
            .set_backend(ConcreteTestNode::from(remote_modif));

        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Conflict);

        assert!(synchro.local.vfs().structural_eq(synchro.remote.vfs()));
        assert!(synchro
            .local
            .vfs()
            .find_node("/Doc/f2.pdf".try_into().unwrap())
            .unwrap()
            .state()
            .is_conflict());
        assert!(synchro
            .remote
            .vfs()
            .find_node("/Doc/f2.pdf".try_into().unwrap())
            .unwrap()
            .state()
            .is_conflict());

        // Resolve the conflict
        assert_eq!(
            synchro
                .resolve_conflict("/Doc/f2.pdf".try_into().unwrap(), SynchroSide::Local)
                .await
                .unwrap(),
            SynchroStatus::Ok
        );
        assert!(synchro
            .local
            .vfs()
            .diff(synchro.remote.vfs())
            .unwrap()
            .is_empty());
    }

    /// Test conflict handling in synchro where a file is modified on one side and removed on the
    /// other
    #[tokio::test]
    async fn test_full_sync_conflict_removed() {
        // First synchronize the folders cleanly
        let local_base = D("", vec![]);
        let mut local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        let mut remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(&mut local_fs, &mut remote_fs);

        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Ok);

        // Then do a modification on both sides
        let local_modif = D(
            "",
            vec![
                D(
                    "Doc",
                    vec![FF("f1.md", b"hello"), FF("f2.pdf", b"cruel world")],
                ),
                FF("file.doc", b"content"),
            ],
        );

        synchro
            .local
            .set_backend(ConcreteTestNode::from(local_modif));

        let remote_modif = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello")]),
                FF("file.doc", b"content"),
            ],
        );

        synchro
            .remote
            .set_backend(ConcreteTestNode::from(remote_modif));

        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Conflict);

        assert!(synchro
            .local
            .vfs()
            .find_node("/Doc/f2.pdf".try_into().unwrap())
            .unwrap()
            .state()
            .is_conflict());
        assert!(synchro
            .remote
            .vfs()
            .find_node("/Doc/f2.pdf".try_into().unwrap())
            .is_none());

        // Resolve the conflict
        assert_eq!(
            synchro
                .resolve_conflict("/Doc/f2.pdf".try_into().unwrap(), SynchroSide::Remote)
                .await
                .unwrap(),
            SynchroStatus::Ok
        );

        assert!(synchro
            .local
            .vfs()
            .diff(synchro.remote.vfs())
            .unwrap()
            .is_empty());
    }

    // Test synchro with from scratch with a file where the concrete FS return an error
    #[tokio::test]
    async fn test_full_sync_errors() {
        let local_base = D("", vec![]);
        let mut local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FE("f2.pdf", "error")]),
                FF("file.doc", b"content"),
            ],
        );

        let mut remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(&mut local_fs, &mut remote_fs);

        // Check 2 sync with an Io error on a file
        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Error);
        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Error);

        // Everything should have been transfered except the file in error
        let expected_local = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello")]),
                FF("file.doc", b"content"),
            ],
        );

        assert!(synchro
            .local
            .vfs()
            .diff(&Vfs::new(expected_local.into_node()))
            .unwrap()
            .is_empty());
        assert!(synchro
            .remote
            .vfs()
            .find_node("/Doc/f2.pdf".try_into().unwrap())
            .unwrap()
            .state()
            .is_err());

        // fix the error
        let remote_fixed = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        synchro
            .remote
            .set_backend(ConcreteTestNode::from(remote_fixed.clone()));

        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Ok);

        // Both filesystems should be perfectly in sync
        assert!(synchro
            .local
            .vfs()
            .diff(synchro.remote.vfs())
            .unwrap()
            .is_empty());
    }

    /// Test synchro with an error on a file that is only modified
    #[tokio::test]
    async fn test_full_sync_errors_file_modified() {
        let local_base = D("", vec![]);
        let mut local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        let mut remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(&mut local_fs, &mut remote_fs);
        // First do a normal sync
        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Ok);

        // Then put the file in error.
        // The next `update_vfs` will consider the node as "changed" because by default error test
        // nodes are converted to a vfs node with hash value 0
        let remote_error = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FE("f2.pdf", "error")]),
                FF("file.doc", b"content"),
            ],
        );
        synchro
            .remote
            .set_backend(ConcreteTestNode::from(remote_error.clone()));

        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Error);

        // Everything should have been transfered except the file in error
        let expected_local = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        assert!(synchro
            .local
            .vfs()
            .diff(&Vfs::new(expected_local.into_node()))
            .unwrap()
            .is_empty());
        assert!(synchro
            .remote
            .vfs()
            .find_node("/Doc/f2.pdf".try_into().unwrap())
            .unwrap()
            .state()
            .is_err());

        // Then fix the error
        let remote_fixed = D(
            "",
            vec![
                D(
                    "Doc",
                    vec![FF("f1.md", b"hello"), FF("f2.pdf", b"new world")],
                ),
                FF("file.doc", b"content"),
            ],
        );

        synchro
            .remote
            .set_backend(ConcreteTestNode::from(remote_fixed.clone()));

        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Ok);

        assert!(synchro
            .local
            .vfs()
            .diff(synchro.remote.vfs())
            .unwrap()
            .is_empty());
    }

    /// test with a created file in error that is then removed
    #[tokio::test]
    async fn test_full_sync_errors_then_removed() {
        // Test with an error during file creation
        let local_base = D("", vec![]);
        let mut local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FE("f2.pdf", "error")]),
                FF("file.doc", b"content"),
            ],
        );

        let mut remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(&mut local_fs, &mut remote_fs);

        // sync with an Io error on a file
        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Error);

        // Everything should have been transfered except the file in error
        let expected_local = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello")]),
                FF("file.doc", b"content"),
            ],
        );

        assert!(synchro
            .local
            .vfs()
            .structural_eq(&Vfs::new(expected_local.into_node())));

        assert!(synchro
            .remote
            .vfs()
            .find_node("/Doc/f2.pdf".try_into().unwrap())
            .unwrap()
            .state()
            .is_err());

        // Remove the file
        let remote_fixed = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello")]),
                FF("file.doc", b"content"),
            ],
        );

        synchro
            .remote
            .set_backend(ConcreteTestNode::from(remote_fixed.clone()));

        let mut synchro = Synchro::new(&mut local_fs, &mut remote_fs);

        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Ok);

        // Both filesystems should be perfectly in sync
        assert!(synchro.local.vfs().structural_eq(synchro.remote.vfs()));
    }

    /// test with a modified file in error that is then removed
    #[tokio::test]
    async fn test_full_sync_errors_modified_then_removed() {
        let local_base = D("", vec![]);
        let mut local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        let mut remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(&mut local_fs, &mut remote_fs);
        // First do a normal sync
        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Ok);

        // Then put the file in error.
        // The next `update_vfs` will consider the node as "changed" because by default error test
        // nodes are converted to a vfs node with hash value 0
        let remote_error = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FE("f2.pdf", "error")]),
                FF("file.doc", b"content"),
            ],
        );
        synchro
            .remote
            .set_backend(ConcreteTestNode::from(remote_error.clone()));

        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Error);

        // Nothing should be changed on the local side because of the error
        let expected_local = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        assert!(synchro
            .local
            .vfs()
            .diff(&Vfs::new(expected_local.into_node()))
            .unwrap()
            .is_empty());
        assert!(synchro
            .remote
            .vfs()
            .find_node("/Doc/f2.pdf".try_into().unwrap())
            .unwrap()
            .state()
            .is_err());

        // Now remove the file in error
        let remote_fixed = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello")]),
                FF("file.doc", b"content"),
            ],
        );

        synchro
            .remote
            .set_backend(ConcreteTestNode::from(remote_fixed.clone()));

        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Ok);

        assert!(synchro
            .local
            .vfs()
            .diff(synchro.remote.vfs())
            .unwrap()
            .is_empty());
    }

    /// test with a modified file in error that is modified on the local side before the error is
    /// fixed
    #[tokio::test]
    async fn test_full_sync_errors_modified_then_modified_on_local() {
        let local_base = D("", vec![]);
        let mut local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        let mut remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(&mut local_fs, &mut remote_fs);
        // First do a normal sync
        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Ok);

        // Then put the file in error.
        // The next `update_vfs` will consider the node as "changed" because by default error test
        // nodes are converted to a vfs node with hash value 0
        let remote_error = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FE("f2.pdf", "error")]),
                FF("file.doc", b"content"),
            ],
        );
        synchro
            .remote
            .set_backend(ConcreteTestNode::from(remote_error.clone()));

        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Error);

        // Nothing should be changed on the local side because of the error
        let expected_local = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        assert!(synchro
            .local
            .vfs()
            .diff(&Vfs::new(expected_local.into_node()))
            .unwrap()
            .is_empty());
        assert!(synchro
            .remote
            .vfs()
            .find_node("/Doc/f2.pdf".try_into().unwrap())
            .unwrap()
            .state()
            .is_err());

        // Modify the file on the local side
        let modified_local = D(
            "",
            vec![
                D(
                    "Doc",
                    vec![FF("f1.md", b"hello"), FF("f2.pdf", b"brave world")],
                ),
                FF("file.doc", b"content"),
            ],
        );

        // Fix the remote error
        let remote_fixed = D(
            "",
            vec![
                D(
                    "Doc",
                    vec![FF("f1.md", b"hello"), FF("f2.pdf", b"cruel world")],
                ),
                FF("file.doc", b"content"),
            ],
        );

        synchro
            .local
            .set_backend(ConcreteTestNode::from(modified_local.clone()));

        synchro
            .remote
            .set_backend(ConcreteTestNode::from(remote_fixed.clone()));

        assert_eq!(synchro.full_sync().await.unwrap(), SynchroStatus::Conflict);

        // This should create a conflict
        assert!(synchro.local.vfs().structural_eq(synchro.remote.vfs()));
        assert!(synchro
            .local
            .vfs()
            .find_node("/Doc/f2.pdf".try_into().unwrap())
            .unwrap()
            .state()
            .is_conflict());
        assert!(synchro
            .remote
            .vfs()
            .find_node("/Doc/f2.pdf".try_into().unwrap())
            .unwrap()
            .state()
            .is_conflict());
    }
}
