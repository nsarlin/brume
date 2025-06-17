//! Link two [`FileSystem`] for bidirectional synchronization

use std::fmt::Display;

use futures::{
    TryFutureExt,
    future::{BoxFuture, try_join_all},
};
use log::warn;
use serde::{Deserialize, Serialize};
use tokio::try_join;

use crate::{
    Error,
    concrete::{
        ConcreteUpdateApplicationError, FSBackend, InvalidBytesSyncInfo, ToBytes, TryFromBytes,
    },
    filesystem::FileSystem,
    sorted_vec::SortedVec,
    update::{AppliedUpdate, ReconciledUpdate, VfsConflict, VfsDiff, VfsDiffList, VfsUpdate},
    vfs::{StatefulVfs, VirtualPath},
};

#[derive(Debug, Default)]
pub struct FullSyncResult {
    local_updates: Vec<VfsUpdate<Vec<u8>>>,
    remote_updates: Vec<VfsUpdate<Vec<u8>>>,
    status: FullSyncStatus,
}

impl FullSyncResult {
    /// Create an empty result that can be updated using [`Self::merge`]
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn status(&self) -> FullSyncStatus {
        self.status
    }

    pub fn local_updates(&self) -> &[VfsUpdate<Vec<u8>>] {
        &self.local_updates
    }

    pub fn remote_updates(&self) -> &[VfsUpdate<Vec<u8>>] {
        &self.remote_updates
    }

    /// Merge the results from 2 update applications
    pub fn merge(&mut self, other: Self) {
        self.local_updates.extend(other.local_updates);
        self.remote_updates.extend(other.remote_updates);
        self.status = self.status.merge(other.status);
    }
}

/// Status in which the [`Synchro`] can be after a call to [`full_sync`]
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
// The statuses are sorted from "best" to "worse", were a bad status will have priority over a good
// one
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord, Default)]
pub enum FullSyncStatus {
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

impl From<&Error> for FullSyncStatus {
    fn from(value: &Error) -> Self {
        if value.is_concrete() {
            Self::Error
        } else {
            Self::Desync
        }
    }
}

impl FullSyncStatus {
    /// Merge the statuses from 2 update applications, giving priority to the worst statuses
    pub fn merge(self, other: Self) -> Self {
        self.max(other)
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
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

impl Display for SynchroSide {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SynchroSide::Local => write!(f, "local"),
            SynchroSide::Remote => write!(f, "remote"),
        }
    }
}

/// A link between 2 [`FileSystem`] that are synchronized.
///
/// Since synchronization is bidirectional, there is almost no difference between how the `local`
/// and `remote` filesystems are handled. The only difference is that conflict files will only be
/// created on the local side.
pub struct Synchro<LocalBackend: FSBackend, RemoteBackend: FSBackend> {
    local: FileSystem<LocalBackend>,
    remote: FileSystem<RemoteBackend>,
}

impl<LocalBackend: FSBackend, RemoteBackend: FSBackend> Synchro<LocalBackend, RemoteBackend> {
    pub fn new(local: FileSystem<LocalBackend>, remote: FileSystem<RemoteBackend>) -> Self {
        Self { local, remote }
    }

    pub fn local(&self) -> &FileSystem<LocalBackend> {
        &self.local
    }

    pub fn remote(&self) -> &FileSystem<RemoteBackend> {
        &self.remote
    }

    pub fn get_status(&self) -> FullSyncStatus {
        if !self.local.vfs().get_errors().is_empty() || !self.remote.vfs().get_errors().is_empty() {
            FullSyncStatus::Error
        } else if !self.local.vfs().get_conflicts().is_empty()
            || !self.remote.vfs().get_conflicts().is_empty()
        {
            FullSyncStatus::Conflict
        } else {
            FullSyncStatus::Ok
        }
    }

    /// Reconciles updates lists from both filesystems by removing duplicates and detecting
    /// conflicts.
    ///
    /// This is done in three steps:
    /// - First merge individual updates by comparing them between one list and the other
    /// - Then access the concrete backends to check if duplicates should be removed or if they are
    ///   conflicts. They are removed when the content of the files are identical on both
    ///   filesystems, or are marked as conflicts if the content differs.
    /// - Then find conflicts with a directory and one of its elements. For example, if a file is
    ///   updated on one side and its parent directory is deleted on the other.
    pub async fn reconcile(
        &self,
        local_updates: VfsDiffList,
        remote_updates: VfsDiffList,
    ) -> Result<SortedVec<ReconciledUpdate>, Error> {
        let local_vfs = match self.local.loaded_vfs() {
            Some(vfs) => vfs,
            None => &self
                .local
                .backend()
                .load_virtual()
                .await
                .map_err(|e| Error::vfs_reload(LocalBackend::TYPE_NAME, e.into()))?,
        };

        let remote_vfs = match self.remote.loaded_vfs() {
            Some(vfs) => vfs,
            None => &self
                .remote
                .backend()
                .load_virtual()
                .await
                .map_err(|e| Error::vfs_reload(RemoteBackend::TYPE_NAME, e.into()))?,
        };

        // Merge updates only looking at their content relative to the Vfs
        let merged = local_updates.merge(remote_updates, local_vfs, remote_vfs)?;

        // Check with concrete backend if the file differ, if duplicates are to be removed or if
        // they are conflicts
        let concrete_merged = self
            .local
            .concrete()
            .filter_update_conflicts_list(self.remote.concrete(), merged)
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
            SortedVec<VfsUpdate<LocalBackend::SyncInfo>>,
            SortedVec<VfsUpdate<RemoteBackend::SyncInfo>>,
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
                ReconciledUpdate::Conflict(conflict) => {
                    warn!("conflict on {:?}", conflict.update());
                    if conflict.is_removal() {
                        // Don't push removal updates since the node will not exist anymore in the
                        // source Vfs. Instead, store a "reverse" update in the destination
                        // directory. For example, if the update was a removed dir in src, instead
                        // we store a created dir in dest.
                        conflicts.insert(conflict.clone().invert());
                        conflicts.insert(conflict);
                    } else {
                        conflicts.insert(conflict.clone());
                    }
                }
            }
        }
        let conflicts =
            conflicts.dedup_by(|conflict| (conflict.path().to_owned(), conflict.update().target()));

        let (local_updates, remote_updates) = applicables.split_local_remote();
        let (local_conflicts, remote_conflicts) = conflicts.split_local_remote();

        // Apply the updates
        let local_futures = try_join_all(
            local_updates
                .into_iter()
                .map(|update| self.local.concrete().apply_update(&self.remote, update)),
        );

        let remote_futures = try_join_all(
            remote_updates
                .into_iter()
                .map(|update| self.remote.concrete().apply_update(&self.local, update)),
        );

        let (local_applied, remote_applied) = try_join!(
            local_futures.map_err(|e| (e, LocalBackend::TYPE_NAME)),
            remote_futures.map_err(|e| (e, RemoteBackend::TYPE_NAME))
        )?;

        let mut local_res = Vec::new();
        let mut remote_res = Vec::new();

        for applied in local_applied.into_iter().flatten() {
            let (remote, local) = applied.into();
            remote_res.push(remote);
            if let Some(local) = local {
                local_res.push(local);
            }
        }

        for applied in remote_applied.into_iter().flatten() {
            let (local, remote) = applied.into();
            local_res.push(local);
            if let Some(remote) = remote {
                remote_res.push(remote);
            }
        }

        local_res.extend(local_conflicts.clone().into_iter().map(VfsUpdate::Conflict));
        remote_res.extend(
            remote_conflicts
                .clone()
                .into_iter()
                .map(VfsUpdate::Conflict),
        );

        Ok((
            SortedVec::from_vec(local_res),
            SortedVec::from_vec(remote_res),
        ))
    }

    /// Applies an update to the concrete FS and update the VFS accordingly
    pub async fn apply_update(
        &mut self,
        side: SynchroSide,
        update: VfsDiff,
    ) -> Result<FullSyncResult, Error>
    where
        <LocalBackend as FSBackend>::SyncInfo: ToBytes,
        <RemoteBackend as FSBackend>::SyncInfo: ToBytes,
    {
        match side {
            SynchroSide::Local => {
                let (local_applied, remote_applied) =
                    apply_to_fs(update, &mut self.local, &mut self.remote).await?;

                Ok(FullSyncResult {
                    local_updates: local_applied
                        .into_iter()
                        .map(|update| update.into())
                        .collect(),
                    remote_updates: remote_applied
                        .into_iter()
                        .map(|update| update.into())
                        .collect(),
                    status: self.get_status(),
                })
            }
            SynchroSide::Remote => {
                let (remote_applied, local_applied) =
                    apply_to_fs(update, &mut self.remote, &mut self.local).await?;

                Ok(FullSyncResult {
                    local_updates: local_applied
                        .into_iter()
                        .map(|update| update.into())
                        .collect(),
                    remote_updates: remote_applied
                        .into_iter()
                        .map(|update| update.into())
                        .collect(),
                    status: self.get_status(),
                })
            }
        }
    }

    /// Applies [`FileSystem::diff_vfs`] on both ends of the synchro.
    ///
    /// Returns the updates on both fs relative to the previously loaded Vfs.
    pub async fn diff_vfs(&mut self) -> Result<(VfsDiffList, VfsDiffList), Error> {
        try_join!(
            self.local
                .diff_vfs()
                .map_err(|e| Error::vfs_reload(LocalBackend::TYPE_NAME, e)),
            self.remote
                .diff_vfs()
                .map_err(|e| Error::vfs_reload(RemoteBackend::TYPE_NAME, e))
        )
    }

    fn get_conflict_update(&self, path: &VirtualPath, side: SynchroSide) -> Option<VfsConflict> {
        match side {
            SynchroSide::Local => self.local.vfs().find_conflict(path).cloned(),
            SynchroSide::Remote => self.remote.vfs().find_conflict(path).cloned(),
        }
    }
}

pub trait Synchronized {
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
    fn full_sync(&mut self) -> BoxFuture<'_, Result<FullSyncResult, Error>>;

    /// Resolves a conflict by selecting a [`SynchroSide`] an applying its update on the other side
    fn resolve_conflict<'a>(
        &'a mut self,
        path: &'a VirtualPath,
        side: SynchroSide,
    ) -> BoxFuture<'a, Result<FullSyncResult, Error>>;

    /// Returns the local [`Vfs`]
    ///
    /// The type specific sync info are erased but the directory structure and node state is
    /// preserved
    ///
    /// [`Vfs`]: crate::vfs::Vfs
    fn local_vfs(&self) -> StatefulVfs<()>;

    /// Returns the remote [`Vfs`]
    ///
    /// The type specific sync info are erased but the directory structure and node state is
    /// preserved
    ///
    /// [`Vfs`]: crate::vfs::Vfs
    fn remote_vfs(&self) -> StatefulVfs<()>;

    /// Updates the local [`Vfs`]
    ///
    /// The sync info are deserialized from bytes, this function returns an error if
    /// deserialization fails
    ///
    /// [`Vfs`]: crate::vfs::Vfs
    fn set_local_vfs(&mut self, vfs: StatefulVfs<Vec<u8>>) -> Result<(), InvalidBytesSyncInfo>;

    /// Updates the remote [`Vfs`]
    ///
    /// The sync info are deserialized from bytes, this function returns an error if
    /// deserialization fails
    ///
    /// [`Vfs`]: crate::vfs::Vfs
    fn set_remote_vfs(&mut self, vfs: StatefulVfs<Vec<u8>>) -> Result<(), InvalidBytesSyncInfo>;
}

impl<LocalBackend: FSBackend + 'static, RemoteBackend: FSBackend + 'static> Synchronized
    for Synchro<LocalBackend, RemoteBackend>
where
    LocalBackend::SyncInfo: ToBytes + TryFromBytes,
    RemoteBackend::SyncInfo: ToBytes + TryFromBytes,
{
    fn full_sync(&mut self) -> BoxFuture<'_, Result<FullSyncResult, Error>> {
        Box::pin(async move {
            let (local_diff, remote_diff) = self.diff_vfs().await?;

            let reconciled = self.reconcile(local_diff, remote_diff).await?;

            let (local_applied, remote_applied) = self
                .apply_updates_list_concrete(reconciled)
                .await
                .map_err(|(e, name)| Error::concrete_application(name, e))?;

            self.local
                .apply_updates_list_vfs(&local_applied)
                .map_err(|e| Error::vfs_update_application::<LocalBackend>(e))?;
            self.remote
                .apply_updates_list_vfs(&remote_applied)
                .map_err(|e| Error::vfs_update_application::<RemoteBackend>(e))?;

            let res = FullSyncResult {
                local_updates: local_applied
                    .into_iter()
                    .map(|update| update.into())
                    .collect(),
                remote_updates: remote_applied
                    .into_iter()
                    .map(|update| update.into())
                    .collect(),
                status: self.get_status(),
            };
            Ok(res)
        })
    }

    fn resolve_conflict<'a>(
        &'a mut self,
        path: &'a VirtualPath,
        side: SynchroSide,
    ) -> BoxFuture<'a, Result<FullSyncResult, Error>> {
        Box::pin(async move {
            let mut result = FullSyncResult::empty();
            if let Some(conflict) = self.get_conflict_update(path, side) {
                for to_recreate in conflict.otherside_dir_to_recreate() {
                    result.merge(self.apply_update(side.invert(), to_recreate).await?);
                }
                result.merge(
                    self.apply_update(side.invert(), conflict.update().clone())
                        .await?,
                );
            } else {
                // If no conflict is found, it probably means that it has been resolved in the
                // meantime, so we just log it
                warn!("conflict not found on node {path:?}");
            }

            Ok(result)
        })
    }

    fn local_vfs(&self) -> StatefulVfs<()> {
        self.local.vfs().into()
    }

    fn remote_vfs(&self) -> StatefulVfs<()> {
        self.remote.vfs().into()
    }

    fn set_local_vfs(&mut self, vfs: StatefulVfs<Vec<u8>>) -> Result<(), InvalidBytesSyncInfo> {
        let typed_vfs = vfs.try_into()?;
        *self.local.vfs_mut() = typed_vfs;
        Ok(())
    }

    fn set_remote_vfs(&mut self, vfs: StatefulVfs<Vec<u8>>) -> Result<(), InvalidBytesSyncInfo> {
        let typed_vfs = vfs.try_into()?;
        *self.remote.vfs_mut() = typed_vfs;
        Ok(())
    }
}

async fn apply_to_fs<TargetBackend: FSBackend, RefBackend: FSBackend>(
    update: VfsDiff,
    target_fs: &mut FileSystem<TargetBackend>,
    ref_fs: &mut FileSystem<RefBackend>,
) -> Result<
    (
        Vec<VfsUpdate<TargetBackend::SyncInfo>>,
        Vec<VfsUpdate<RefBackend::SyncInfo>>,
    ),
    Error,
> {
    let applied = target_fs
        .concrete()
        .apply_update(ref_fs, update)
        .await
        .map_err(|e| Error::concrete_application(TargetBackend::TYPE_NAME, e))?;

    let mut target_updates = Vec::new();
    let mut ref_updates = Vec::new();

    for update in applied {
        // Errors are applied on the source Vfs to make them trigger a resync, but they
        // are detected on the target side. So we need to move them
        // from one fs to the other.
        if let AppliedUpdate::FailedApplication(failed_update) = update {
            let failed = VfsUpdate::FailedApplication(failed_update);
            ref_fs
                .apply_update_vfs(&failed)
                .inspect(|_| ref_updates.push(failed))
        } else {
            let (ref_update, target_update) = update.into();
            ref_fs
                .apply_update_vfs(&ref_update)
                .inspect(|_| ref_updates.push(ref_update))
                .and_then(|_| {
                    if let Some(target) = target_update {
                        target_fs
                            .apply_update_vfs(&target)
                            .inspect(|_| target_updates.push(target))
                    } else {
                        Ok(())
                    }
                })
        }
        .map_err(|e| Error::vfs_update_application::<TargetBackend>(e))?;
    }
    Ok((target_updates, ref_updates))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        test_utils::{
            ConcreteTestNode,
            TestNode::{D, FE, FF},
        },
        update::VfsDiff,
        vfs::VirtualPathBuf,
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
        let local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );
        let remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base));

        let mut synchro = Synchro::new(local_fs, remote_fs);
        synchro.diff_vfs().await.unwrap();

        let local_diff = SortedVec::from([
            VfsDiff::file_modified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsDiff::dir_removed(VirtualPathBuf::new("/a/b").unwrap()),
            VfsDiff::dir_created(VirtualPathBuf::new("/e").unwrap()),
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
        let local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );

        let remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(local_fs, remote_fs);
        synchro.diff_vfs().await.unwrap();

        let local_diff = SortedVec::from([
            VfsDiff::file_modified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsDiff::dir_created(VirtualPathBuf::new("/e").unwrap()),
        ]);

        let remote_diff =
            SortedVec::from([VfsDiff::dir_removed(VirtualPathBuf::new("/a/b").unwrap())]);

        let reconciled = synchro.reconcile(local_diff, remote_diff).await.unwrap();

        let reconciled_ref = SortedVec::from([
            ReconciledUpdate::applicable_remote(&VfsDiff::file_modified(
                VirtualPathBuf::new("/Doc/f1.md").unwrap(),
            )),
            ReconciledUpdate::applicable_local(&VfsDiff::dir_removed(
                VirtualPathBuf::new("/a/b").unwrap(),
            )),
            ReconciledUpdate::applicable_remote(&VfsDiff::dir_created(
                VirtualPathBuf::new("/e").unwrap(),
            )),
        ]);

        assert_eq!(reconciled, reconciled_ref);
    }

    /// Test conflict detection
    #[tokio::test]
    async fn test_reconciliation_conflict() {
        let base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hi"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );

        let local_fs = FileSystem::new(ConcreteTestNode::from(base.clone()));
        let remote_fs = FileSystem::new(ConcreteTestNode::from(base.clone()));

        let mut synchro = Synchro::new(local_fs, remote_fs);
        synchro.full_sync().await.unwrap();

        let local_mod = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );
        synchro.local.set_backend(ConcreteTestNode::from(local_mod));

        let remote_mod = D(
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
        synchro
            .remote
            .set_backend(ConcreteTestNode::from(remote_mod));

        let local_diff = SortedVec::from([
            VfsDiff::file_modified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsDiff::dir_removed(VirtualPathBuf::new("/a/b").unwrap()),
        ]);

        let remote_diff = SortedVec::from([
            VfsDiff::file_modified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsDiff::file_created(VirtualPathBuf::new("/a/b/test.log").unwrap()),
        ]);

        let reconciled = synchro.reconcile(local_diff, remote_diff).await.unwrap();

        let modif_update = VfsDiff::file_modified(VirtualPathBuf::new("/Doc/f1.md").unwrap());

        let reconciled_ref = SortedVec::from([
            ReconciledUpdate::conflict_both(&modif_update),
            ReconciledUpdate::conflict_local(
                &VfsDiff::dir_removed(VirtualPathBuf::new("/a/b").unwrap()),
                &[VirtualPathBuf::new("/a/b/test.log").unwrap()],
            ),
            ReconciledUpdate::conflict_remote(
                &VfsDiff::file_created(VirtualPathBuf::new("/a/b/test.log").unwrap()),
                &[VirtualPathBuf::new("/a/b").unwrap()],
            ),
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
        let local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

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
        let remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(local_fs, remote_fs);
        synchro.diff_vfs().await.unwrap();

        let local_diff = SortedVec::from([VfsDiff::dir_created(VirtualPathBuf::new("/").unwrap())]);

        let remote_diff = local_diff.clone();

        let reconciled = synchro.reconcile(local_diff, remote_diff).await.unwrap();

        let reconciled_ref = SortedVec::from([
            ReconciledUpdate::conflict_both(&VfsDiff::file_created(
                VirtualPathBuf::new("/Doc/f1.md").unwrap(),
            )),
            ReconciledUpdate::applicable_local(&VfsDiff::file_created(
                VirtualPathBuf::new("/a/b/test.log").unwrap(),
            )),
            ReconciledUpdate::applicable_remote(&VfsDiff::dir_created(
                VirtualPathBuf::new("/e/g").unwrap(),
            )),
        ]);

        assert_eq!(reconciled, reconciled_ref);
    }

    #[tokio::test]
    async fn test_full_sync() {
        let local_base = D("", vec![]);
        let local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        let remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(local_fs, remote_fs);

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Ok
        );

        assert!(synchro.local.vfs().structural_eq(synchro.remote.vfs()));
    }

    /// Test conflict handling in synchro
    #[tokio::test]
    async fn test_full_sync_conflict() {
        // First synchronize the folders cleanly
        let local_base = D("", vec![]);
        let local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        let remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(local_fs, remote_fs);

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Ok
        );

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

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Conflict
        );

        assert!(synchro.local.vfs().structural_eq(synchro.remote.vfs()));
        assert!(
            synchro
                .local
                .vfs()
                .find_node("/Doc/f2.pdf".try_into().unwrap())
                .unwrap()
                .state()
                .is_conflict()
        );
        assert!(
            synchro
                .remote
                .vfs()
                .find_node("/Doc/f2.pdf".try_into().unwrap())
                .unwrap()
                .state()
                .is_conflict()
        );

        // Do a second sync to check that the conflict is not lost
        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Conflict
        );

        // Resolve the conflict
        assert_eq!(
            synchro
                .resolve_conflict("/Doc/f2.pdf".try_into().unwrap(), SynchroSide::Local)
                .await
                .unwrap()
                .status(),
            FullSyncStatus::Ok
        );
        assert!(synchro.local.vfs().structural_eq(synchro.remote.vfs()));
    }

    /// Test conflict handling in synchro where a file is modified on one side and removed on the
    /// other. The conflict is resolved by confirming the deletion.
    #[tokio::test]
    async fn test_full_sync_conflict_removed_confirmed() {
        // First synchronize the folders cleanly
        let local_base = D("", vec![]);
        let local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        let remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(local_fs, remote_fs);

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Ok
        );

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

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Conflict
        );

        assert!(
            synchro
                .local
                .vfs()
                .find_node("/Doc/f2.pdf".try_into().unwrap())
                .unwrap()
                .state()
                .is_conflict()
        );
        assert!(
            synchro
                .remote
                .vfs()
                .find_node("/Doc/f2.pdf".try_into().unwrap())
                .unwrap()
                .state()
                .is_conflict()
        );

        // Do a second sync to check that the conflict is not lost
        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Conflict
        );

        // Resolve the conflict
        assert_eq!(
            synchro
                .resolve_conflict("/Doc/f2.pdf".try_into().unwrap(), SynchroSide::Remote)
                .await
                .unwrap()
                .status(),
            FullSyncStatus::Ok
        );

        assert!(synchro.local.vfs().structural_eq(synchro.remote.vfs()));
    }

    /// Test conflict handling in synchro where a file is modified on one side and removed on the
    /// other. The conflict is resolved by cancelling the deletion.
    #[tokio::test]
    async fn test_full_sync_conflict_removed_cancelled() {
        // First synchronize the folders cleanly
        let local_base = D("", vec![]);
        let local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        let remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(local_fs, remote_fs);

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Ok
        );

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

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Conflict
        );

        assert!(
            synchro
                .local
                .vfs()
                .find_node("/Doc/f2.pdf".try_into().unwrap())
                .unwrap()
                .state()
                .is_conflict()
        );
        assert!(
            synchro
                .remote
                .vfs()
                .find_node("/Doc/f2.pdf".try_into().unwrap())
                .unwrap()
                .state()
                .is_conflict()
        );

        // Do a second sync to check that the conflict is not lost
        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Conflict
        );

        // Resolve the conflict
        assert_eq!(
            synchro
                .resolve_conflict("/Doc/f2.pdf".try_into().unwrap(), SynchroSide::Local)
                .await
                .unwrap()
                .status(),
            FullSyncStatus::Ok
        );

        assert!(synchro.local.vfs().structural_eq(synchro.remote.vfs()));
    }

    /// Test conflict handling in synchro where a file is modified on one side and its parent is
    /// removed on the other. The conflict is resolved by confirming the deletion.
    #[tokio::test]
    async fn test_full_sync_conflict_removed_parent_confirmed() {
        // First synchronize the folders cleanly
        let local_base = D("", vec![]);
        let local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        let remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(local_fs, remote_fs);

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Ok
        );

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

        let remote_modif = D("", vec![FF("file.doc", b"content")]);

        synchro
            .remote
            .set_backend(ConcreteTestNode::from(remote_modif));

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Conflict
        );

        assert!(
            synchro
                .local
                .vfs()
                .find_node("/Doc/f2.pdf".try_into().unwrap())
                .unwrap()
                .state()
                .is_conflict()
        );
        assert!(
            synchro
                .remote
                .vfs()
                .find_node("/Doc".try_into().unwrap())
                .unwrap()
                .state()
                .is_conflict()
        );

        // Do a second sync to check that the conflict is not lost
        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Conflict
        );

        // Resolve the conflict
        assert_eq!(
            synchro
                .resolve_conflict("/Doc".try_into().unwrap(), SynchroSide::Remote)
                .await
                .unwrap()
                .status(),
            FullSyncStatus::Ok
        );

        assert!(synchro.local.vfs().structural_eq(synchro.remote.vfs()));
    }

    /// Test conflict handling in synchro where a file is modified on one side and its parent is
    /// removed on the other. The conflict is resolved by cancelling the deletion.
    #[tokio::test]
    async fn test_full_sync_conflict_removed_parent_cancelled() {
        // First synchronize the folders cleanly
        let local_base = D("", vec![]);
        let local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        let remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(local_fs, remote_fs);

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Ok
        );

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

        let remote_modif = D("", vec![FF("file.doc", b"content")]);

        synchro
            .remote
            .set_backend(ConcreteTestNode::from(remote_modif));

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Conflict
        );

        assert!(
            synchro
                .local
                .vfs()
                .find_node("/Doc/f2.pdf".try_into().unwrap())
                .unwrap()
                .state()
                .is_conflict()
        );
        assert!(
            synchro
                .remote
                .vfs()
                .find_node("/Doc".try_into().unwrap())
                .unwrap()
                .state()
                .is_conflict()
        );

        // Do a second sync to check that the conflict is not lost
        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Conflict
        );

        // Resolve the conflict
        assert_eq!(
            synchro
                .resolve_conflict("/Doc/f2.pdf".try_into().unwrap(), SynchroSide::Local)
                .await
                .unwrap()
                .status(),
            FullSyncStatus::Ok
        );

        assert!(synchro.local.vfs().structural_eq(synchro.remote.vfs()));
    }

    // Test synchro with from scratch with a file where the concrete FS return an error
    #[tokio::test]
    async fn test_full_sync_errors() {
        let local_base = D("", vec![]);
        let local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FE("f2.pdf", "error")]),
                FF("file.doc", b"content"),
            ],
        );

        let remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(local_fs, remote_fs);

        // Check 2 sync with an Io error on a file
        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Error
        );
        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Error
        );

        // Everything should have been transferred except the file in error
        let expected_local = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello")]),
                FF("file.doc", b"content"),
            ],
        );

        assert_eq!(synchro.local.backend(), &expected_local.into());
        assert!(
            synchro
                .remote
                .vfs()
                .find_node("/Doc/f2.pdf".try_into().unwrap())
                .unwrap()
                .state()
                .is_err()
        );

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

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Ok
        );

        // Both filesystems should be perfectly in sync
        assert!(synchro.local.vfs().structural_eq(synchro.remote.vfs()));
    }

    /// Test synchro with an error on a file that is only modified
    #[tokio::test]
    async fn test_full_sync_errors_file_modified() {
        let local_base = D("", vec![]);
        let local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        let remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(local_fs, remote_fs);
        // First do a normal sync
        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Ok
        );

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

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Error
        );

        // Everything should have been transferred except the file in error
        let expected_local = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        assert_eq!(synchro.local.backend(), &expected_local.into());
        assert!(
            synchro
                .remote
                .vfs()
                .find_node("/Doc/f2.pdf".try_into().unwrap())
                .unwrap()
                .state()
                .is_err()
        );

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

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Ok
        );

        assert!(synchro.local.vfs().structural_eq(synchro.remote.vfs()));
    }

    /// test with a created file in error that is then removed
    #[tokio::test]
    async fn test_full_sync_errors_then_removed() {
        // Test with an error during file creation
        let local_base = D("", vec![]);
        let local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FE("f2.pdf", "error")]),
                FF("file.doc", b"content"),
            ],
        );

        let remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(local_fs, remote_fs);

        // sync with an Io error on a file
        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Error
        );

        // Everything should have been transferred except the file in error
        let expected_local = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello")]),
                FF("file.doc", b"content"),
            ],
        );

        assert_eq!(synchro.local.backend(), &expected_local.into());

        assert!(
            synchro
                .remote
                .vfs()
                .find_node("/Doc/f2.pdf".try_into().unwrap())
                .unwrap()
                .state()
                .is_err()
        );

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

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Ok
        );

        // Both filesystems should be perfectly in sync
        assert_eq!(synchro.local.backend(), synchro.remote.backend());
    }

    /// test with a modified file in error that is then removed
    #[tokio::test]
    async fn test_full_sync_errors_modified_then_removed() {
        let local_base = D("", vec![]);
        let local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        let remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(local_fs, remote_fs);
        // First do a normal sync
        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Ok
        );

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

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Error
        );

        // Nothing should be changed on the local side because of the error
        let expected_local = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        assert_eq!(synchro.local.backend(), &expected_local.into());
        assert!(
            synchro
                .remote
                .vfs()
                .find_node("/Doc/f2.pdf".try_into().unwrap())
                .unwrap()
                .state()
                .is_err()
        );

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

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Ok
        );

        assert!(synchro.local.vfs().structural_eq(synchro.remote.vfs()));
    }

    /// test with a modified file in error that is modified on the local side before the error is
    /// fixed
    #[tokio::test]
    async fn test_full_sync_errors_modified_then_modified_on_local() {
        let local_base = D("", vec![]);
        let local_fs = FileSystem::new(ConcreteTestNode::from(local_base.clone()));

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        let remote_fs = FileSystem::new(ConcreteTestNode::from(remote_base.clone()));

        let mut synchro = Synchro::new(local_fs, remote_fs);
        // First do a normal sync
        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Ok
        );

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

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Error
        );

        // Nothing should be changed on the local side because of the error
        let expected_local = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        assert_eq!(synchro.local.backend(), &expected_local.into());
        assert!(
            synchro
                .remote
                .vfs()
                .find_node("/Doc/f2.pdf".try_into().unwrap())
                .unwrap()
                .state()
                .is_err()
        );

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

        assert_eq!(
            synchro.full_sync().await.unwrap().status(),
            FullSyncStatus::Conflict
        );

        // This should create a conflict
        assert!(synchro.local.vfs().structural_eq(synchro.remote.vfs()));
        assert!(
            synchro
                .local
                .vfs()
                .find_node("/Doc/f2.pdf".try_into().unwrap())
                .unwrap()
                .state()
                .is_conflict()
        );
        assert!(
            synchro
                .remote
                .vfs()
                .find_node("/Doc/f2.pdf".try_into().unwrap())
                .unwrap()
                .state()
                .is_conflict()
        );
    }
}
