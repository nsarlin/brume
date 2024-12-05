//! Link two [`FileSystem`] for bidirectional synchronization

use std::future::Future;

use futures::TryFutureExt;
use tokio::try_join;

use crate::{
    concrete::ConcreteFS,
    filesystem::FileSystem,
    sorted_vec::SortedVec,
    update::{AppliedUpdate, ReconciledUpdate, VfsUpdateList},
    Error,
};

/// A link between 2 [`FileSystem`] that are synchronized.
///
/// Since synchronization is bidirectional, there is almost no difference between how the `local`
/// and `remote` filesystems are handled. The only difference is that conflict files will only be
/// created on the local side.
pub struct Synchro<LocalConcrete: ConcreteFS, RemoteConcrete: ConcreteFS> {
    local: FileSystem<LocalConcrete>,
    remote: FileSystem<RemoteConcrete>,
}

/// Trait for a pair of Filesystems that can be synchronized
pub trait Synchronizable {
    /// Fully synchronize both filesystems.
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
    fn full_sync(&mut self) -> impl Future<Output = Result<(), Error>>;
}

impl<LocalConcrete: ConcreteFS, RemoteConcrete: ConcreteFS> Synchro<LocalConcrete, RemoteConcrete> {
    pub fn new(local: LocalConcrete, remote: RemoteConcrete) -> Self {
        let local = FileSystem::new(local);
        let remote = FileSystem::new(remote);
        Self { local, remote }
    }

    pub fn local(&self) -> &FileSystem<LocalConcrete> {
        &self.local
    }

    pub fn remote(&self) -> &FileSystem<RemoteConcrete> {
        &self.remote
    }

    /// Reconcile updates lists from both filesystems by removing duplicates and detecting
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

    /// Apply a list of updates on the two FS on both ends of the synchro
    ///
    /// The target of the update will be chosen based on the value of the [`UpdateTarget`] of the
    /// [`ApplicableUpdate`]. Conflict files will always be created on the FileSystem in `self`.
    ///
    /// [`ApplicableUpdate`]: crate::update::ApplicableUpdate
    /// [`UpdateTarget`]: crate::update::UpdateTarget
    pub async fn apply_updates_list_concrete(
        &self,
        updates: SortedVec<ReconciledUpdate>,
    ) -> Result<
        (
            Vec<AppliedUpdate<LocalConcrete::SyncInfo>>,
            Vec<AppliedUpdate<RemoteConcrete::SyncInfo>>,
        ),
        Error,
    > {
        self.local
            .apply_updates_list_concrete(&self.remote, updates)
            .await
            .map_err(|(e, name)| Error::concrete_application(name, e))
    }

    /// Update both [`Vfs`] by querying and parsing their respective concrete FS.
    ///
    /// Return the updates on both fs, relative to the previously loaded Vfs.
    ///
    /// [`Vfs`]: crate::vfs::Vfs
    pub async fn update_vfs(&mut self) -> Result<(VfsUpdateList, VfsUpdateList), Error> {
        try_join!(
            self.local
                .update_vfs()
                .map_err(|e| Error::vfs_reload(LocalConcrete::TYPE_NAME, e)),
            self.remote
                .update_vfs()
                .map_err(|e| Error::vfs_reload(RemoteConcrete::TYPE_NAME, e))
        )
    }
}

impl<LocalConcrete: ConcreteFS, RemoteConcrete: ConcreteFS> Synchronizable
    for Synchro<LocalConcrete, RemoteConcrete>
{
    async fn full_sync(&mut self) -> Result<(), Error> {
        let (local_diff, remote_diff) = self.update_vfs().await?;
        let reconciled = self.reconcile(local_diff, remote_diff).await?;

        let (local_applied, remote_applied) = self.apply_updates_list_concrete(reconciled).await?;

        self.local
            .vfs_mut()
            .apply_updates_list(local_applied)
            .map_err(|e| Error::vfs_update_application(LocalConcrete::TYPE_NAME, e))?;
        self.remote
            .vfs_mut()
            .apply_updates_list(remote_applied)
            .map_err(|e| Error::vfs_update_application(RemoteConcrete::TYPE_NAME, e))?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        test_utils::{
            ConcreteTestNode,
            TestNode::{D, FF},
        },
        update::VfsNodeUpdate,
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
        let local_fs: ConcreteTestNode = local_base.clone().into();

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );
        let remote_fs: ConcreteTestNode = remote_base.into();

        let mut synchro = Synchro::new(local_fs, remote_fs);
        synchro.update_vfs().await.unwrap();

        let local_diff = SortedVec::from([
            VfsNodeUpdate::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodeUpdate::DirRemoved(VirtualPathBuf::new("/a/b").unwrap()),
            VfsNodeUpdate::DirCreated(VirtualPathBuf::new("/e").unwrap()),
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
        let local_fs: ConcreteTestNode = local_base.clone().into();

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );

        let remote_fs: ConcreteTestNode = remote_base.into();

        let mut synchro = Synchro::new(local_fs, remote_fs);
        synchro.update_vfs().await.unwrap();

        let local_diff = SortedVec::from([
            VfsNodeUpdate::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodeUpdate::DirCreated(VirtualPathBuf::new("/e").unwrap()),
        ]);

        let remote_diff = SortedVec::from([VfsNodeUpdate::DirRemoved(
            VirtualPathBuf::new("/a/b").unwrap(),
        )]);

        let reconciled = synchro.reconcile(local_diff, remote_diff).await.unwrap();

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
        let local_fs: ConcreteTestNode = local_base.clone().into();

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
        let remote_fs: ConcreteTestNode = remote_base.into();

        let mut synchro = Synchro::new(local_fs, remote_fs);
        synchro.update_vfs().await.unwrap();

        let local_diff = SortedVec::from([
            VfsNodeUpdate::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodeUpdate::DirRemoved(VirtualPathBuf::new("/a/b").unwrap()),
        ]);

        let remote_diff = SortedVec::from([
            VfsNodeUpdate::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodeUpdate::FileCreated(VirtualPathBuf::new("/a/b/test.log").unwrap()),
        ]);

        let reconciled = synchro.reconcile(local_diff, remote_diff).await.unwrap();

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
        let local_fs: ConcreteTestNode = local_base.clone().into();

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
        let remote_fs: ConcreteTestNode = remote_base.into();

        let mut synchro = Synchro::new(local_fs, remote_fs);
        synchro.update_vfs().await.unwrap();

        let local_diff =
            SortedVec::from([VfsNodeUpdate::DirCreated(VirtualPathBuf::new("/").unwrap())]);

        let remote_diff = local_diff.clone();

        let reconciled = synchro.reconcile(local_diff, remote_diff).await.unwrap();

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

    #[tokio::test]
    async fn test_full_sync() {
        let local_base = D("", vec![]);
        let local_fs: ConcreteTestNode = local_base.clone().into();

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                FF("file.doc", b"content"),
            ],
        );

        let remote_fs: ConcreteTestNode = remote_base.into();

        let mut synchro = Synchro::new(local_fs, remote_fs);

        synchro.full_sync().await.unwrap();

        assert!(synchro.local.vfs().structural_eq(synchro.remote.vfs()));

        // TODO: add tests with conflicts
        // TODO: add tests where fs are modified
    }
}
