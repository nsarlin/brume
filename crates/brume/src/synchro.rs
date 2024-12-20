//! Link two [`FileSystem`] for bidirectional synchronization

use futures::TryFutureExt;
use tokio::try_join;

use crate::{
    concrete::{ConcreteFS, ConcreteUpdateApplicationError},
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
pub struct Synchro<'local, 'remote, LocalConcrete: ConcreteFS, RemoteConcrete: ConcreteFS> {
    local: &'local mut FileSystem<LocalConcrete>,
    remote: &'remote mut FileSystem<RemoteConcrete>,
}

impl<'local, 'remote, LocalConcrete: ConcreteFS, RemoteConcrete: ConcreteFS>
    Synchro<'local, 'remote, LocalConcrete, RemoteConcrete>
{
    pub fn new(
        local: &'local mut FileSystem<LocalConcrete>,
        remote: &'remote mut FileSystem<RemoteConcrete>,
    ) -> Self {
        Self { local, remote }
    }

    pub fn local(&self) -> &FileSystem<LocalConcrete> {
        self.local
    }

    pub fn remote(&self) -> &FileSystem<RemoteConcrete> {
        self.remote
    }

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
    pub async fn full_sync(&mut self) -> Result<(), Error> {
        let (local_diff, remote_diff) = self.update_vfs().await?;

        let reconciled = self.reconcile(local_diff, remote_diff).await?;

        let (local_applied, remote_applied) = self
            .apply_updates_list_concrete(reconciled)
            .await
            .map_err(|(e, name)| Error::concrete_application(name, e))?;

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
        (ConcreteUpdateApplicationError, &'static str),
    > {
        self.local
            .apply_updates_list_concrete(self.remote, updates)
            .await
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

        synchro.full_sync().await.unwrap();

        assert!(synchro
            .local
            .vfs()
            .diff(synchro.remote.vfs())
            .unwrap()
            .is_empty());
    }

    // TODO: add tests with conflicts
    // TODO: add tests where fs are modified

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
        synchro.full_sync().await.unwrap();
        synchro.full_sync().await.unwrap();

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
            .set_concrete(ConcreteTestNode::from(remote_fixed.clone()));

        synchro.full_sync().await.unwrap();

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
        synchro.full_sync().await.unwrap();

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
            .set_concrete(ConcreteTestNode::from(remote_error.clone()));

        synchro.full_sync().await.unwrap();

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
            .set_concrete(ConcreteTestNode::from(remote_fixed.clone()));

        synchro.full_sync().await.unwrap();

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
        synchro.full_sync().await.unwrap();

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
            .set_concrete(ConcreteTestNode::from(remote_fixed.clone()));

        let mut synchro = Synchro::new(&mut local_fs, &mut remote_fs);

        synchro.full_sync().await.unwrap();

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
        synchro.full_sync().await.unwrap();

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
            .set_concrete(ConcreteTestNode::from(remote_error.clone()));

        synchro.full_sync().await.unwrap();

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
            .set_concrete(ConcreteTestNode::from(remote_fixed.clone()));

        synchro.full_sync().await.unwrap();

        assert!(synchro
            .local
            .vfs()
            .diff(synchro.remote.vfs())
            .unwrap()
            .is_empty());
    }

    // TODO: test file in error then modified on the other side
}
