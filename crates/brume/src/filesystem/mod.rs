//! File system manipulation

use log::{debug, error, info};

use futures::future::join_all;

use crate::{
    concrete::{ConcreteFS, ConcreteUpdateApplicationError, FSBackend, FsBackendError},
    sorted_vec::SortedVec,
    update::{
        AppliedDirCreation, AppliedFileUpdate, AppliedUpdate, DiffError, FailedUpdateApplication,
        UpdateKind, VfsDiff, VfsDiffList, VfsUpdate, VfsUpdateApplicationError,
    },
    vfs::{DirTree, FileMeta, InvalidPathError, Vfs, VfsNode, VirtualPath},
    Error,
};

#[derive(Error, Debug)]
pub enum VfsReloadError {
    #[error("error from the concrete fs")]
    FsBackendError(#[from] FsBackendError),
    #[error("failed to diff newer and previous version of filesystem")]
    DiffError(#[from] DiffError),
}

/// Main representation of any kind of filesystem, remote or local.
///
/// It is composed of a generic [`ConcreteFS`] backend that used for file operations, and a
/// [`Virtual FS`] that is its in-memory representation, using a tree-structure.
///
/// The [`Virtual FS`] is actually stored twice:
/// - The **loaded** VFS matches exactly the structure of the ConcreteFS. It is updated after every
///   call to [`diff_vfs`].
/// - The **status** VFS holds the status of each nodes. It is allowed to diverge from the
///   ConcreteFS, for example if a node has been removed but the removal has not been propagated
///   because of a [`Conflict`] or an [`Error`].
///
/// [`Virtual FS`]: Vfs
/// [`diff_vfs`]: Self::diff_vfs
/// [`Conflict`]: crate::vfs::dir_tree::NodeState::Conflict
/// [`Error`]: crate::vfs::dir_tree::NodeState::Error
#[derive(Debug)]
pub struct FileSystem<Backend: FSBackend> {
    concrete: ConcreteFS<Backend>,
    status_vfs: Vfs<Backend::SyncInfo>,
    loaded_vfs: Vfs<Backend::SyncInfo>,
}

/// Returned by [`FileSystem::clone_dir_concrete`]
///
/// This type holds the list of nodes that were successfully cloned and the one that resulted in an
/// error.
pub struct ConcreteDirCloneResult<SrcSyncInfo, DstSyncInfo> {
    success: Option<(DirTree<SrcSyncInfo>, DirTree<DstSyncInfo>)>,
    failures: Vec<FailedUpdateApplication>,
}

impl<SrcSyncInfo, DstSyncInfo> ConcreteDirCloneResult<SrcSyncInfo, DstSyncInfo> {
    /// Create a new Clone result for a mkdir that failed
    fn new_mkdir_failed<E: Into<FsBackendError>>(path: &VirtualPath, error: E) -> Self {
        Self {
            success: None,
            failures: vec![FailedUpdateApplication::new(
                VfsDiff::dir_created(path.to_owned()),
                error.into(),
            )],
        }
    }

    fn new(
        src_success: DirTree<SrcSyncInfo>,
        dst_success: DirTree<DstSyncInfo>,
        failures: Vec<FailedUpdateApplication>,
    ) -> Self {
        Self {
            success: Some((src_success, dst_success)),
            failures,
        }
    }

    /// Returns the successfully cloned nodes, if any, sorted in a [`DirTree`]
    pub fn take_success(&mut self) -> Option<(DirTree<SrcSyncInfo>, DirTree<DstSyncInfo>)> {
        self.success.take()
    }

    /// Returns the nodes that failed to be cloned
    pub fn take_failures(&mut self) -> Vec<FailedUpdateApplication> {
        std::mem::take(&mut self.failures)
    }
}

impl<Backend: FSBackend> FileSystem<Backend> {
    pub fn new(backend: Backend) -> Self {
        Self {
            concrete: ConcreteFS::new(backend),
            status_vfs: Vfs::empty(),
            loaded_vfs: Vfs::empty(),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_backend(&mut self, backend: Backend) {
        self.concrete = ConcreteFS::new(backend)
    }

    /// Loads the [`Vfs`] from the [`ConcreteFS`], compares it with the currently recorded version
    /// and returns the differences
    pub async fn diff_vfs(&mut self) -> Result<VfsDiffList, VfsReloadError> {
        debug!("Updating VFS from {}", Backend::TYPE_NAME);
        let new_vfs = self.backend().load_virtual().await.map_err(|e| e.into())?;

        let updates = self.status_vfs.diff(&new_vfs)?;

        self.loaded_vfs = new_vfs;
        debug!("VFS from {} updated", Backend::TYPE_NAME);
        Ok(updates)
    }

    /// Returns the virtual view of this FileSystem.
    ///
    /// This view also holds the information about nodes that are in error or conflict.
    pub fn vfs(&self) -> &Vfs<Backend::SyncInfo> {
        &self.status_vfs
    }

    /// Returns the virtual view of this FileSystem.
    ///
    /// This representation matches exactly the state of the [`FSBackend`], but has no information
    /// about errors and conflicts.
    pub fn loaded_vfs(&self) -> &Vfs<Backend::SyncInfo> {
        &self.loaded_vfs
    }

    /// Returns a mutable ref to the virtual representation of this FileSystem
    pub fn vfs_mut(&mut self) -> &mut Vfs<Backend::SyncInfo> {
        &mut self.status_vfs
    }

    /// Returns the concrete filesystem
    pub fn concrete(&self) -> &ConcreteFS<Backend> {
        &self.concrete
    }

    /// Returns a direct access to the backend behind the concrete filesystem
    fn backend(&self) -> &Backend {
        self.concrete.backend()
    }

    /// Returns the dir at the given path in the "loaded" vfs, or an Error if the path is not found
    /// or not a directory
    pub fn find_loaded_dir(
        &self,
        path: &VirtualPath,
    ) -> Result<FileSystemDir<'_, Backend>, InvalidPathError> {
        Ok(FileSystemDir {
            concrete: &self.concrete,
            dir: self.loaded_vfs.find_dir(path)?,
        })
    }

    /// Apply an update on the concrete fs.
    ///
    /// The file contents will be taken from `ref_fs`
    // TODO: handle if ref_fs is not sync ?
    // TODO: check for "last minute" changes in target fs
    pub async fn apply_update_concrete<RefBackend: FSBackend>(
        &self,
        ref_fs: &FileSystem<RefBackend>,
        update: VfsDiff,
    ) -> Result<
        Vec<AppliedUpdate<RefBackend::SyncInfo, Backend::SyncInfo>>,
        ConcreteUpdateApplicationError,
    > {
        if update.path().is_root() {
            return Err(ConcreteUpdateApplicationError::PathIsRoot);
        }

        match update.kind() {
            UpdateKind::DirCreated => {
                let path = update.path();
                let dir = ref_fs.find_loaded_dir(path)?;

                let mut res = self.clone_dir_concrete(dir, path).await;

                let mut updates: Vec<_> = res
                    .take_success()
                    .map(|(src_dir, dst_dir)| {
                        AppliedUpdate::DirCreated(AppliedDirCreation::new(
                            // The update path is not root so we can unwrap
                            path.parent().unwrap(),
                            src_dir,
                            dst_dir,
                        ))
                    })
                    .into_iter()
                    .collect();

                updates.extend(
                    res.take_failures()
                        .into_iter()
                        .map(AppliedUpdate::FailedApplication),
                );
                Ok(updates)
            }
            UpdateKind::DirRemoved => {
                let path = update.path();
                info!("Removing dir {:?} from {}", path, Backend::TYPE_NAME);

                // If the update is a removal of a node that as never been created because of an
                // error, we can skip it
                if let Some(node) = ref_fs.vfs().find_node(update.path()) {
                    if node.can_skip_removal() {
                        return Ok(vec![AppliedUpdate::DirRemovedSkipped(path.to_owned())]);
                    }
                }

                self.concrete
                    .backend()
                    .rmdir(path)
                    .await
                    .map(|_| AppliedUpdate::DirRemoved(path.to_owned()))
                    .or_else(|e| {
                        Ok(AppliedUpdate::FailedApplication(
                            FailedUpdateApplication::new(update, e.into()),
                        ))
                    })
                    .map(|update| vec![update])
            }
            UpdateKind::FileCreated => {
                let path = update.path();
                self.concrete
                    .clone_file(&ref_fs.concrete, path)
                    .await
                    .map(|clone_result| {
                        AppliedUpdate::FileCreated(AppliedFileUpdate::from_clone_result(
                            path,
                            clone_result,
                        ))
                    })
                    .or_else(|e| {
                        Ok(AppliedUpdate::FailedApplication(
                            FailedUpdateApplication::new(update, e),
                        ))
                    })
                    .map(|update| vec![update])
            }
            UpdateKind::FileModified => {
                let path = update.path();
                self.concrete
                    .clone_file(&ref_fs.concrete, path)
                    .await
                    .map(|clone_result| {
                        AppliedUpdate::FileModified(AppliedFileUpdate::from_clone_result(
                            path,
                            clone_result,
                        ))
                    })
                    .or_else(|e| {
                        Ok(AppliedUpdate::FailedApplication(
                            FailedUpdateApplication::new(update, e),
                        ))
                    })
                    .map(|update| vec![update])
            }
            UpdateKind::FileRemoved => {
                let path = update.path();
                info!("Removing file {:?} from {}", path, Backend::TYPE_NAME);

                // If the update is a removal of a node that as never been created because of an
                // error, we can skip it
                if let Some(node) = ref_fs.vfs().find_node(update.path()) {
                    if node.can_skip_removal() {
                        return Ok(vec![AppliedUpdate::FileRemovedSkipped(path.to_owned())]);
                    }
                }

                self.backend()
                    .rm(path)
                    .await
                    .map(|_| AppliedUpdate::FileRemoved(path.to_owned()))
                    .or_else(|e| {
                        Ok(AppliedUpdate::FailedApplication(
                            FailedUpdateApplication::new(update, e.into()),
                        ))
                    })
                    .map(|update| vec![update])
            }
        }
    }

    /// Clone a directory from `ref_concrete` into the concrete fs of self.
    pub async fn clone_dir_concrete<RefBackend: FSBackend>(
        &self,
        ref_fs: FileSystemDir<'_, RefBackend>,
        path: &VirtualPath,
    ) -> ConcreteDirCloneResult<RefBackend::SyncInfo, Backend::SyncInfo> {
        let src_info = match ref_fs.concrete.backend().get_sync_info(path).await {
            Ok(dir_info) => dir_info,
            Err(err) => {
                error!("Failed to create dir {path:?}: {err:?}");
                return ConcreteDirCloneResult::new_mkdir_failed(path, err);
            }
        };

        let dst_info = match self.backend().mkdir(path).await {
            Ok(dir_info) => dir_info,
            Err(err) => {
                error!("Failed to create dir {path:?}: {err:?}");
                return ConcreteDirCloneResult::new_mkdir_failed(path, err);
            }
        };

        let mut dst_dir = DirTree::new(path.name(), dst_info);
        let mut src_dir = DirTree::new(path.name(), src_info);
        let mut errors = Vec::new();

        let futures: Vec<_> = ref_fs
            .dir
            .children()
            .iter()
            .map(|ref_child| async move {
                let mut child_path = path.to_owned();
                child_path.push(ref_child.name());
                match ref_child {
                    VfsNode::Dir(ref_dir) => {
                        let mut result = Box::pin(self.clone_dir_concrete(
                            FileSystemDir::new(ref_fs.concrete, ref_dir),
                            &child_path,
                        ))
                        .await;

                        let success = result.take_success().map(|(src_dir, dst_dir)| {
                            (VfsNode::Dir(src_dir), VfsNode::Dir(dst_dir))
                        });
                        let failures = result.take_failures();
                        (success, failures)
                    }
                    VfsNode::File(_) => {
                        match self.concrete.clone_file(ref_fs.concrete, &child_path).await {
                            Ok(clone_result) => {
                                let size = clone_result.file_size();
                                let (src_info, dst_info) = clone_result.into();
                                let src_node =
                                    VfsNode::File(FileMeta::new(child_path.name(), size, src_info));
                                let dst_node =
                                    VfsNode::File(FileMeta::new(child_path.name(), size, dst_info));
                                (Some((src_node, dst_node)), Vec::new())
                            }
                            Err(error) => {
                                let failure = FailedUpdateApplication::new(
                                    VfsDiff::file_created(child_path),
                                    error,
                                );
                                (None, vec![failure])
                            }
                        }
                    }
                }
            })
            .collect();

        let results = join_all(futures).await;

        for (success, failures) in results {
            if let Some((src_node, dst_node)) = success {
                dst_dir.insert_child(dst_node);
                src_dir.insert_child(src_node);
            }

            errors.extend(failures);
        }

        ConcreteDirCloneResult::new(src_dir, dst_dir, errors)
    }

    /// Applies a list of updates to the [`Vfs`], by calling [`Self::apply_update_vfs`] on each of
    /// them.
    pub fn apply_updates_list_vfs(
        &mut self,
        updates: &SortedVec<VfsUpdate<Backend::SyncInfo>>,
    ) -> Result<(), VfsUpdateApplicationError> {
        for update in updates.iter() {
            self.apply_update_vfs(update)?;
        }
        Ok(())
    }

    /// Applies an update to the [`Vfs`] of this filesystem.
    pub fn apply_update_vfs(
        &mut self,
        update: &VfsUpdate<Backend::SyncInfo>,
    ) -> Result<(), VfsUpdateApplicationError> {
        self.status_vfs.apply_update(update, &self.loaded_vfs)
    }
}

/// A directory in the [`FileSystem`]
pub struct FileSystemDir<'fs, Backend: FSBackend> {
    concrete: &'fs ConcreteFS<Backend>,
    dir: &'fs DirTree<Backend::SyncInfo>,
}

impl<'fs, Backend: FSBackend> FileSystemDir<'fs, Backend> {
    pub fn new(concrete: &'fs ConcreteFS<Backend>, dir: &'fs DirTree<Backend::SyncInfo>) -> Self {
        Self { concrete, dir }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        test_utils::{
            TestFileSystem,
            TestNode::{D, FF},
        },
        vfs::VirtualPathBuf,
    };

    #[tokio::test]
    async fn test_concrete_apply() {
        let local_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );
        let mut local_fs = TestFileSystem::new(local_base.clone().into());
        local_fs.diff_vfs().await.unwrap();

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D(
                    "e",
                    vec![D(
                        "g",
                        vec![
                            FF("tmp.txt", b"content"),
                            D("h", vec![FF("file.bin", b"bin"), D("i", vec![])]),
                        ],
                    )],
                ),
            ],
        );

        let mut remote_fs = TestFileSystem::new(remote_base.into());
        remote_fs.diff_vfs().await.unwrap();

        let dir_path = VirtualPathBuf::new("/e/g/h").unwrap();
        let applied = local_fs
            .apply_update_concrete(&remote_fs, VfsDiff::dir_created(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.first().unwrap().path(), dir_path);
        assert_eq!(local_fs.backend(), remote_fs.backend());

        let mut local_fs = TestFileSystem::new(local_base.clone().into());
        local_fs.diff_vfs().await.unwrap();

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );

        let mut remote_fs = TestFileSystem::new(remote_base.into());
        remote_fs.diff_vfs().await.unwrap();

        let dir_path = VirtualPathBuf::new("/a/b").unwrap();
        let applied = local_fs
            .apply_update_concrete(&remote_fs, VfsDiff::dir_removed(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.first().unwrap().path(), dir_path);
        assert_eq!(local_fs.backend(), remote_fs.backend());

        let mut local_fs = TestFileSystem::new(local_base.clone().into());
        local_fs.diff_vfs().await.unwrap();

        let remote_base = D(
            "",
            vec![
                D(
                    "Doc",
                    vec![
                        FF("f1.md", b"hello"),
                        FF("f2.pdf", b"world"),
                        FF("f3.rs", b"pub fn code(){}"),
                    ],
                ),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );

        let mut remote_fs = TestFileSystem::new(remote_base.into());
        remote_fs.diff_vfs().await.unwrap();

        let dir_path = VirtualPathBuf::new("/Doc/f3.rs").unwrap();
        let applied = local_fs
            .apply_update_concrete(&remote_fs, VfsDiff::file_created(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.first().unwrap().path(), dir_path);
        assert_eq!(local_fs.backend(), remote_fs.backend());

        let mut local_fs = TestFileSystem::new(local_base.clone().into());
        local_fs.diff_vfs().await.unwrap();

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"bye"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );

        let mut remote_fs = TestFileSystem::new(remote_base.into());
        remote_fs.diff_vfs().await.unwrap();

        let dir_path = VirtualPathBuf::new("/Doc/f1.md").unwrap();
        let applied = local_fs
            .apply_update_concrete(&remote_fs, VfsDiff::file_modified(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.first().unwrap().path(), dir_path);
        assert_eq!(local_fs.backend(), remote_fs.backend());

        let mut local_fs = TestFileSystem::new(local_base.clone().into());
        local_fs.diff_vfs().await.unwrap();

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );

        let mut remote_fs = TestFileSystem::new(remote_base.into());
        remote_fs.diff_vfs().await.unwrap();

        let dir_path = VirtualPathBuf::new("/Doc/f2.pdf").unwrap();
        let applied = local_fs
            .apply_update_concrete(&remote_fs, VfsDiff::file_removed(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.first().unwrap().path(), dir_path);
        assert_eq!(local_fs.backend(), remote_fs.backend());
    }
}
