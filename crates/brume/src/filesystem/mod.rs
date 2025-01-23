//! File system manipulation

use log::{debug, error, info};

use futures::future::join_all;

use crate::{
    concrete::{ConcreteFS, ConcreteUpdateApplicationError, FSBackend, FsBackendError},
    update::{
        AppliedDirCreation, AppliedFileUpdate, AppliedUpdate, DiffError, FailedUpdateApplication,
        UpdateKind, VfsDiff, VfsDiffList,
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
/// [`Concrete FS`]: FSBackend
/// [`Virtual FS`]: Vfs
#[derive(Debug)]
pub struct FileSystem<Backend: FSBackend> {
    concrete: ConcreteFS<Backend>,
    vfs: Vfs<Backend::SyncInfo>,
}

/// Returned by [`FileSystem::clone_dir_concrete`]
///
/// This type holds the list of nodes that were successfully cloned and the one that resulted in an
/// error.
pub struct ConcreteDirCloneResult<SyncInfo> {
    success: Option<DirTree<SyncInfo>>,
    failures: Vec<FailedUpdateApplication>,
}

impl<SyncInfo> ConcreteDirCloneResult<SyncInfo> {
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

    fn new(success: DirTree<SyncInfo>, failures: Vec<FailedUpdateApplication>) -> Self {
        Self {
            success: Some(success),
            failures,
        }
    }

    /// Returns the successfully cloned nodes, if any, sorted in a [`DirTree`]
    pub fn take_success(&mut self) -> Option<DirTree<SyncInfo>> {
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
            vfs: Vfs::empty(),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_backend(&mut self, backend: Backend) {
        self.concrete = ConcreteFS::new(backend)
    }

    /// Queries the concrete FS to update the in memory virtual representation
    pub async fn update_vfs(&mut self) -> Result<VfsDiffList, VfsReloadError> {
        debug!("Updating VFS from {}", Backend::TYPE_NAME);
        let new_vfs = self.backend().load_virtual().await.map_err(|e| e.into())?;

        let updates = self.vfs.diff(&new_vfs)?;

        self.vfs = new_vfs;
        debug!("VFS from {} updated", Backend::TYPE_NAME);
        Ok(updates)
    }

    /// Returns the virtual representation of this FileSystem
    pub fn vfs(&self) -> &Vfs<Backend::SyncInfo> {
        &self.vfs
    }

    /// Returns a mutable ref to the virtual representation of this FileSystem
    pub fn vfs_mut(&mut self) -> &mut Vfs<Backend::SyncInfo> {
        &mut self.vfs
    }

    /// Returns the concrete filesystem
    pub fn concrete(&self) -> &ConcreteFS<Backend> {
        &self.concrete
    }

    /// Returns a direct access to the backend behind the concrete filesystem
    fn backend(&self) -> &Backend {
        self.concrete.backend()
    }

    /// Return the dir at the given path, or an Error if the path is not found or not a directory
    pub fn find_dir(
        &self,
        path: &VirtualPath,
    ) -> Result<FileSystemDir<'_, Backend>, InvalidPathError> {
        Ok(FileSystemDir {
            concrete: &self.concrete,
            dir: self.vfs.find_dir(path)?,
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
    ) -> Result<Vec<AppliedUpdate<Backend::SyncInfo>>, ConcreteUpdateApplicationError> {
        if update.path().is_root() {
            return Err(ConcreteUpdateApplicationError::PathIsRoot);
        }

        match update.kind() {
            UpdateKind::DirCreated => {
                let path = update.path();
                let dir = ref_fs.find_dir(path)?;

                let mut res = self.clone_dir_concrete(dir, path).await;

                let mut updates: Vec<_> = res
                    .take_success()
                    .map(|created_dir| {
                        AppliedUpdate::DirCreated(AppliedDirCreation::new(
                            // The update path is not root so we can unwrap
                            path.parent().unwrap(),
                            created_dir,
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
                    .map(|(sync_info, size)| {
                        AppliedUpdate::FileCreated(AppliedFileUpdate::new(path, size, sync_info))
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
                    .map(|(sync_info, size)| {
                        AppliedUpdate::FileModified(AppliedFileUpdate::new(path, size, sync_info))
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
    pub async fn clone_dir_concrete<OtherBackend: FSBackend>(
        &self,
        ref_fs: FileSystemDir<'_, OtherBackend>,
        path: &VirtualPath,
    ) -> ConcreteDirCloneResult<Backend::SyncInfo> {
        let sync_info = match self.backend().mkdir(path).await {
            Ok(dir_info) => dir_info,
            Err(err) => {
                error!("Failed to create dir {path:?}: {err:?}");
                return ConcreteDirCloneResult::new_mkdir_failed(path, err);
            }
        };

        let mut dir = DirTree::new(path.name(), sync_info);
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

                        let success = result.take_success().map(VfsNode::Dir);
                        let failures = result.take_failures();
                        (success, failures)
                    }
                    VfsNode::File(_) => {
                        match self.concrete.clone_file(ref_fs.concrete, &child_path).await {
                            Ok((sync_info, size)) => {
                                let node = VfsNode::File(FileMeta::new(
                                    child_path.name(),
                                    size,
                                    sync_info,
                                ));
                                (Some(node), Vec::new())
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
            if let Some(node) = success {
                dir.insert_child(node);
            }

            errors.extend(failures);
        }

        ConcreteDirCloneResult::new(dir, errors)
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
        local_fs.update_vfs().await.unwrap();

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
        remote_fs.update_vfs().await.unwrap();

        let dir_path = VirtualPathBuf::new("/e/g/h").unwrap();
        let applied = local_fs
            .apply_update_concrete(&remote_fs, VfsDiff::dir_created(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.first().unwrap().path(), dir_path);
        assert_eq!(local_fs.backend(), remote_fs.backend());

        let mut local_fs = TestFileSystem::new(local_base.clone().into());
        local_fs.update_vfs().await.unwrap();

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );

        let mut remote_fs = TestFileSystem::new(remote_base.into());
        remote_fs.update_vfs().await.unwrap();

        let dir_path = VirtualPathBuf::new("/a/b").unwrap();
        let applied = local_fs
            .apply_update_concrete(&remote_fs, VfsDiff::dir_removed(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.first().unwrap().path(), dir_path);
        assert_eq!(local_fs.backend(), remote_fs.backend());

        let mut local_fs = TestFileSystem::new(local_base.clone().into());
        local_fs.update_vfs().await.unwrap();

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
        remote_fs.update_vfs().await.unwrap();

        let dir_path = VirtualPathBuf::new("/Doc/f3.rs").unwrap();
        let applied = local_fs
            .apply_update_concrete(&remote_fs, VfsDiff::file_created(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.first().unwrap().path(), dir_path);
        assert_eq!(local_fs.backend(), remote_fs.backend());

        let mut local_fs = TestFileSystem::new(local_base.clone().into());
        local_fs.update_vfs().await.unwrap();

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"bye"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );

        let mut remote_fs = TestFileSystem::new(remote_base.into());
        remote_fs.update_vfs().await.unwrap();

        let dir_path = VirtualPathBuf::new("/Doc/f1.md").unwrap();
        let applied = local_fs
            .apply_update_concrete(&remote_fs, VfsDiff::file_modified(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.first().unwrap().path(), dir_path);
        assert_eq!(local_fs.backend(), remote_fs.backend());

        let mut local_fs = TestFileSystem::new(local_base.clone().into());
        local_fs.update_vfs().await.unwrap();

        let remote_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );

        let mut remote_fs = TestFileSystem::new(remote_base.into());
        remote_fs.update_vfs().await.unwrap();

        let dir_path = VirtualPathBuf::new("/Doc/f2.pdf").unwrap();
        let applied = local_fs
            .apply_update_concrete(&remote_fs, VfsDiff::file_removed(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.first().unwrap().path(), dir_path);
        assert_eq!(local_fs.backend(), remote_fs.backend());
    }
}
