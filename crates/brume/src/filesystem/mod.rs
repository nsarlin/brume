//! File system manipulation

mod byte_counter;

use byte_counter::ByteCounterExt;
use log::{error, info, warn};

use std::sync::{atomic::AtomicU64, Arc};

use futures::{future::try_join_all, TryFutureExt};
use tokio::try_join;

use crate::{
    concrete::{ConcreteFS, ConcreteFsError, ConcreteUpdateApplicationError},
    sorted_vec::SortedVec,
    update::{
        AppliedDirCreation, AppliedFileUpdate, AppliedUpdate, DiffError, FailedUpdateApplication,
        ReconciledUpdate, UpdateKind, UpdateTarget, VfsNodeUpdate, VfsUpdateList,
    },
    vfs::{DirTree, FileMeta, InvalidPathError, Vfs, VfsNode, VirtualPath},
    Error,
};

#[derive(Error, Debug)]
pub enum VfsReloadError {
    #[error("error from the concrete fs")]
    ConcreteFsError(#[from] ConcreteFsError),
    #[error("failed to diff newer and previous version of filesystem")]
    DiffError(#[from] DiffError),
}

/// Main representation of any kind of filesystem, remote or local.
///
/// It is composed of a generic Concrete FS backend that used for file operations, and a Virtual FS
/// that is its in-memory representation, using a tree-structure.
#[derive(Debug)]
pub struct FileSystem<Concrete: ConcreteFS> {
    concrete: Concrete,
    vfs: Vfs<Concrete::SyncInfo>,
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
    fn new_mkdir_failed<E: Into<ConcreteFsError>>(path: &VirtualPath, error: E) -> Self {
        Self {
            success: None,
            failures: vec![FailedUpdateApplication::new(
                VfsNodeUpdate::dir_created(path.to_owned()),
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

    /// Returned the nodes that failed to be cloned
    pub fn take_failures(&mut self) -> Vec<FailedUpdateApplication> {
        std::mem::take(&mut self.failures)
    }
}

impl<Concrete: ConcreteFS> FileSystem<Concrete> {
    pub fn new(concrete: Concrete) -> Self {
        Self {
            concrete,
            vfs: Vfs::empty(),
        }
    }

    #[cfg(test)]
    pub(crate) fn set_concrete(&mut self, concrete: Concrete) {
        self.concrete = concrete
    }

    /// Query the concrete FS to update the in memory virtual representation
    pub async fn update_vfs(&mut self) -> Result<VfsUpdateList, VfsReloadError> {
        info!("Updating VFS from {}", Concrete::TYPE_NAME);
        let new_vfs = self.concrete.load_virtual().await.map_err(|e| e.into())?;

        let updates = self.vfs.diff(&new_vfs)?;

        self.vfs = new_vfs;
        info!("VFS from {} updated", Concrete::TYPE_NAME);
        Ok(updates)
    }

    /// Return the virtual representation of this FileSystem
    pub fn vfs(&self) -> &Vfs<Concrete::SyncInfo> {
        &self.vfs
    }

    /// Return a mutable ref to the virtual representation of this FileSystem
    pub fn vfs_mut(&mut self) -> &mut Vfs<Concrete::SyncInfo> {
        &mut self.vfs
    }

    /// Return the concrete filesystem
    pub fn concrete(&self) -> &Concrete {
        &self.concrete
    }

    /// Return the dir at the given path, or an Error if the path is not found or not a directory
    pub fn find_dir(
        &self,
        path: &VirtualPath,
    ) -> Result<FileSystemDir<'_, Concrete>, InvalidPathError> {
        Ok(FileSystemDir {
            concrete: &self.concrete,
            dir: self.vfs.root().find_dir(path)?,
        })
    }

    /// Apply a list of updates on the two provided concrete FS.
    ///
    /// The target of the update will be chosen based on the value of the [`UpdateTarget`] of the
    /// [`ApplicableUpdate`]. Conflict files will always be created on the FileSystem in `self`.
    ///
    /// In case of error, return the underlying error and the name of the filesystem where this
    /// error occurred.
    ///
    /// [`ApplicableUpdate`]: crate::update::ApplicableUpdate
    pub async fn apply_updates_list_concrete<OtherConcrete: ConcreteFS>(
        &self,
        other_fs: &FileSystem<OtherConcrete>,
        updates: SortedVec<ReconciledUpdate>,
    ) -> Result<
        (
            Vec<AppliedUpdate<Concrete::SyncInfo>>,
            Vec<AppliedUpdate<OtherConcrete::SyncInfo>>,
        ),
        (ConcreteUpdateApplicationError, &'static str),
    > {
        let mut applicables = Vec::new();
        let mut conflicts = Vec::new();
        for update in updates {
            match update {
                ReconciledUpdate::Applicable(applicable) => applicables.push(applicable),
                ReconciledUpdate::Conflict(update) => {
                    warn!("conflict on {update:?}");
                    // Don't push removal updates since the node will not exist anymore in the
                    // source Vfs
                    if !update.is_removal() {
                        conflicts.push(update)
                    }
                }
            }
        }

        let (self_updates, other_updates): (Vec<_>, Vec<_>) =
            applicables
                .into_iter()
                .partition(|update| match update.target() {
                    UpdateTarget::SelfFs => true,
                    UpdateTarget::OtherFs => false,
                });

        let (self_conflicts, other_conflicts): (Vec<_>, Vec<_>) =
            conflicts
                .into_iter()
                .partition(|update| match update.target() {
                    UpdateTarget::SelfFs => true,
                    UpdateTarget::OtherFs => false,
                });

        let self_futures = try_join_all(
            self_updates
                .into_iter()
                .map(|update| self.apply_update_concrete(other_fs, update.clone().into())),
        );

        let other_futures = try_join_all(
            other_updates
                .into_iter()
                .map(|update| other_fs.apply_update_concrete(self, update.clone().into())),
        );

        let (self_applied, other_applied) = try_join!(
            self_futures.map_err(|e| (e, Concrete::TYPE_NAME)),
            other_futures.map_err(|e| (e, OtherConcrete::TYPE_NAME))
        )?;

        // Errors are applied on the source Vfs, to make them trigger a resync, so we need to move
        // them from one to the other
        let mut self_res = Vec::new();
        let mut self_failed = Vec::new();
        for applied in self_applied.into_iter().flatten() {
            match applied {
                AppliedUpdate::FailedApplication(failed_update) => {
                    self_failed.push(AppliedUpdate::FailedApplication(failed_update))
                }
                _ => self_res.push(applied),
            }
        }

        let mut other_res = Vec::new();
        let mut other_failed = Vec::new();
        for applied in other_applied.into_iter().flatten() {
            match applied {
                AppliedUpdate::FailedApplication(failed_update) => {
                    other_failed.push(AppliedUpdate::FailedApplication(failed_update))
                }
                _ => other_res.push(applied),
            }
        }

        self_res.extend(other_failed);
        self_res.extend(
            self_conflicts
                .clone()
                .into_iter()
                .map(|update| AppliedUpdate::Conflict(update.update().clone())),
        );
        other_res.extend(self_failed);
        other_res.extend(
            other_conflicts
                .clone()
                .into_iter()
                .map(|update| AppliedUpdate::Conflict(update.update().clone())),
        );

        Ok((self_res, other_res))
    }

    /// Apply an update on the concrete fs.
    ///
    /// The file contents will be taken from `ref_fs`
    // TODO: handle if ref_fs is not sync ?
    // TODO: check for "last minute" changes in target fs
    pub async fn apply_update_concrete<OtherConcrete: ConcreteFS>(
        &self,
        ref_fs: &FileSystem<OtherConcrete>,
        update: VfsNodeUpdate,
    ) -> Result<Vec<AppliedUpdate<Concrete::SyncInfo>>, ConcreteUpdateApplicationError> {
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
                info!("Removing dir {:?} from {}", path, Concrete::TYPE_NAME);

                self.concrete
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
                self.clone_file_concrete(&ref_fs.concrete, path)
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
                self.clone_file_concrete(&ref_fs.concrete, path)
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
                info!("Removing file {:?} from {}", path, Concrete::TYPE_NAME);

                self.concrete
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

    /// Clone a file from `ref_concrete` into the concrete fs of self.
    ///
    /// Return the syncinfo associated with the created file and the number of bytes written.
    pub async fn clone_file_concrete<OtherConcrete: ConcreteFS>(
        &self,
        ref_concrete: &OtherConcrete,
        path: &VirtualPath,
    ) -> Result<(Concrete::SyncInfo, u64), ConcreteFsError> {
        info!(
            "Cloning file {:?} from {} to {}",
            path,
            OtherConcrete::TYPE_NAME,
            Concrete::TYPE_NAME
        );
        let counter = Arc::new(AtomicU64::new(0));
        let stream = ref_concrete
            .read_file(path)
            .await
            .map_err(|e| {
                error!("Failed to read file {path:?}: {e:?}");
                e.into()
            })?
            .count_bytes(counter.clone());
        let res = self
            .concrete
            .write_file(path, stream)
            .await
            .map_err(|e| {
                error!("Failed to clone file {path:?}: {e:?}");
                e.into()
            })
            .map(|info| (info, counter.load(std::sync::atomic::Ordering::SeqCst)))?;

        info!("File {path:?} successfully cloned");

        Ok(res)
    }

    /// Clone a directory from `ref_concrete` into the concrete fs of self.
    pub async fn clone_dir_concrete<'otherfs, OtherConcrete: ConcreteFS>(
        &self,
        ref_fs: FileSystemDir<'otherfs, OtherConcrete>,
        path: &VirtualPath,
    ) -> ConcreteDirCloneResult<Concrete::SyncInfo> {
        let sync_info = match self.concrete.mkdir(path).await {
            Ok(dir_info) => dir_info,
            Err(err) => {
                error!("Failed to create dir {path:?}: {err:?}");
                return ConcreteDirCloneResult::new_mkdir_failed(path, err);
            }
        };

        let mut dir = DirTree::new(path.name(), sync_info);

        let mut errors = Vec::new();

        // TODO: parallelize file download
        for ref_child in ref_fs.dir.children().iter() {
            let mut child_path = path.to_owned();
            child_path.push(ref_child.name());
            match ref_child {
                VfsNode::Dir(ref_dir) => {
                    let mut result = Box::pin(self.clone_dir_concrete(
                        FileSystemDir::new(ref_fs.concrete, ref_dir),
                        &child_path,
                    ))
                    .await;

                    if let Some(success) = result.take_success() {
                        dir.insert_child(VfsNode::Dir(success));
                    }

                    errors.extend(result.take_failures())
                }
                VfsNode::File(_) => {
                    match self.clone_file_concrete(ref_fs.concrete, &child_path).await {
                        Ok((sync_info, size)) => {
                            let node =
                                VfsNode::File(FileMeta::new(child_path.name(), size, sync_info));
                            dir.insert_child(node);
                        }
                        Err(error) => {
                            let failure = FailedUpdateApplication::new(
                                VfsNodeUpdate::file_created(child_path),
                                error,
                            );
                            errors.push(failure)
                        }
                    }
                }
            };
        }

        ConcreteDirCloneResult::new(dir, errors)
    }
}

/// A directory in the [`FileSystem`]
pub struct FileSystemDir<'fs, Concrete: ConcreteFS> {
    concrete: &'fs Concrete,
    dir: &'fs DirTree<Concrete::SyncInfo>,
}

impl<'fs, Concrete: ConcreteFS> FileSystemDir<'fs, Concrete> {
    pub fn new(concrete: &'fs Concrete, dir: &'fs DirTree<Concrete::SyncInfo>) -> Self {
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
            .apply_update_concrete(&remote_fs, VfsNodeUpdate::dir_created(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.first().unwrap().path(), dir_path);
        assert_eq!(local_fs.concrete, remote_fs.concrete);

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
            .apply_update_concrete(&remote_fs, VfsNodeUpdate::dir_removed(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.first().unwrap().path(), dir_path);
        assert_eq!(local_fs.concrete, remote_fs.concrete);

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
            .apply_update_concrete(&remote_fs, VfsNodeUpdate::file_created(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.first().unwrap().path(), dir_path);
        assert_eq!(local_fs.concrete, remote_fs.concrete);

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
            .apply_update_concrete(&remote_fs, VfsNodeUpdate::file_modified(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.first().unwrap().path(), dir_path);
        assert_eq!(local_fs.concrete, remote_fs.concrete);

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
            .apply_update_concrete(&remote_fs, VfsNodeUpdate::file_removed(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.first().unwrap().path(), dir_path);
        assert_eq!(local_fs.concrete, remote_fs.concrete);
    }
}
