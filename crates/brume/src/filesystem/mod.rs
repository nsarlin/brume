//! File system manipulation

use thiserror::Error;
use tracing::{debug, error, instrument};

use crate::{
    concrete::{ConcreteFS, FSBackend, FsBackendError},
    sorted_vec::SortedVec,
    update::{
        DiffError, UpdateKind, VfsDiff, VfsDiffList, VfsDirCreation, VfsDirModification,
        VfsFileUpdate, VfsUpdate, VfsUpdateApplicationError,
    },
    vfs::{DirTree, InvalidPathError, NodeState, StatefulVfs, Vfs, VirtualPath, VirtualPathBuf},
};

#[derive(Error, Debug)]
pub enum VfsReloadError {
    #[error("error from the concrete fs")]
    FsBackendError(#[from] FsBackendError),
    #[error("failed to diff newer and previous version of filesystem")]
    DiffError(#[from] DiffError),
}

#[derive(Error, Debug)]
pub enum SkipUpdateError {
    #[error("invalid path provided for update")]
    InvalidPath(#[from] InvalidPathError),
    #[error("loaded vfs is missing")]
    MissingLoadedVfs,
    #[error("Tried to skip dir modification at {0}")]
    DirModificationSkipped(VirtualPathBuf),
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
    status_vfs: StatefulVfs<Backend::SyncInfo>,
    loaded_vfs: Option<Vfs<Backend::SyncInfo>>,
}

impl<Backend: FSBackend> FileSystem<Backend> {
    pub fn new(backend: Backend) -> Self {
        Self {
            concrete: ConcreteFS::new(backend),
            status_vfs: Vfs::empty(),
            loaded_vfs: None,
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn set_backend(&mut self, backend: Backend) {
        self.concrete = ConcreteFS::new(backend)
    }

    /// Loads the [`Vfs`] from the [`ConcreteFS`], compares it with the currently recorded version
    /// and returns the differences
    pub async fn diff_vfs(&mut self) -> Result<VfsDiffList, VfsReloadError> {
        debug!("Updating VFS from {}", Backend::TYPE_NAME);
        let new_vfs = self.backend().load_virtual().await.map_err(|e| e.into())?;

        // We retry any node in error, if they are still present in the vfs
        let errors = SortedVec::from_vec(
            self.status_vfs
                .get_errors()
                .iter()
                .filter(|failed_update| {
                    failed_update.update().is_removal()
                        || new_vfs.find_node(failed_update.path()).is_some()
                })
                .map(|failed_update| failed_update.update().clone())
                .collect(),
        );

        let updates = self.status_vfs.diff(&new_vfs)?;
        self.loaded_vfs = Some(new_vfs);

        debug!("VFS from {} updated", Backend::TYPE_NAME);
        Ok(updates.merge(errors))
    }

    /// Returns the virtual view of this FileSystem.
    ///
    /// This view also holds the information about nodes that are in error or conflict.
    pub fn vfs(&self) -> &StatefulVfs<Backend::SyncInfo> {
        &self.status_vfs
    }

    /// Returns the virtual view of this FileSystem.
    ///
    /// This representation matches exactly the state of the [`FSBackend`], but has no information
    /// about errors and conflicts.
    pub fn loaded_vfs(&self) -> Option<&Vfs<Backend::SyncInfo>> {
        self.loaded_vfs.as_ref()
    }

    #[cfg(test)]
    pub fn loaded_vfs_mut(&mut self) -> &mut Option<Vfs<Backend::SyncInfo>> {
        &mut self.loaded_vfs
    }

    /// Returns a mutable ref to the virtual representation of this FileSystem
    pub fn vfs_mut(&mut self) -> &mut StatefulVfs<Backend::SyncInfo> {
        &mut self.status_vfs
    }

    /// Returns the concrete filesystem
    pub fn concrete(&self) -> &ConcreteFS<Backend> {
        &self.concrete
    }

    /// Returns a direct access to the backend behind the concrete filesystem
    pub fn backend(&self) -> &Backend {
        self.concrete.backend()
    }

    /// Returns the dir at the given path in the "loaded" vfs, or an Error if the path is not found
    /// or not a directory
    pub fn find_loaded_dir(
        &self,
        path: &VirtualPath,
    ) -> Result<&DirTree<Backend::SyncInfo>, InvalidPathError> {
        self.loaded_vfs
            .as_ref()
            .map(|vfs| vfs.find_dir(path))
            .unwrap_or(Err(InvalidPathError::NotFound(path.to_owned())))
    }

    /// Applies a list of updates to the [`Vfs`], by calling [`Self::apply_update_vfs`] on each of
    /// them.
    #[instrument(skip_all, fields(fs = Backend::TYPE_NAME))]
    pub fn apply_updates_list_vfs(
        &mut self,
        updates: &SortedVec<VfsUpdate<Backend::SyncInfo>>,
    ) -> Result<(), VfsUpdateApplicationError> {
        for update in updates.iter() {
            self.apply_update_vfs(update)?;
        }
        Ok(())
    }

    /// Resets the [`Vfs`] to the empty state. All metadata will be lost
    pub fn reset_vfs(&mut self) {
        self.status_vfs = Vfs::empty();
        self.loaded_vfs = None;
    }

    /// Applies an update to the [`Vfs`] of this filesystem.
    #[instrument(skip_all, fields(fs = Backend::TYPE_NAME))]
    pub fn apply_update_vfs(
        &mut self,
        update: &VfsUpdate<Backend::SyncInfo>,
    ) -> Result<(), VfsUpdateApplicationError> {
        let ref_vfs = self
            .loaded_vfs
            .as_ref()
            .ok_or(VfsUpdateApplicationError::MissingReferenceVfs)?;
        self.status_vfs.apply_update(update, ref_vfs)
    }

    /// Sets the state of a node in the [`Vfs`] to `state`
    ///
    /// Returns an error if the path is not valid in the FS, or is the root node
    pub fn set_node_state_vfs(
        &mut self,
        path: &VirtualPath,
        state: NodeState<Backend::SyncInfo>,
    ) -> Result<(), VfsUpdateApplicationError> {
        self.status_vfs.update_node_state(path, state)
    }

    /// Fetches the sync info from the concrete FS and updates the [`Vfs`] accordingly
    ///
    /// Returns None if the node is not present in the Vfs, or an Error if it failed to access the
    /// concrete FS
    #[instrument(skip_all, fields(path = %path))]
    pub async fn reload_sync_info(
        &mut self,
        path: &VirtualPath,
    ) -> Option<NodeState<Backend::SyncInfo>> {
        // Skip if the node does not exist, meaning it has been removed so there is no state to
        // update
        if self.status_vfs.find_node(path).is_some() {
            let concrete = self.concrete();
            let state = match concrete.backend().get_node_info(path).await {
                Ok(info) => NodeState::Ok(info.into_metadata()),
                // If we can't reach the concrete FS, we delay until the next full_sync
                Err(_) => NodeState::NeedResync,
            };
            self.set_node_state_vfs(path, state.clone())
                .expect("Path should be valid here because we retrieved it above");

            Some(state)
        } else {
            None
        }
    }

    /// Skips the update by returning the [`VfsUpdate`] without actually applying it on the concrete
    /// dir.
    ///
    /// The metadata will be taken from the `loaded_vfs`
    pub fn skip_update(
        &self,
        update: VfsDiff,
    ) -> Result<VfsUpdate<Backend::SyncInfo>, SkipUpdateError> {
        let loaded = self
            .loaded_vfs
            .as_ref()
            .ok_or(SkipUpdateError::MissingLoadedVfs)?;
        let path = update.path();

        match update.kind() {
            UpdateKind::DirCreated => {
                let loaded_dir = loaded.find_dir(path)?.clone().into_ok();
                Ok(VfsUpdate::dir_created(
                    path,
                    VfsDirCreation::new(loaded_dir),
                ))
            }
            UpdateKind::DirRemoved => Ok(VfsUpdate::dir_removed(path)),
            UpdateKind::FileCreated => {
                let loaded_file = loaded.find_file(path)?.clone();
                Ok(VfsUpdate::file_created(
                    path,
                    VfsFileUpdate::new(
                        loaded_file.size(),
                        loaded_file.last_modified(),
                        loaded_file.into_metadata(),
                    ),
                ))
            }
            UpdateKind::FileRemoved => Ok(VfsUpdate::file_removed(path)),
            UpdateKind::FileModified => {
                let loaded_file = loaded.find_file(path)?.clone();
                Ok(VfsUpdate::file_modified(
                    path,
                    VfsFileUpdate::new(
                        loaded_file.size(),
                        loaded_file.last_modified(),
                        loaded_file.into_metadata(),
                    ),
                ))
            }
            UpdateKind::DirModified => {
                let loaded_dir = loaded.find_dir(path)?;
                Ok(VfsUpdate::dir_modified(
                    path,
                    VfsDirModification::new(
                        loaded_dir.last_modified(),
                        loaded_dir.metadata().clone(),
                    ),
                ))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        test_utils::{
            TestFileSystem,
            TestNode::{D, FF},
        },
        update::VfsDiff,
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
            .concrete()
            .apply_update(&remote_fs, VfsDiff::dir_created(dir_path.clone()))
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
            .concrete()
            .apply_update(&remote_fs, VfsDiff::dir_removed(dir_path.clone()))
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
            .concrete()
            .apply_update(&remote_fs, VfsDiff::file_created(dir_path.clone()))
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
            .concrete()
            .apply_update(&remote_fs, VfsDiff::file_modified(dir_path.clone()))
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
            .concrete()
            .apply_update(&remote_fs, VfsDiff::file_removed(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.first().unwrap().path(), dir_path);
        assert_eq!(local_fs.backend(), remote_fs.backend());
    }
}
