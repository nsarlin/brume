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
        AppliedDirCreation, AppliedFileUpdate, AppliedUpdate, DiffError, ReconciledUpdate,
        UpdateTarget, VfsNodeUpdate, VfsUpdateList,
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
/// that is its in-memory representation, using a tree-structure.#[derive(Clone, Debug)]
#[derive(Debug)]
pub struct FileSystem<Concrete: ConcreteFS> {
    concrete: Concrete,
    vfs: Vfs<Concrete::SyncInfo>,
}

impl<Concrete: ConcreteFS> FileSystem<Concrete> {
    pub fn new(concrete: Concrete) -> Self {
        Self {
            concrete,
            vfs: Vfs::empty(),
        }
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
    // TODO: handle conflicts
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
        let handle_conflicts: Vec<_> = updates
            .into_iter()
            .filter_map(|update| match update {
                ReconciledUpdate::Applicable(update) => Some(update),
                ReconciledUpdate::Conflict(path) => {
                    warn!("WARNING: conflict on {path:?}");
                    None
                }
            })
            .collect();

        let (self_updates, other_updates): (Vec<_>, Vec<_>) = handle_conflicts
            .into_iter()
            .partition(|update| match update.target() {
                UpdateTarget::SelfFs => true,
                UpdateTarget::OtherFs => false,
            });

        let self_futures = try_join_all(
            self_updates
                .into_iter()
                .map(|update| self.apply_update_concrete(other_fs, update.into())),
        );

        let other_futures = try_join_all(
            other_updates
                .into_iter()
                .map(|update| other_fs.apply_update_concrete(self, update.into())),
        );

        try_join!(
            self_futures.map_err(|e| (e, Concrete::TYPE_NAME)),
            other_futures.map_err(|e| (e, OtherConcrete::TYPE_NAME))
        )
    }

    /// Apply an update on the concrete fs.
    ///
    /// The file contents will be taken from `ref_fs`
    // TODO: handle if ref_fs is not sync ?
    pub async fn apply_update_concrete<OtherConcrete: ConcreteFS>(
        &self,
        ref_fs: &FileSystem<OtherConcrete>,
        update: VfsNodeUpdate,
    ) -> Result<AppliedUpdate<Concrete::SyncInfo>, ConcreteUpdateApplicationError> {
        if update.path().is_root() {
            return Err(ConcreteUpdateApplicationError::PathIsRoot);
        }

        match update {
            VfsNodeUpdate::DirCreated(path) => {
                let dir = ref_fs.find_dir(&path)?;

                self.clone_dir_concrete(dir, &path)
                    .await
                    .map(|created_dir| {
                        AppliedUpdate::DirCreated(AppliedDirCreation::new(
                            // The update path is not root so we can unwrap
                            path.parent().unwrap(),
                            created_dir,
                        ))
                    })
            }
            VfsNodeUpdate::DirRemoved(path) => {
                self.concrete.rmdir(&path).await.map_err(|e| e.into())?;
                Ok(AppliedUpdate::DirRemoved(path.to_owned()))
            }
            VfsNodeUpdate::FileCreated(path) => self
                .clone_file_concrete(&ref_fs.concrete, &path)
                .await
                .map(|(sync_info, size)| {
                    AppliedUpdate::FileCreated(AppliedFileUpdate::new(&path, size, sync_info))
                }),
            VfsNodeUpdate::FileModified(path) => self
                .clone_file_concrete(&ref_fs.concrete, &path)
                .await
                .map(|(sync_info, size)| {
                    AppliedUpdate::FileModified(AppliedFileUpdate::new(&path, size, sync_info))
                }),
            VfsNodeUpdate::FileRemoved(path) => {
                self.concrete.rm(&path).await.map_err(|e| e.into())?;
                Ok(AppliedUpdate::FileRemoved(path.to_owned()))
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
    ) -> Result<(Concrete::SyncInfo, u64), ConcreteUpdateApplicationError> {
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
    ) -> Result<DirTree<Concrete::SyncInfo>, ConcreteUpdateApplicationError> {
        let sync_info = self.concrete.mkdir(path).await.map_err(|e| {
            error!("Failed to create dir {path:?}: {e:?}");
            e.into()
        })?;
        let mut dir = DirTree::new(path.name(), sync_info);

        // TODO: parallelize file download
        for ref_child in ref_fs.dir.children().iter() {
            let mut child_path = path.to_owned();
            child_path.push(ref_child.name());
            let child =
                match ref_child {
                    VfsNode::Dir(ref_dir) => Box::pin(self.clone_dir_concrete(
                        FileSystemDir::new(ref_fs.concrete, ref_dir),
                        &child_path,
                    ))
                    .await
                    .map(VfsNode::Dir)?,
                    VfsNode::File(_) => self
                        .clone_file_concrete(ref_fs.concrete, &child_path)
                        .await
                        .map(|(sync_info, size)| FileMeta::new(child_path.name(), size, sync_info))
                        .map(VfsNode::File)?,
                };

            dir.insert_child(child);
        }

        Ok(dir)
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
            .apply_update_concrete(&remote_fs, VfsNodeUpdate::DirCreated(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.path(), dir_path);
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
            .apply_update_concrete(&remote_fs, VfsNodeUpdate::DirRemoved(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.path(), dir_path);
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
            .apply_update_concrete(&remote_fs, VfsNodeUpdate::FileCreated(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.path(), dir_path);
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
            .apply_update_concrete(&remote_fs, VfsNodeUpdate::FileModified(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.path(), dir_path);
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
            .apply_update_concrete(&remote_fs, VfsNodeUpdate::FileRemoved(dir_path.clone()))
            .await
            .unwrap();

        assert_eq!(applied.path(), dir_path);
        assert_eq!(local_fs.concrete, remote_fs.concrete);
    }
}
