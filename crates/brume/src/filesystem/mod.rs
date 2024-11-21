//! File system manipulation

mod byte_counter;

use byte_counter::ByteCounterExt;

use std::{
    cmp::Ordering,
    sync::{atomic::AtomicU64, Arc},
};

use futures::future::try_join_all;
use tokio::try_join;

use crate::{
    concrete::{concrete_eq_file, ConcreteFS, ConcreteFsError, ConcreteUpdateApplicationError},
    sorted_vec::{Sortable, SortedVec},
    update::{
        AppliedDirCreation, AppliedFileUpdate, AppliedUpdate, DiffError, ReconciledUpdate,
        ReconciliationError, UpdateTarget, VfsNodeUpdate, VfsUpdateList,
    },
    vfs::{DirTree, FileMeta, InvalidPathError, Vfs, VfsNode, VirtualPath},
    Error, NameMismatchError,
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

    pub fn root(&self) -> FileSystemNode<'_, Concrete> {
        FileSystemNode {
            concrete: &self.concrete,
            node: self.vfs.root(),
        }
    }

    /// Query the concrete FS to update the in memory virtual representation
    pub async fn update_vfs(&mut self) -> Result<VfsUpdateList, VfsReloadError> {
        let new_vfs = self.concrete.load_virtual().await.map_err(|e| e.into())?;

        let updates = self.vfs.diff(&new_vfs)?;

        self.vfs = new_vfs;
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

    /// Return the node at the given path, if any
    pub fn find_node(&self, path: &VirtualPath) -> Option<FileSystemNode<'_, Concrete>> {
        Some(FileSystemNode {
            concrete: &self.concrete,
            node: self.vfs.root().find_node(path)?,
        })
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
        ConcreteUpdateApplicationError,
    > {
        let handle_conflicts: Vec<_> = updates
            .into_iter()
            .filter_map(|update| match update {
                ReconciledUpdate::Applicable(update) => Some(update),
                ReconciledUpdate::Conflict(path) => {
                    println!("WARNING: conflict on {path:?}");
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

        try_join!(self_futures, other_futures)
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
        match update {
            VfsNodeUpdate::DirCreated(path) => {
                let dir = ref_fs.find_dir(&path)?;

                self.clone_dir_concrete(dir, &path)
                    .await
                    .and_then(|created_dir| {
                        AppliedDirCreation::new(&path, created_dir)
                            .map(AppliedUpdate::DirCreated)
                            .map_err(|e| e.into())
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
        let counter = Arc::new(AtomicU64::new(0));
        let stream = ref_concrete
            .open(path)
            .await
            .map_err(|e| e.into())?
            .count_bytes(counter.clone());
        self.concrete
            .write(path, stream)
            .await
            .map_err(|e| e.into())
            .map_err(|e| e.into())
            .map(|info| (info, counter.load(std::sync::atomic::Ordering::SeqCst)))
    }

    /// Clone a directory from `ref_concrete` into the concrete fs of self.
    pub async fn clone_dir_concrete<'otherfs, OtherConcrete: ConcreteFS>(
        &self,
        ref_fs: FileSystemDir<'otherfs, OtherConcrete>,
        path: &VirtualPath,
    ) -> Result<DirTree<Concrete::SyncInfo>, ConcreteUpdateApplicationError> {
        let sync_info = self.concrete.mkdir(path).await.map_err(|e| e.into())?;
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
    pub async fn full_sync<OtherConcrete: ConcreteFS>(
        &mut self,
        other: &mut FileSystem<OtherConcrete>,
    ) -> Result<(), Error> {
        let (remote_diff, local_diff) = try_join!(self.update_vfs(), other.update_vfs())?;

        let reconciled = local_diff.reconcile(remote_diff, self, other).await?;

        let (local_applied, remote_applied) =
            self.apply_updates_list_concrete(other, reconciled).await?;

        self.vfs_mut().apply_updates_list(local_applied)?;
        other.vfs_mut().apply_updates_list(remote_applied)?;

        Ok(())
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

    /// Diff two directories, using both the [`Vfs`] to check their structures and the
    /// [`ConcreteFS`] for the content of the files.
    ///
    /// Since this diff use the `ConcreteFS` instead of the SyncInfo, it can be used to compare two
    /// different filesystems, for example during the [`reconciliation`] process.
    ///
    /// [`reconciliation`]: VfsUpdateList::reconcile
    pub async fn diff<'otherfs, OtherConcrete: ConcreteFS>(
        &self,
        other: &FileSystemDir<'otherfs, OtherConcrete>,
        parent_path: &VirtualPath,
    ) -> Result<VfsUpdateList, ReconciliationError> {
        // Here we cannot use `iter_zip_map` or an async variant of it because it does not seem
        // possible to express the lifetimes required by the async closures

        let mut dir_path = parent_path.to_owned();
        dir_path.push(self.dir.name());

        let mut ret = SortedVec::new();
        let mut self_iter = self.dir.children().iter();
        let mut other_iter = other.dir.children().iter();

        let mut self_item_opt = self_iter.next();
        let mut other_item_opt = other_iter.next();

        while let (Some(self_item), Some(other_item)) = (self_item_opt, other_item_opt) {
            match self_item.key().cmp(other_item.key()) {
                Ordering::Less => {
                    ret.insert(self_item.to_removed_diff(&dir_path));
                    self_item_opt = self_iter.next()
                }
                // We can use `unchecked_extend` because we know that the updates will be produced
                // in order
                Ordering::Equal => {
                    let self_child = FileSystemNode::new(self.concrete, self_item);
                    let other_child = FileSystemNode::new(other.concrete, other_item);

                    ret.unchecked_extend(Box::pin(self_child.diff(&other_child, &dir_path)).await?);
                    self_item_opt = self_iter.next();
                    other_item_opt = other_iter.next();
                }
                Ordering::Greater => {
                    ret.insert(other_item.to_created_diff(&dir_path));
                    other_item_opt = other_iter.next();
                }
            }
        }

        // Handle the remaining nodes that are present in an iterator and not the
        // other one
        while let Some(self_item) = self_item_opt {
            ret.insert(self_item.to_removed_diff(&dir_path));
            self_item_opt = self_iter.next();
        }

        while let Some(other_item) = other_item_opt {
            ret.insert(other_item.to_created_diff(&dir_path));
            other_item_opt = other_iter.next();
        }

        Ok(ret)
    }
}

/// A node in a [`FileSystem`], can represent a file or a directory.
pub struct FileSystemNode<'fs, Concrete: ConcreteFS> {
    concrete: &'fs Concrete,
    node: &'fs VfsNode<Concrete::SyncInfo>,
}

impl<'fs, Concrete: ConcreteFS> FileSystemNode<'fs, Concrete> {
    pub fn new(concrete: &'fs Concrete, node: &'fs VfsNode<Concrete::SyncInfo>) -> Self {
        Self { concrete, node }
    }

    /// Get the differences between [`FileSystem`] trees, by checking file contents with their
    /// concrete backends.
    ///
    /// Compared to [``VfsNode::diff``], this will diff the files based on their content and not
    /// only on their SyncInfo.
    pub async fn diff<'otherfs, OtherConcrete: ConcreteFS>(
        &self,
        other: &FileSystemNode<'otherfs, OtherConcrete>,
        parent_path: &VirtualPath,
    ) -> Result<VfsUpdateList, ReconciliationError> {
        if self.node.name() != other.node.name() {
            return Err(NameMismatchError {
                found: self.node.name().to_string(),
                expected: other.node.name().to_string(),
            }
            .into());
        }

        match (self.node, other.node) {
            (VfsNode::Dir(dself), VfsNode::Dir(dother)) => {
                let self_dir = FileSystemDir::new(self.concrete, dself);
                let other_dir = FileSystemDir::new(other.concrete, dother);
                self_dir.diff(&other_dir, parent_path).await
            }
            (VfsNode::File(fself), VfsNode::File(fother)) => {
                // Diff the file based on their size or hash
                let mut file_path = parent_path.to_owned();
                file_path.push(fself.name());

                if fself.size() != fother.size()
                    || !concrete_eq_file(self.concrete, other.concrete, &file_path).await?
                {
                    let diff = VfsNodeUpdate::FileModified(file_path);
                    Ok(VfsUpdateList::from([diff]))
                } else {
                    Ok(VfsUpdateList::new())
                }
            }
            (nself, nother) => Ok(VfsUpdateList::from_vec(vec![
                nself.to_removed_diff(parent_path),
                nother.to_created_diff(parent_path),
            ])),
        }
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
    async fn test_concrete_diff() {
        let local_base = D(
            "",
            vec![
                D("Doc", vec![FF("f1.md", b"hello"), FF("f2.pdf", b"world")]),
                D("a", vec![D("b", vec![D("c", vec![])])]),
                D("e", vec![D("g", vec![FF("tmp.txt", b"content")])]),
            ],
        );
        let mut local_fs = TestFileSystem::new(local_base.into());
        local_fs.update_vfs().await.unwrap();

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
        let mut remote_fs = TestFileSystem::new(remote_base.into());
        remote_fs.update_vfs().await.unwrap();

        let diff = local_fs
            .root()
            .diff(&remote_fs.root(), VirtualPath::root())
            .await
            .unwrap();

        let reference_diff = [
            VfsNodeUpdate::FileModified(VirtualPathBuf::new("/Doc/f1.md").unwrap()),
            VfsNodeUpdate::FileCreated(VirtualPathBuf::new("/a/b/test.log").unwrap()),
            VfsNodeUpdate::DirRemoved(VirtualPathBuf::new("/e/g").unwrap()),
        ]
        .into();

        assert_eq!(diff, reference_diff);
    }

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
