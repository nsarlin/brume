//! Definition of the file operations on real local or remote file systems

pub mod local;
pub mod nextcloud;

use std::error::Error;
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::future::{BoxFuture, try_join_all};
use futures::{Stream, TryStream, TryStreamExt};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{Instrument, error, info, info_span, instrument};
use xxhash_rust::xxh3::xxh3_64;

use crate::filesystem::{FileSystem, SkipUpdateError};
use crate::sorted_vec::SortedVec;
use crate::update::{
    AppliedDirCreation, AppliedFileUpdate, AppliedUpdate, DirModification, FailedUpdateApplication,
    IsModified, ReconciledUpdate, ReconciliationError, UpdateConflict, UpdateKind, VfsDiff,
    VirtualReconciledUpdate,
};
use crate::vfs::{
    DirInfo, DirTree, FileInfo, InvalidPathError, NodeInfo, StatefulDirTree, Vfs, VfsNode,
    VirtualPath, VirtualPathBuf,
};

/// The SyncInfo stored in bytes is invalid
#[derive(Error, Debug)]
#[error("Failed to load SyncInfo from raw bytes")]
pub struct InvalidBytesSyncInfo;

/// A SyncInfo that can be converted to bytes
///
/// This allows application to store it regardless of its concrete type
pub trait ToBytes {
    fn to_bytes(&self) -> Vec<u8>;
}

impl ToBytes for () {
    fn to_bytes(&self) -> Vec<u8> {
        Vec::new()
    }
}

/// A SyncInfo that can be created from bytes
///
/// This allows application to load it regardless of its concrete type
pub trait TryFromBytes: Sized {
    fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, InvalidBytesSyncInfo>;
}

impl TryFromBytes for () {
    fn try_from_bytes(bytes: Vec<u8>) -> Result<Self, InvalidBytesSyncInfo> {
        if bytes.is_empty() {
            Ok(())
        } else {
            Err(InvalidBytesSyncInfo)
        }
    }
}

/// Returned by [`ConcreteFS::clone_dir`]
///
/// This type holds the list of nodes that were successfully cloned and the one that resulted in an
/// error.
pub struct ConcreteDirCloneResult<SrcSyncInfo, DstSyncInfo> {
    success: Option<(StatefulDirTree<SrcSyncInfo>, StatefulDirTree<DstSyncInfo>)>,
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
        src_success: StatefulDirTree<SrcSyncInfo>,
        dst_success: StatefulDirTree<DstSyncInfo>,
        failures: Vec<FailedUpdateApplication>,
    ) -> Self {
        Self {
            success: Some((src_success, dst_success)),
            failures,
        }
    }

    /// Returns the successfully cloned nodes, if any, sorted in a [`DirTree`]
    pub fn take_success(
        &mut self,
    ) -> Option<(StatefulDirTree<SrcSyncInfo>, StatefulDirTree<DstSyncInfo>)> {
        self.success.take()
    }

    /// Returns the nodes that failed to be cloned
    pub fn take_failures(&mut self) -> Vec<FailedUpdateApplication> {
        std::mem::take(&mut self.failures)
    }
}

#[derive(Error, Debug, Clone)]
#[error(transparent)]
pub struct FsBackendError(Arc<dyn std::error::Error + Send + Sync>);

/// Error encountered while applying an update to a FSBackend
#[derive(Error, Debug)]
pub enum ConcreteUpdateApplicationError {
    #[error("invalid path provided for update")]
    InvalidPath(#[from] InvalidPathError),
    #[error("cannot apply an update to the root dir itself")]
    PathIsRoot,
    #[error("failed to process skipped update, loaded vfs is invalid")]
    SkipFailed(#[from] SkipUpdateError),
}

/// Failed to clone a file between filesystems
#[derive(Error, Debug)]
pub enum CloneFileError {
    #[error("The metadata returned by the backend are invalid")]
    InvalidBackendMeta,
    #[error("Error returned by the underlying backend")]
    BackendError(#[from] FsBackendError),
}

pub trait FsInstanceDescription: Display {
    fn name(&self) -> &str;
}

pub trait Named {
    /// Human readable name of the filesystem type, for user errors
    const TYPE_NAME: &'static str;
}

/// A backend is used by the [`ConcreteFS`] to perform io operations
pub trait FSBackend:
    Named + TryFrom<Self::CreationInfo, Error = <Self as FSBackend>::IoError> + Send + Sync
{
    /// Type used to detect updates on nodes of this filesystem. See [`IsModified`].
    type SyncInfo: IsModified + Debug + Named + Clone + Send + Sync;
    /// Errors returned by this FileSystem type
    type IoError: Error + Send + Sync + 'static + Into<FsBackendError>;
    /// Info needed to create a new filesystem of this type (url, login,...)
    type CreationInfo: Debug + Clone + Serialize + for<'a> Deserialize<'a>;
    /// A unique description of a particular filesystem instance
    type Description: FsInstanceDescription
        + From<Self::CreationInfo>
        + Clone
        + Hash
        + PartialEq
        + Debug
        + Serialize
        + for<'a> Deserialize<'a>;

    /// Checks if the creation info can be used to instantiate a concrete FS
    ///
    /// This is used to allow clients to return early errors to users.
    /// For example, it may imply checking that a login/password is valid or that a path exists
    fn validate(info: &Self::CreationInfo) -> BoxFuture<'_, Result<(), Self::IoError>>;

    /// Returns a description of this filesystem instance.
    ///
    /// This description should uniquely identify the Filesystem but also have a human readable
    /// form.
    fn description(&self) -> Self::Description;

    /// Returns updated metadata for a specific node
    fn get_node_info<'a>(
        &'a self,
        path: &'a VirtualPath,
    ) -> BoxFuture<'a, Result<NodeInfo<Self::SyncInfo>, Self::IoError>>;

    /// Loads a virtual FS from the concrete one, by parsing its structure
    fn load_virtual(&self) -> BoxFuture<'_, Result<Vfs<Self::SyncInfo>, Self::IoError>>;

    /// Opens and read a file on the concrete filesystem
    #[allow(clippy::type_complexity)] // Implementors cannot always name the stream type
    fn read_file<'a>(
        &'a self,
        path: &'a VirtualPath,
    ) -> BoxFuture<
        'a,
        Result<
            impl Stream<Item = Result<Bytes, Self::IoError>> + Send + Unpin + 'static,
            Self::IoError,
        >,
    >;

    // TODO: write and write_new for file modif or create
    /// Writes a file on the concrete filesystem
    fn write_file<'a, Data: TryStream + Send + 'static + Unpin>(
        &'a self,
        path: &'a VirtualPath,
        data: Data,
    ) -> BoxFuture<'a, Result<FileInfo<Self::SyncInfo>, Self::IoError>>
    where
        Data::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        Bytes: From<Data::Ok>;

    /// Removes a file on the concrete filesystem
    fn rm<'a>(&'a self, path: &'a VirtualPath) -> BoxFuture<'a, Result<(), Self::IoError>>;

    /// Creates a directory on the concrete filesystem
    fn mkdir<'a>(
        &'a self,
        path: &'a VirtualPath,
    ) -> BoxFuture<'a, Result<DirInfo<Self::SyncInfo>, Self::IoError>>;

    /// Removes a directory on the concrete filesystem
    fn rmdir<'a>(&'a self, path: &'a VirtualPath) -> BoxFuture<'a, Result<(), Self::IoError>>;
}

impl<T: FSBackend> Named for T {
    const TYPE_NAME: &'static str = T::SyncInfo::TYPE_NAME;
}

/// Return value of a successful [`ConcreteFS::clone_file`].
///
/// It holds up-to-date values for the syncinfo on the source and destination filesystems, that can
/// be used to update the [`Vfs`] accordingly.
///
/// [`Vfs`]: crate::vfs::Vfs
pub struct ConcreteFileCloneResult<SrcSyncInfo, DstSyncInfo> {
    src_file_info: FileInfo<SrcSyncInfo>,
    dst_file_info: FileInfo<DstSyncInfo>,
}

impl<SrcSyncInfo, DstSyncInfo> ConcreteFileCloneResult<SrcSyncInfo, DstSyncInfo> {
    pub fn new(src_file_info: FileInfo<SrcSyncInfo>, dst_file_info: FileInfo<DstSyncInfo>) -> Self {
        Self {
            src_file_info,
            dst_file_info,
        }
    }

    pub fn file_size(&self) -> u64 {
        self.src_file_info.size()
    }

    pub fn last_modified(&self) -> DateTime<Utc> {
        self.src_file_info.last_modified()
    }
}

impl<SrcSyncInfo, DstSyncInfo> From<ConcreteFileCloneResult<SrcSyncInfo, DstSyncInfo>>
    for (FileInfo<SrcSyncInfo>, FileInfo<DstSyncInfo>)
{
    fn from(value: ConcreteFileCloneResult<SrcSyncInfo, DstSyncInfo>) -> Self {
        (value.src_file_info, value.dst_file_info)
    }
}

/// Represents a concrete underlying filesystem.
///
/// It holds a [`FSBackend`] object, which provides the actual implementation for filesystem
/// operations.
#[derive(Debug)]
pub struct ConcreteFS<Backend: FSBackend> {
    backend: Backend,
}

impl<Backend: FSBackend> ConcreteFS<Backend> {
    pub fn new(backend: Backend) -> Self {
        Self { backend }
    }

    pub fn backend(&self) -> &Backend {
        &self.backend
    }

    /// Computes a hash of the content of the file, for cross-FS comparison
    #[instrument(skip_all)]
    async fn hash_file(&self, path: &VirtualPath) -> Result<u64, FsBackendError> {
        let stream = self.backend.read_file(path).await.map_err(|e| e.into())?;

        let data = stream
            .map_err(|e| e.into())
            .try_fold(Vec::new(), |mut acc, chunk| async move {
                acc.extend_from_slice(&chunk);
                Ok(acc)
            })
            .await?;

        Ok(xxh3_64(&data))
    }

    /// Checks if two files on different filesystems are identical by reading them and computing a
    /// hash of their content
    ///
    /// In case of error, return the underlying error and the name of the filesystem where this
    /// error occurred.
    #[instrument(skip_all)]
    pub async fn eq_file<OtherBackend: FSBackend>(
        &self,
        other: &ConcreteFS<OtherBackend>,
        path: &VirtualPath,
    ) -> Result<bool, (FsBackendError, &'static str)> {
        // TODO: cache files when possible
        let (local_hash, remote_hash) = tokio::join!(self.hash_file(path), other.hash_file(path));

        Ok(local_hash.map_err(|e| (e, Backend::TYPE_NAME))?
            == remote_hash.map_err(|e| (e, OtherBackend::TYPE_NAME))?)
    }

    /// Clone a directory from `ref_concrete` into the concrete fs of self.
    #[instrument(skip_all)]
    pub async fn clone_dir<RefBackend: FSBackend>(
        &self,
        ref_concrete: &ConcreteFS<RefBackend>,
        ref_dir: &DirTree<RefBackend::SyncInfo>,
        path: &VirtualPath,
    ) -> ConcreteDirCloneResult<RefBackend::SyncInfo, Backend::SyncInfo> {
        let src_info = match ref_concrete
            .backend()
            .get_node_info(path)
            .instrument(info_span!("get_node_info"))
            .await
        {
            Ok(dir_info) => dir_info,
            Err(err) => {
                error!("Failed to create dir {path}: {err:?}");
                return ConcreteDirCloneResult::new_mkdir_failed(path, err);
            }
        };

        let dst_info = match self
            .backend()
            .mkdir(path)
            .instrument(info_span!("mkdir"))
            .await
        {
            Ok(dir_info) => dir_info,
            Err(err) => {
                error!("Failed to create dir {path}: {err:?}");
                return ConcreteDirCloneResult::new_mkdir_failed(path, err);
            }
        };

        let mut dst_dir = DirTree::new_ok(
            path.name(),
            dst_info.last_modified(),
            dst_info.into_metadata(),
        );
        let mut src_dir = DirTree::new_ok(
            path.name(),
            src_info.last_modified(),
            src_info.into_metadata(),
        );
        let mut errors = Vec::new();

        let children: Vec<_> = ref_dir
            .children()
            .iter()
            .map(|child| {
                let mut path = path.to_owned();
                path.push(child.name());
                (path, child.is_dir())
            })
            .collect();

        let mut results = Vec::new();
        for (child_path, is_dir) in children.into_iter() {
            if is_dir {
                let mut rel_path = VirtualPathBuf::root();
                rel_path.push(child_path.name());
                let child_ref_dir = ref_dir.find_dir(&rel_path).unwrap();
                let mut result =
                    Box::pin(self.clone_dir(ref_concrete, child_ref_dir, &child_path)).await;

                let success = result
                    .take_success()
                    .map(|(src_dir, dst_dir)| (VfsNode::Dir(src_dir), VfsNode::Dir(dst_dir)));
                let failures = result.take_failures();
                results.push((success, failures));
            } else {
                match self.clone_file(ref_concrete, &child_path).await {
                    Ok(clone_result) => {
                        let (src_info, dst_info) = clone_result.into();
                        let src_node = VfsNode::File(src_info.into_ok());
                        let dst_node = VfsNode::File(dst_info.into_ok());
                        results.push((Some((src_node, dst_node)), Vec::new()));
                    }
                    Err(error) => {
                        let failure =
                            FailedUpdateApplication::new(VfsDiff::file_created(child_path), error);
                        results.push((None, vec![failure]));
                    }
                }
            }
        }

        for (success, failures) in results {
            if let Some((src_node, dst_node)) = success {
                dst_dir.insert_child(dst_node);
                src_dir.insert_child(src_node);
            }

            errors.extend(failures);
        }

        ConcreteDirCloneResult::new(src_dir, dst_dir, errors)
    }

    /// Clone a file from `ref_concrete` into the concrete fs of self.
    #[instrument(skip_all)]
    pub async fn clone_file<RefBackend: FSBackend>(
        &self,
        ref_concrete: &ConcreteFS<RefBackend>,
        path: &VirtualPath,
    ) -> Result<ConcreteFileCloneResult<RefBackend::SyncInfo, Backend::SyncInfo>, CloneFileError>
    {
        info!(
            "Cloning file {} from {} to {}",
            path,
            RefBackend::TYPE_NAME,
            Backend::TYPE_NAME
        );
        let stream = ref_concrete.backend.read_file(path).await.map_err(|e| {
            error!("Failed to read file {path}: {e:?}");
            e.into()
        })?;
        let dst_info = self.backend().write_file(path, stream).await.map_err(|e| {
            error!("Failed to clone file {path}: {e:?}");
            e.into()
        })?;

        // TODO: what happens if the src file is modified during the clone?
        let src_info = ref_concrete
            .backend
            .get_node_info(path)
            .await
            .map_err(|e| {
                error!("Failed to read src sync info {path}: {e:?}");
                e.into()
            })?
            .into_file_info()
            .ok_or(CloneFileError::InvalidBackendMeta)?;

        info!("File {path} successfully cloned");

        Ok(ConcreteFileCloneResult::new(src_info, dst_info))
    }

    /// Apply an update on the concrete fs.
    ///
    /// The file contents will transferred from `ref_concrete`
    // TODO: handle if ref_fs is not sync ?
    // TODO: check for "last minute" changes in target fs
    #[instrument(skip_all)]
    #[allow(clippy::type_complexity)]
    pub async fn apply_update<RefBackend: FSBackend>(
        &self,
        ref_fs: &FileSystem<RefBackend>,
        update: VfsDiff,
    ) -> Result<
        Vec<AppliedUpdate<RefBackend::SyncInfo, Backend::SyncInfo>>,
        ConcreteUpdateApplicationError,
    > {
        // Only supported update on root node is to modify its SyncInfo
        if update.path().is_root() && update.kind() != UpdateKind::DirModified {
            return Err(ConcreteUpdateApplicationError::PathIsRoot);
        }

        let path = update.path().to_owned();

        match update.kind() {
            UpdateKind::DirCreated => {
                let dir = ref_fs.find_loaded_dir(&path)?;

                let mut res = self.clone_dir(ref_fs.concrete(), dir, &path).await;

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
                info!("Removing dir {} from {}", path, Backend::TYPE_NAME);

                // If the update is a removal of a node that as never been created because of an
                // error, we can skip it
                if let Some(node) = ref_fs.vfs().find_node(update.path()) {
                    if node.can_skip_removal() {
                        return Ok(vec![AppliedUpdate::DirRemovedSkipped(path.to_owned())]);
                    }
                }

                self.backend()
                    .rmdir(&path)
                    .instrument(info_span!("rmdir"))
                    .await
                    .map(|_| AppliedUpdate::DirRemoved(path.to_owned()))
                    .or_else(|e| {
                        Ok(AppliedUpdate::FailedApplication(
                            FailedUpdateApplication::new(update, e.into()),
                        ))
                    })
                    .map(|update| vec![update])
            }
            // There is nothing to do on the backend for a dir modified, this update is just used to
            // propgate the SyncInfo to the vfs
            UpdateKind::DirModified => {
                let loaded_dir = ref_fs.find_loaded_dir(&path)?;
                let dir_mod = DirModification::new(
                    &path,
                    loaded_dir.last_modified(),
                    loaded_dir.metadata().clone(),
                );

                Ok(vec![AppliedUpdate::DirModified(dir_mod)])
            }
            UpdateKind::FileCreated => self
                .clone_file(ref_fs.concrete(), &path)
                .await
                .map(|clone_result| {
                    AppliedUpdate::FileCreated(AppliedFileUpdate::from_clone_result(
                        &path,
                        clone_result,
                    ))
                })
                .or_else(|e| {
                    Ok(AppliedUpdate::FailedApplication(
                        FailedUpdateApplication::new(update, e),
                    ))
                })
                .map(|update| vec![update]),
            UpdateKind::FileModified => self
                .clone_file(ref_fs.concrete(), &path)
                .await
                .map(|clone_result| {
                    AppliedUpdate::FileModified(AppliedFileUpdate::from_clone_result(
                        &path,
                        clone_result,
                    ))
                })
                .or_else(|e| {
                    Ok(AppliedUpdate::FailedApplication(
                        FailedUpdateApplication::new(update, e),
                    ))
                })
                .map(|update| vec![update]),
            UpdateKind::FileRemoved => {
                info!("Removing file {} from {}", path, Backend::TYPE_NAME);

                // If the update is a removal of a node that as never been created because of an
                // error, we can skip it
                if let Some(node) = ref_fs.vfs().find_node(update.path()) {
                    if node.can_skip_removal() {
                        return Ok(vec![AppliedUpdate::FileRemovedSkipped(path)]);
                    }
                }

                self.backend()
                    .rm(&path)
                    .instrument(info_span!("rm"))
                    .await
                    .map(|_| AppliedUpdate::FileRemoved(path))
                    .or_else(|e| {
                        Ok(AppliedUpdate::FailedApplication(
                            FailedUpdateApplication::new(update, e.into()),
                        ))
                    })
                    .map(|update| vec![update])
            }
        }
    }

    /// After updates are detected on the same file in both [`Vfs`], this checks with the backend if
    /// the conflict is real or not.
    ///
    /// "Fake" conflict happen when the same modification is done in both filesystem at the same
    /// time. Vfs update detection will detect a conflict but since the content of the file is not
    /// modified, the update should just be dropped on both sides.
    async fn filter_update_conflict<OtherBackend: FSBackend>(
        &self,
        other: &ConcreteFS<OtherBackend>,
        update: VirtualReconciledUpdate,
    ) -> Result<ReconciledUpdate, ReconciliationError> {
        match update {
            VirtualReconciledUpdate::Applicable(update) => Ok(ReconciledUpdate::Applicable(update)),
            VirtualReconciledUpdate::NeedBackendCheck(update) => {
                let path = update.path().to_owned();
                if self
                    .eq_file(other, &path)
                    .await
                    .map_err(|(e, name)| ReconciliationError::concrete(name, e))?
                {
                    Ok(ReconciledUpdate::Skip(update))
                } else {
                    Ok(ReconciledUpdate::Conflict(UpdateConflict::new_same_path(
                        update,
                    )))
                }
            }
            VirtualReconciledUpdate::Conflict(conflict) => Ok(ReconciledUpdate::Conflict(
                UpdateConflict::new_same_path(conflict),
            )),
            VirtualReconciledUpdate::Skip(update) => Ok(ReconciledUpdate::Skip(update)),
        }
    }

    /// Removes "Fake" updates from the list, where a conflict has been detected in the [`Vfs`] but
    /// the file are identical on both sides.
    #[instrument(skip_all)]
    pub(crate) async fn filter_update_conflicts_list<OtherBackend: FSBackend>(
        &self,
        other: &ConcreteFS<OtherBackend>,
        updates: SortedVec<VirtualReconciledUpdate>,
    ) -> Result<SortedVec<ReconciledUpdate>, ReconciliationError> {
        let filtered = try_join_all(
            updates
                .into_iter()
                .map(|update| self.filter_update_conflict(other, update)),
        )
        .await?;

        let filtered = filtered.into_iter().collect();

        // The order of the elements in the vec will be kept between input and output
        Ok(SortedVec::unchecked_from_vec(filtered))
    }
}
