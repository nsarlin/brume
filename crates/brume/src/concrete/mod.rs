//! Definition of the file operations on real local or remote file systems

mod byte_counter;
pub mod local;
pub mod nextcloud;

use std::error::Error;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::hash::Hash;
use std::io::{self, ErrorKind};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use byte_counter::ByteCounterExt;
use bytes::Bytes;
use futures::{Stream, TryStream, TryStreamExt};
use log::{error, info};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;
use xxhash_rust::xxh3::xxh3_64;

use crate::update::IsModified;
use crate::vfs::{InvalidPathError, Vfs, VirtualPath};

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
    Named + Sized + TryFrom<Self::CreationInfo, Error = <Self as FSBackend>::IoError>
{
    /// Type used to detect updates on nodes of this filesystem. See [`IsModified`].
    type SyncInfo: IsModified + Debug + Named + Clone;
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
    fn validate(info: &Self::CreationInfo) -> impl Future<Output = Result<(), Self::IoError>>;

    /// Returns a description of this filesystem instance.
    ///
    /// This description should uniquely identify the Filesystem but also have a human readable
    /// form.
    fn description(&self) -> Self::Description;

    /// Returns updated `SyncInfo` for a specific node
    fn get_sync_info(
        &self,
        path: &VirtualPath,
    ) -> impl Future<Output = Result<Self::SyncInfo, Self::IoError>>;

    /// Loads a virtual FS from the concrete one, by parsing its structure
    fn load_virtual(&self) -> impl Future<Output = Result<Vfs<Self::SyncInfo>, Self::IoError>>;

    /// Opens and read a file on the concrete filesystem
    fn read_file(
        &self,
        path: &VirtualPath,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<Bytes, Self::Error>> + Send + Unpin + 'static,
            Self::IoError,
        >,
    >;
    // TODO: write and write_new for file modif or create
    /// Writes a file on the concrete filesystem
    fn write_file<Data: TryStream + Send + 'static + Unpin>(
        &self,
        path: &VirtualPath,
        data: Data,
    ) -> impl Future<Output = Result<Self::SyncInfo, Self::IoError>>
    where
        Data::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        Bytes: From<Data::Ok>;

    /// Removes a file on the concrete filesystem
    fn rm(&self, path: &VirtualPath) -> impl Future<Output = Result<(), Self::IoError>>;

    /// Creates a directory on the concrete filesystem
    fn mkdir(
        &self,
        path: &VirtualPath,
    ) -> impl Future<Output = Result<Self::SyncInfo, Self::IoError>>;

    /// Removes a directory on the concrete filesystem
    fn rmdir(&self, path: &VirtualPath) -> impl Future<Output = Result<(), Self::IoError>>;
}

impl<T: FSBackend> Named for T {
    const TYPE_NAME: &'static str = T::SyncInfo::TYPE_NAME;
}

/// Return value of a successful [`ConcreteFS::clone_file`].
///
/// It holds up-to-date values for the syncinfo on the source and destination filesystems, that can
/// be used to update the [`Vfs`] accordingly.
pub struct ConcreteFileCloneResult<SrcSyncInfo, DstSyncInfo> {
    src_file_info: SrcSyncInfo,
    dst_file_info: DstSyncInfo,
    file_size: u64,
}

impl<SrcSyncInfo, DstSyncInfo> ConcreteFileCloneResult<SrcSyncInfo, DstSyncInfo> {
    pub fn new(file_size: u64, src_file_info: SrcSyncInfo, dst_file_info: DstSyncInfo) -> Self {
        Self {
            file_size,
            src_file_info,
            dst_file_info,
        }
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }
}

impl<SrcSyncInfo, DstSyncInfo> From<ConcreteFileCloneResult<SrcSyncInfo, DstSyncInfo>>
    for (SrcSyncInfo, DstSyncInfo)
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
    async fn hash_file(&self, path: &VirtualPath) -> Result<u64, FsBackendError> {
        let stream = self.backend.read_file(path).await.map_err(|e| e.into())?;
        let mut reader = StreamReader::new(stream.map_err(|e| io::Error::new(ErrorKind::Other, e)));

        let mut data = Vec::new();
        reader.read_to_end(&mut data).await?;

        Ok(xxh3_64(&data))
    }

    /// Checks if two files on different filesystems are identical by reading them and computing a
    /// hash of their content
    ///
    /// In case of error, return the underlying error and the name of the filesystem where this
    /// error occurred.
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

    /// Clone a file from `ref_concrete` into the concrete fs of self.
    ///
    /// Return the syncinfo associated with the created file and the number of bytes written.
    pub async fn clone_file<RefBackend: FSBackend>(
        &self,
        ref_concrete: &ConcreteFS<RefBackend>,
        path: &VirtualPath,
    ) -> Result<ConcreteFileCloneResult<RefBackend::SyncInfo, Backend::SyncInfo>, FsBackendError>
    {
        info!(
            "Cloning file {:?} from {} to {}",
            path,
            RefBackend::TYPE_NAME,
            Backend::TYPE_NAME
        );
        let counter = Arc::new(AtomicU64::new(0));
        let stream = ref_concrete
            .backend
            .read_file(path)
            .await
            .map_err(|e| {
                error!("Failed to read file {path:?}: {e:?}");
                e.into()
            })?
            .count_bytes(counter.clone());
        let dst_info = self.backend().write_file(path, stream).await.map_err(|e| {
            error!("Failed to clone file {path:?}: {e:?}");
            e.into()
        })?;
        let src_info = ref_concrete
            .backend
            .get_sync_info(path)
            .await
            .map_err(|e| {
                error!("Failed to read src sync info {path:?}: {e:?}");
                e.into()
            })?;

        let size = counter.load(std::sync::atomic::Ordering::SeqCst);

        info!("File {path:?} successfully cloned");

        Ok(ConcreteFileCloneResult::new(size, src_info, dst_info))
    }
}
