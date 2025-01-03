//! Definition of the file operations on real local or remote file systems

pub mod local;
pub mod nextcloud;

use std::error::Error;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::hash::Hash;
use std::io::{self, ErrorKind};
use std::sync::Arc;

use bytes::Bytes;
use futures::{Stream, TryStream, TryStreamExt};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;
use xxhash_rust::xxh3::xxh3_64;

use crate::update::IsModified;
use crate::vfs::{InvalidPathError, Vfs, VirtualPath};

#[derive(Error, Debug, Clone)]
#[error(transparent)]
pub struct ConcreteFsError(Arc<dyn std::error::Error + Send + Sync>);

/// Error encountered while applying an update to a ConcreteFS
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

/// Definition of the operations needed for a concrete FS backend
pub trait ConcreteFS:
    Named + Sized + TryFrom<Self::CreationInfo, Error = <Self as ConcreteFS>::IoError>
{
    /// Type used to detect updates on nodes of this filesystem. See [`IsModified`].
    type SyncInfo: IsModified + Debug + Named + Clone;
    /// Errors returned by this FileSystem type
    type IoError: Error + Send + Sync + 'static + Into<ConcreteFsError>;
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

    /// Return a description of this filesystem instance.
    ///
    /// This description should uniquely identify the Filesystem but also have a human readable
    /// form.
    fn description(&self) -> Self::Description;

    /// Load a virtual FS from the concrete one, by parsing its structure
    fn load_virtual(&self) -> impl Future<Output = Result<Vfs<Self::SyncInfo>, Self::IoError>>;

    /// Open and read a file on the concrete filesystem
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
    /// Write a file on the concrete filesystem
    fn write_file<Data: TryStream + Send + 'static + Unpin>(
        &self,
        path: &VirtualPath,
        data: Data,
    ) -> impl Future<Output = Result<Self::SyncInfo, Self::IoError>>
    where
        Data::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        Bytes: From<Data::Ok>;

    /// Remove a file on the concrete filesystem
    fn rm(&self, path: &VirtualPath) -> impl Future<Output = Result<(), Self::IoError>>;

    /// Create a directory on the concrete filesystem
    fn mkdir(
        &self,
        path: &VirtualPath,
    ) -> impl Future<Output = Result<Self::SyncInfo, Self::IoError>>;

    /// Remove a directory on the concrete filesystem
    fn rmdir(&self, path: &VirtualPath) -> impl Future<Output = Result<(), Self::IoError>>;
}

impl<T: ConcreteFS> Named for T {
    const TYPE_NAME: &'static str = T::SyncInfo::TYPE_NAME;
}

/// Compute a hash of the content of the file, for cross-FS comparison
async fn concrete_hash_file<Concrete: ConcreteFS>(
    concrete: &Concrete,
    path: &VirtualPath,
) -> Result<u64, ConcreteFsError> {
    let stream = concrete.read_file(path).await.map_err(|e| e.into())?;
    let mut reader = StreamReader::new(stream.map_err(|e| io::Error::new(ErrorKind::Other, e)));

    let mut data = Vec::new();
    reader.read_to_end(&mut data).await?;

    Ok(xxh3_64(&data))
}

/// Check if two files on different filesystems are identical by reading them and computing a hash
/// of their content
///
/// In case of error, return the underlying error and the name of the filesystem where this
/// error occurred.
pub async fn concrete_eq_file<LocalConcrete: ConcreteFS, RemoteConcrete: ConcreteFS>(
    local_concrete: &LocalConcrete,
    remote_concrete: &RemoteConcrete,
    path: &VirtualPath,
) -> Result<bool, (ConcreteFsError, &'static str)> {
    // TODO: cache files when possible
    let (local_hash, remote_hash) = tokio::join!(
        concrete_hash_file(local_concrete, path),
        concrete_hash_file(remote_concrete, path)
    );

    Ok(local_hash.map_err(|e| (e, LocalConcrete::TYPE_NAME))?
        == remote_hash.map_err(|e| (e, RemoteConcrete::TYPE_NAME))?)
}
