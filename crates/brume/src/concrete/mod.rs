//! Definition of the file operations on real local or remote file systems

pub mod local;
pub mod nextcloud;

use std::error::Error;
use std::fmt::Display;
use std::future::Future;
use std::io::{self, ErrorKind};

use bytes::Bytes;
use futures::{Stream, TryStream, TryStreamExt};
use thiserror::Error;
use tokio::io::AsyncReadExt;
use tokio_util::io::StreamReader;
use xxhash_rust::xxh3::xxh3_64;

use crate::update::IsModified;
use crate::vfs::{InvalidPathError, Vfs, VirtualPath};

#[derive(Error, Debug)]
#[error(transparent)]
pub struct ConcreteFsError(Box<dyn std::error::Error + Send + Sync>);

impl ConcreteFsError {
    pub fn new<E: std::error::Error + Send + Sync + Sized + 'static>(err: E) -> Self {
        Self(Box::new(err))
    }
}

/// Error encountered while applying an update to a ConcreteFS
#[derive(Error, Debug)]
pub enum ConcreteUpdateApplicationError {
    #[error("error from the concrete fs during upgrade application")]
    ConcreteFsError(#[from] ConcreteFsError),
    #[error("invalid path provided for update")]
    InvalidPath(#[from] InvalidPathError),
    #[error("cannot apply an update to the root dir itself")]
    PathIsRoot,
}

pub trait Named {
    /// Human readable name of the filesystem type, for user errors
    const NAME: &'static str;
}

/// Definition of the operations needed for a concrete FS backend
pub trait ConcreteFS: Named {
    type SyncInfo: IsModified + Named + Clone;
    type Error: Error + Send + Sync + 'static + Into<ConcreteFsError>;

    /// Load a virtual FS from the concrete one, by parsing its structure
    fn load_virtual(&self) -> impl Future<Output = Result<Vfs<Self::SyncInfo>, Self::Error>>;
    fn open(
        &self,
        path: &VirtualPath,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<Bytes, Self::Error>> + Send + Unpin + 'static,
            Self::Error,
        >,
    >;
    // TODO: write and write_new for file modif or create
    fn write<Data: TryStream + Send + 'static + Unpin>(
        &self,
        path: &VirtualPath,
        data: Data,
    ) -> impl Future<Output = Result<Self::SyncInfo, Self::Error>>
    where
        Data::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
        Bytes: From<Data::Ok>;
    fn rm(&self, path: &VirtualPath) -> impl Future<Output = Result<(), Self::Error>>;
    fn mkdir(
        &self,
        path: &VirtualPath,
    ) -> impl Future<Output = Result<Self::SyncInfo, Self::Error>>;
    fn rmdir(&self, path: &VirtualPath) -> impl Future<Output = Result<(), Self::Error>>;
}

impl<T: ConcreteFS> Named for T {
    const NAME: &'static str = T::SyncInfo::NAME;
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
pub async fn concrete_eq_file<Concrete: ConcreteFS, OtherConcrete: ConcreteFS>(
    concrete_self: &Concrete,
    concrete_other: &OtherConcrete,
    path: &VirtualPath,
) -> Result<bool, (ConcreteFsError, &'static str)> {
    // TODO: cache files when possible
    let (self_hash, other_hash) = tokio::join!(
        concrete_hash_file(concrete_self, path),
        concrete_hash_file(concrete_other, path)
    );

    Ok(self_hash.map_err(|e| (e, Concrete::TYPE_NAME))?
        == other_hash.map_err(|e| (e, Concrete::TYPE_NAME))?)
}
