//! File operations on real local or remote file systems

pub mod local;
pub mod nextcloud;

use std::{future::Future, io::Read};

use crate::{
    vfs::{IsModified, Vfs, VirtualPath},
    Error,
};

/// Definition of the operations needed for a concrete FS backend
pub trait ConcreteFS: Sized {
    type SyncInfo: IsModified<Self::SyncInfo> + Clone;
    type Error;

    fn load_virtual(&self) -> impl Future<Output = Result<Vfs<Self::SyncInfo>, Self::Error>>;
    fn open(&self, path: &VirtualPath) -> impl Future<Output = Result<impl Read, Self::Error>>;
    // TODO: write and write_new for file modif or create
    fn write<Stream: Read>(
        &self,
        path: &VirtualPath,
        data: &mut Stream,
    ) -> impl Future<Output = Result<(), Self::Error>>;
    fn mkdir(&self, path: &VirtualPath) -> impl Future<Output = Result<(), Self::Error>>;
    fn hash(&self, path: &VirtualPath) -> impl Future<Output = Result<u64, Self::Error>>;
}

/// Check if two files on different filesystems are identical by reading them and computing a hash
/// of their content
pub async fn concrete_eq_file<Concrete: ConcreteFS, OtherConcrete: ConcreteFS>(
    concrete_self: &Concrete,
    concrete_other: &OtherConcrete,
    path: &VirtualPath,
) -> Result<bool, Error>
where
    Error: From<Concrete::Error> + From<OtherConcrete::Error>,
{
    let (self_hash, other_hash) = tokio::join!(concrete_self.hash(path), concrete_other.hash(path));

    Ok(self_hash? == other_hash?)
}
