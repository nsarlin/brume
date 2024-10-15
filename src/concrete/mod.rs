pub mod local;
pub mod remote;

use std::{future::Future, io::Read};

use crate::vfs::{Vfs, VirtualPath};

pub trait ConcreteFS: Sized {
    type SyncInfo;
    type Error;

    fn load_virtual(self) -> impl Future<Output = Result<Vfs<Self>, Self::Error>>;
    fn open(&self, path: &VirtualPath) -> impl Future<Output = Result<impl Read, Self::Error>>;
    // TODO: write and write_new for file modif or create
    fn write<Stream: Read>(
        &self,
        path: &VirtualPath,
        data: &mut Stream,
    ) -> impl Future<Output = Result<(), Self::Error>>;
    fn mkdir(&self, path: &VirtualPath) -> impl Future<Output = Result<(), Self::Error>>;
}
