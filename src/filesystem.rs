//! A file system is represented by a Concrete FS backend used for file operations and a Virtual FS
//! that is its in-momory representation, using a tree-structure.

use crate::{
    concrete::ConcreteFS,
    vfs::{SortedPatchList, Vfs},
    Error,
};

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

    pub async fn update_vfs(&mut self) -> Result<SortedPatchList, Error>
    where
        Error: From<Concrete::Error>,
    {
        let new_vfs = self.concrete.load_virtual().await?;

        let updates = self.vfs.diff(&new_vfs)?;

        self.vfs = new_vfs;
        Ok(updates)
    }

    pub fn vfs(&self) -> &Vfs<Concrete::SyncInfo> {
        &self.vfs
    }

    pub fn concrete(&self) -> &Concrete {
        &self.concrete
    }
}
