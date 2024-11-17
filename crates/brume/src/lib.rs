use std::fmt::Display;

use concrete::{ConcreteFsError, ConcreteUpdateApplicationError};
use thiserror::Error;
use update::{DiffError, ReconciliationError, VfsUpdateApplicationError};
use vfs::InvalidPathError;

pub mod concrete;
pub mod filesystem;
pub mod sorted_vec;
#[cfg(test)]
mod test_utils;
pub mod update;
pub mod vfs;

#[derive(Error, Debug)]
pub enum Error {
    #[error("error from the concrete fs")]
    ConcreteFsError(#[from] ConcreteFsError),
    #[error("invalid path provided")]
    InvalidPath(#[from] InvalidPathError),
    #[error("failed to get VFS diff")]
    VfsDiffFailed(#[from] DiffError),
    #[error("failed to apply update to VFS node")]
    VfsUpdateFailed(#[from] VfsUpdateApplicationError),
    #[error("failed to apply update to Concrete FS")]
    ConcreteUpdateFailed(#[from] ConcreteUpdateApplicationError),
    #[error("failed to reconcile updates from both filesystems")]
    ReconciliationFailed(#[from] ReconciliationError),
}

#[derive(Error, Debug)]
pub struct NameMismatchError {
    pub found: String,
    pub expected: String,
}

impl Display for NameMismatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "name mismatch, expected {}, got {}",
            self.expected, self.found
        )
    }
}
