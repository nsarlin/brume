use std::fmt::Display;

use concrete::ConcreteUpdateApplicationError;
use filesystem::VfsReloadError;
use thiserror::Error;
use update::{ReconciliationError, VfsUpdateApplicationError};

pub mod concrete;
pub mod filesystem;
pub mod sorted_vec;
pub mod synchro;
#[cfg(test)]
mod test_utils;
pub mod update;
pub mod vfs;

#[derive(Error, Debug)]
pub enum Error {
    #[error("failed to reload vfs from concrete FS {fs_name}")]
    VfsReloadError {
        fs_name: String,
        source: VfsReloadError,
    },
    #[error("failed to apply update to VFS node on {fs_name}")]
    VfsUpdateApplicationFailed {
        fs_name: String,
        source: VfsUpdateApplicationError,
    },
    #[error("failed to apply update to Concrete FS {fs_name}")]
    ConcreteUpdateFailed {
        fs_name: String,
        source: ConcreteUpdateApplicationError,
    },
    #[error("failed to reconcile updates from both filesystems")]
    ReconciliationFailed(#[from] ReconciliationError),
}

impl Error {
    pub fn vfs_reload<E: Into<VfsReloadError>>(fs_name: &str, source: E) -> Self {
        Self::VfsReloadError {
            fs_name: fs_name.to_string(),
            source: source.into(),
        }
    }

    pub fn vfs_update_application<E: Into<VfsUpdateApplicationError>>(
        fs_name: &str,
        source: E,
    ) -> Self {
        Self::VfsUpdateApplicationFailed {
            fs_name: fs_name.to_string(),
            source: source.into(),
        }
    }

    pub fn concrete_application<E: Into<ConcreteUpdateApplicationError>>(
        fs_name: &str,
        source: E,
    ) -> Self {
        Self::ConcreteUpdateFailed {
            fs_name: fs_name.to_string(),
            source: source.into(),
        }
    }
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
