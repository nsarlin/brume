use brume_vfs::{
    InvalidPathError,
    update::{MergeError, VfsUpdateApplicationError},
};
use concrete::{ConcreteUpdateApplicationError, FSBackend};
use filesystem::VfsReloadError;
use synchro::ReconciliationError;
use thiserror::Error;

pub mod concrete;
pub mod filesystem;
pub mod synchro;

#[cfg(test)]
mod test_utils;

pub mod vfs {
    pub use brume_vfs::*;
}

pub mod update {
    pub use brume_vfs::update::*;
}

pub mod sorted_vec {
    pub use brume_vfs::sorted_vec::*;
}

pub use brume_vfs::Named;

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
    #[error("Provided path is incorrect")]
    InvalidPath(#[from] InvalidPathError),
}

impl Error {
    pub fn vfs_reload<E: Into<VfsReloadError>>(fs_name: &str, source: E) -> Self {
        Self::VfsReloadError {
            fs_name: fs_name.to_string(),
            source: source.into(),
        }
    }

    pub fn vfs_update_application<Backend: FSBackend>(source: VfsUpdateApplicationError) -> Self {
        Self::VfsUpdateApplicationFailed {
            fs_name: Backend::TYPE_NAME.to_string(),
            source,
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

    /// Returns true if the error is caused by the concrete backend (eg: network down, no space
    /// left,...)
    pub fn is_concrete(&self) -> bool {
        match self {
            Error::VfsReloadError { source, .. } => match source {
                VfsReloadError::FsBackendError(_) => true,
                VfsReloadError::DiffError(_) => false,
            },
            Error::ReconciliationFailed(ReconciliationError::FsBackendError { .. }) => true,
            _ => false,
        }
    }
}

impl From<MergeError> for Error {
    fn from(value: MergeError) -> Self {
        Self::ReconciliationFailed(value.into())
    }
}
