use thiserror::Error;
use vfs::{DiffError, VirtualPathBuf};

pub mod concrete;
mod sorted_list;
#[cfg(test)]
mod test_utils;
pub mod vfs;

const NC_DAV_PATH_STR: &str = "/remote.php/dav/files/";

#[derive(Error, Debug)]
pub enum Error {
    #[error("error from the concrete fs")]
    ConcreteFsError(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("invalid path provided")]
    InvalidPath(VirtualPathBuf),
    #[error("failed to get VFS diff")]
    InvalidSyncInfo(#[from] DiffError),
}
