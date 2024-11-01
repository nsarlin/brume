use std::io;

use concrete::remote::RemoteFsError;

pub mod concrete;
mod sorted_list;
#[cfg(test)]
mod test_utils;
pub mod vfs;

const NC_DAV_PATH_STR: &str = "/remote.php/dav/files/";

#[derive(Debug)]
pub enum Error {
    Local(io::Error),
    Remote(RemoteFsError),
    InvalidPath,
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Local(value)
    }
}

impl From<RemoteFsError> for Error {
    fn from(value: RemoteFsError) -> Self {
        Self::Remote(value)
    }
}
