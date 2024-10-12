pub mod concrete;
#[cfg(test)]
mod test_utils;
pub mod vfs;

const NC_DAV_PATH_STR: &str = "/remote.php/dav/files/";

#[derive(Debug)]
pub struct Error;
