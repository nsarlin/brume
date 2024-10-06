use reqwest_dav::{Auth, Client, ClientBuilder, Depth};

mod dav;

use crate::{
    vfs::{Vfs, VirtualPathError},
    NC_DAV_PATH_STR,
};
use dav::dav_parse_vfs;

/// An error during synchronisation with the remote file system
#[derive(Debug)]
pub enum RemoteFsError {
    /// A path provided by the server is invalid
    InvalidPath(VirtualPathError),
    /// The structure of the remote FS is not valid
    BadStructure,
    /// A dav protocol error occured during communication with the remote server
    ProtocolError(reqwest_dav::Error),
}

impl From<reqwest_dav::Error> for RemoteFsError {
    fn from(value: reqwest_dav::Error) -> Self {
        Self::ProtocolError(value)
    }
}

#[derive(Debug)]
#[allow(unused)]
pub struct RemoteFs {
    client: Client,
    vfs: Vfs,
}

impl RemoteFs {
    pub async fn new(url: &str, login: &str, password: &str) -> Result<Self, RemoteFsError> {
        let name = login.to_string();
        let client = ClientBuilder::new()
            .set_host(format!("{}{}{}/", url, NC_DAV_PATH_STR, &name))
            .set_auth(Auth::Basic(login.to_string(), password.to_string()))
            .build()?;

        let elements = client.list("", Depth::Infinity).await?;

        let root = dav_parse_vfs(elements, &name)?;

        Ok(Self { client, vfs: root })
    }
}
