mod dav;
#[cfg(test)]
mod test_utils;
pub mod vfs;

use dav::dav_parse_vfs;
use reqwest_dav::{Auth, Client, ClientBuilder, Depth, Error};
use vfs::Vfs;

const NC_DAV_PATH_STR: &str = "/remote.php/dav/files/";

#[derive(Debug)]
#[allow(unused)]
pub struct RemoteDir {
    client: Client,
    dir_name: String,
    vfs: Vfs,
}

impl RemoteDir {
    pub async fn new(url: &str, login: &str, password: &str) -> Result<Self, Error> {
        let name = login.to_string();
        let client = ClientBuilder::new()
            .set_host(format!("{}{}{}/", url, NC_DAV_PATH_STR, &name))
            .set_auth(Auth::Basic(login.to_string(), password.to_string()))
            .build()?;

        let elements = client.list("", Depth::Infinity).await?;

        let root = dav_parse_vfs(elements, &name).unwrap();

        Ok(Self {
            client,
            dir_name: name,
            vfs: root,
        })
    }
}
