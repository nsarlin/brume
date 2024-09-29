use reqwest_dav::{Auth, Client, ClientBuilder, Depth, Error};

mod dav;

use crate::{vfs::Vfs, NC_DAV_PATH_STR};
use dav::dav_parse_vfs;

#[derive(Debug)]
#[allow(unused)]
pub struct RemoteDir {
    client: Client,
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

        Ok(Self { client, vfs: root })
    }
}
