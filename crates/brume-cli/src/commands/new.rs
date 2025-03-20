use std::fs;

use brume_daemon_proto::{
    AnyFsCreationInfo, AnyFsDescription, BrumeServiceClient, LocalDirCreationInfo,
    NextcloudFsCreationInfo,
};
use clap::Args;
use tarpc::context;
use url::Url;

#[derive(Args)]
pub struct CommandNew {
    /// The local filesystem for the synchronization
    #[arg(short, long, value_name = "FILESYSTEM", value_parser = parse_fs_argument)]
    local: AnyFsCreationInfo,

    /// The remote filesystem for the synchronization
    #[arg(short, long, value_name = "FILESYSTEM", value_parser = parse_fs_argument)]
    remote: AnyFsCreationInfo,

    /// An optional name that will be given to the synchro instead of the default one
    #[arg(short, long)]
    name: Option<String>,
}

fn parse_fs_argument(arg: &str) -> Result<AnyFsCreationInfo, String> {
    if let Ok(url) = Url::parse(arg) {
        let port_fmt = if let Some(port) = url.port() {
            format!(":{port}")
        } else {
            String::new()
        };
        let address = format!(
            "{}://{}{}{}",
            url.scheme(),
            url.host_str().ok_or("Invalid url".to_string())?,
            port_fmt,
            url.path().trim_end_matches('/')
        );
        let login = url.username().to_string();
        let password = url.password().unwrap_or("").to_string();
        Ok(AnyFsCreationInfo::Nextcloud(NextcloudFsCreationInfo::new(
            &address, &login, &password,
        )))
    } else if let Ok(path) = fs::canonicalize(arg) {
        Ok(AnyFsCreationInfo::LocalDir(LocalDirCreationInfo::new(
            &path,
        )))
    } else {
        Err("<FILESYSTEM> should be a valid path on your filesystem or an url".to_string())
    }
}

pub async fn new(
    daemon: BrumeServiceClient,
    args: CommandNew,
) -> Result<(), Box<dyn std::error::Error>> {
    let CommandNew {
        local,
        remote,
        name,
    } = args;

    let local_desc = AnyFsDescription::from(local.clone());
    let remote_desc = AnyFsDescription::from(remote.clone());
    println!("Creating synchro between {local_desc} and {remote_desc}");
    daemon
        .new_synchro(context::current(), local, remote, name)
        .await??;
    println!("Done");

    Ok(())
}
