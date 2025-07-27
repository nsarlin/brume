use brume_daemon_proto::{
    AnyFsCreationInfo, AnyFsDescription, BrumeServiceClient, LocalDirCreationInfo,
    NextcloudFsCreationInfo, SynchroSide,
};
use clap::Args;
use inquire::Confirm;
use tarpc::context;
use url::Url;

use crate::prompt::{check_or_prompt_folder_creation, prompt_filesystem};

#[derive(Args)]
pub struct CommandNew {
    /// The local filesystem for the synchronization
    #[arg(short, long, value_name = "FILESYSTEM", value_parser = parse_fs_argument)]
    local: Option<AnyFsCreationInfo>,

    /// The remote filesystem for the synchronization
    #[arg(short, long, value_name = "FILESYSTEM", value_parser = parse_fs_argument)]
    remote: Option<AnyFsCreationInfo>,

    /// An optional name that will be given to the synchro instead of the default one
    #[arg(short, long)]
    name: Option<String>,
}

fn parse_fs_argument(arg: &str) -> anyhow::Result<AnyFsCreationInfo> {
    if let Ok(url) = Url::parse(arg) {
        let port_fmt = if let Some(port) = url.port() {
            format!(":{port}")
        } else {
            String::new()
        };
        let address = format!(
            "{}://{}{}{}",
            url.scheme(),
            url.host_str().ok_or(anyhow::anyhow!("Invalid url"))?,
            port_fmt,
            url.path().trim_end_matches('/')
        );
        let login = url.username().to_string();
        let password = url.password().unwrap_or("").to_string();
        Ok(AnyFsCreationInfo::Nextcloud(NextcloudFsCreationInfo::new(
            &address, &login, &password,
        )))
    } else {
        let path = check_or_prompt_folder_creation(arg).map_err(|_| {
            anyhow::anyhow!(
                "<FILESYSTEM> should be a valid path on your filesystem or an url".to_string()
            )
        })?;
        Ok(AnyFsCreationInfo::LocalDir(LocalDirCreationInfo::new(path)))
    }
}

pub async fn new(daemon: BrumeServiceClient, args: CommandNew) -> anyhow::Result<()> {
    let CommandNew {
        local,
        remote,
        name,
    } = args;

    let local = local
        .map(Ok)
        .unwrap_or_else(|| prompt_filesystem(SynchroSide::Local))?;

    let remote = remote
        .map(Ok)
        .unwrap_or_else(|| prompt_filesystem(SynchroSide::Remote))?;

    let local_desc = AnyFsDescription::from(local.clone());
    let remote_desc = AnyFsDescription::from(remote.clone());
    println!("Creating synchro between {local_desc} and {remote_desc}");
    // TODO: ask user to create local folder if it does not exist
    if !Confirm::new("Confirm?").with_default(true).prompt()? {
        println!("Cancelled");
        return Ok(());
    }

    daemon
        .new_synchro(context::current(), local, remote, name)
        .await?
        .map_err(|e| anyhow::anyhow!(e))?;
    println!("Done");

    Ok(())
}
