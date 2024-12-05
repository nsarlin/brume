use std::fs;

use brume_cli::connect_to_daemon;
use tarpc::context;

use clap::{Parser, Subcommand};
use url::Url;

use brume_daemon::protocol::{
    AnyFsCreationInfo, AnyFsDescription, LocalDirCreationInfo, NextcloudFsCreationInfo,
};

#[derive(Parser)]
#[command(version, about, long_about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new synchronization
    #[command(after_help = "FILESYSTEM can be any of:
\t- The path to a valid folder on the local machine
\t- An URL to a Nextcloud server, structured as `http://user:password@domain.tld/endpoint`
")]
    New {
        /// The local filesystem for the synchronization
        #[arg(short, long, value_name = "FILESYSTEM", value_parser = parse_fs_argument)]
        local: AnyFsCreationInfo,

        /// The remote filesystem for the synchronization
        #[arg(short, long, value_name = "FILESYSTEM", value_parser = parse_fs_argument)]
        remote: AnyFsCreationInfo,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::New { local, remote } => {
            let local_desc = AnyFsDescription::from(&local);
            let remote_desc = AnyFsDescription::from(&remote);
            println!("Creating synchro between {local_desc} and {remote_desc}");
            let res = connect_to_daemon()
                .await
                .map_err(|_| "Failed to connect to brume daemon. Are your sure it's running ?")?
                .new_synchro(context::current(), local, remote)
                .await??;

            println!("Done: {}", res.id());
        }
    }

    Ok(())
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
        Err(format!(
            "{arg} is neither a valid path on your filesystem nor an url"
        ))
    }
}
