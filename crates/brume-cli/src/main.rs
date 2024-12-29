use std::fs;

use brume_cli::connect_to_daemon;
use comfy_table::Table;
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

        /// An optional name that will be given to the synchro instead of the default one
        #[arg(short, long)]
        name: Option<String>,
    },

    /// List all synchronizations
    #[command(visible_alias = "ls")]
    List {},
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let daemon = connect_to_daemon()
        .await
        .map_err(|_| "Failed to connect to brume daemon. Are your sure it's running ?")?;

    match cli.command {
        Commands::New {
            local,
            remote,
            name,
        } => {
            let local_desc = AnyFsDescription::from(local.clone());
            let remote_desc = AnyFsDescription::from(remote.clone());
            println!("Creating synchro between {local_desc} and {remote_desc}");
            daemon
                .new_synchro(context::current(), local, remote, name)
                .await??;
            println!("Done");
        }
        Commands::List {} => {
            let synchros = daemon.list_synchros(context::current()).await?;
            let mut table = Table::new();
            table.set_header(vec!["ID", "Local", "Remote", "Name"]);

            for synchro in synchros {
                table.add_row(vec![
                    format!("{:08x}", synchro.id().short()),
                    synchro.local().to_string(),
                    synchro.remote().to_string(),
                    synchro.name().to_string(),
                ]);
            }

            println!("{table}");
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
        Err("<FILESYSTEM> should be a valid path on your filesystem or an url".to_string())
    }
}
