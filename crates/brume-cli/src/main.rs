use std::fs;

use brume_cli::{connect_to_daemon, get_synchro, get_synchro_id};
use comfy_table::Table;
use tarpc::context;

use anyhow::{anyhow, Context};
use clap::{Parser, Subcommand};
use url::Url;

use brume_daemon_proto::{
    AnyFsCreationInfo, AnyFsDescription, LocalDirCreationInfo, NextcloudFsCreationInfo,
    SynchroState, VirtualPathBuf,
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
    #[command(
        after_help = "FILESYSTEM can be any of:
\t- The path to a valid folder on the local machine
\t- An URL to a Nextcloud server, structured as `http://user:password@domain.tld/endpoint`
",
        visible_alias = "add"
    )]
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
    #[command(visible_alias = "list")]
    Ls {},

    /// Remove a synchronization
    #[command(visible_aliases = ["remove", "delete"])]
    Rm { synchro: String },

    /// Pause a synchronization
    Pause { synchro: String },

    /// Resume a synchronization
    Resume { synchro: String },

    /// Get the status of a synchronization
    Status { synchro: String },

    /// Resolve a conflict in a synchronization
    Resolve {
        /// The id of the synchro
        synchro: String,
        /// The path of the node in conflict, as an absolute path from the root of the synchro
        #[arg(short, long)]
        path: String,
        /// The side of the synchro to chose for the resolution
        #[arg(short, long)]
        side: SynchroSide,
    },
}

/// Like [`brume_daemon::protocol::SynchroSide`], but can be parsed by clap
#[derive(clap::ValueEnum, Copy, Clone, Debug)]
enum SynchroSide {
    Local,
    Remote,
}

impl From<SynchroSide> for brume_daemon_proto::SynchroSide {
    fn from(value: SynchroSide) -> Self {
        match value {
            SynchroSide::Local => Self::Local,
            SynchroSide::Remote => Self::Remote,
        }
    }
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
        Commands::Ls {} => {
            let synchros = daemon.list_synchros(context::current()).await?;
            let mut table = Table::new();
            table.set_header(vec!["ID", "Status", "State", "Local", "Remote", "Name"]);

            for (id, synchro) in synchros {
                table.add_row(vec![
                    format!("{:08x}", id.short()),
                    synchro.status().to_string(),
                    synchro.state().to_string(),
                    synchro.local().to_string(),
                    synchro.remote().to_string(),
                    synchro.name().to_string(),
                ]);
            }

            println!("{table}");
        }
        Commands::Rm { synchro } => {
            println!("Removing synchro: {synchro}");
            let list = daemon.list_synchros(context::current()).await?;

            let id = get_synchro_id(&list, &synchro)
                .await
                .ok_or_else(|| anyhow!("Invalid synchro descriptor"))?;

            daemon.delete_synchro(context::current(), id).await??;
            println!("Done");
        }
        Commands::Pause { synchro } => {
            println!("Pausing synchro: {synchro}");
            let list = daemon.list_synchros(context::current()).await?;

            let (id, sync) = get_synchro(&list, &synchro)
                .await
                .ok_or_else(|| anyhow!("Invalid synchro descriptor"))?;

            if matches!(sync.state(), SynchroState::Paused) {
                println!("Synchro is already paused");
                return Ok(());
            }

            daemon.pause_synchro(context::current(), id).await??;
            println!("Done");
        }

        Commands::Resume { synchro } => {
            println!("Resuming synchro: {synchro}");
            let list = daemon.list_synchros(context::current()).await?;

            let (id, sync) = get_synchro(&list, &synchro)
                .await
                .ok_or_else(|| anyhow!("Invalid synchro descriptor"))?;

            if matches!(sync.state(), SynchroState::Running) {
                println!("Synchro is already running");
                return Ok(());
            }

            daemon.resume_synchro(context::current(), id).await??;
            println!("Done");
        }

        Commands::Status { synchro } => {
            let list = daemon.list_synchros(context::current()).await?;

            let (id, sync) = get_synchro(&list, &synchro)
                .await
                .ok_or_else(|| anyhow!("Invalid synchro descriptor"))?;

            println!("○ Synchro: {} - {}", sync.name(), id.id());
            for (key, value) in [
                ("Status", sync.status().to_string().as_str()),
                ("State", sync.state().to_string().as_str()),
                ("Local type", sync.local().description().type_name()),
                ("Local", &sync.local().description().to_string()),
                ("Remote type", sync.remote().description().type_name()),
                ("Remote", &sync.remote().description().to_string()),
            ] {
                println!("{:>15}: {}", key, value);
            }

            //TODO: display more information in case of error/desync
        }

        Commands::Resolve {
            synchro,
            path,
            side,
        } => {
            println!("Resolving conflict in synchro: {synchro}, path: {path}, side: {side:?}");
            let list = daemon.list_synchros(context::current()).await?;

            let id = get_synchro_id(&list, &synchro)
                .await
                .ok_or_else(|| anyhow!("Invalid synchro descriptor"))?;

            let vpath = VirtualPathBuf::new(&path).context("Invalid path")?;

            daemon
                .resolve_conflict(context::current(), id, vpath, side.into())
                .await??;
            println!("Done");
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
