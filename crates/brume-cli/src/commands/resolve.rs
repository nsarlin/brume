use anyhow::{Context, anyhow};
use clap::Args;
use tarpc::context;

use brume_daemon_proto::{BrumeServiceClient, VirtualPathBuf};

use crate::get_synchro_id;

use super::SynchroSide;

#[derive(Args)]
pub struct CommandResolve {
    /// The id of the synchro
    synchro: String,
    /// The path of the node in conflict, as an absolute path from the root of the synchro
    #[arg(short, long)]
    path: String,
    /// The side of the synchro to chose for the resolution
    #[arg(short, long)]
    side: SynchroSide,
}

pub async fn resolve(
    daemon: BrumeServiceClient,
    args: CommandResolve,
) -> Result<(), Box<dyn std::error::Error>> {
    let CommandResolve {
        synchro,
        path,
        side,
    } = args;

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

    Ok(())
}
