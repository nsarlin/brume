use anyhow::Context;
use clap::Args;
use tarpc::context;

use brume_daemon_proto::{BrumeServiceClient, VirtualPathBuf};

use crate::{
    get_synchro,
    prompt::{prompt_side, prompt_synchro},
};

use super::SynchroSide;

#[derive(Args)]
pub struct CommandResolve {
    /// The id of the synchro
    synchro: Option<String>,
    /// The path of the node in conflict, as an absolute path from the root of the synchro
    #[arg(short, long)]
    path: String,
    /// The side of the synchro to chose for the resolution
    #[arg(short, long)]
    side: Option<SynchroSide>,
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
    let list = daemon.list_synchros(context::current()).await?;

    let (id, sync) = synchro
        .map(|sync| {
            get_synchro(&list, &sync).ok_or_else(|| String::from("Invalid synchro descriptor"))
        })
        .unwrap_or_else(|| prompt_synchro(&list))?;

    let side = side
        .map(|side| side.into())
        .map(Ok)
        .unwrap_or_else(|| prompt_side())?;

    println!(
        "Resolving conflict in synchro: {} ({:x}), path: {path}, side: {side:?}",
        sync.name(),
        id.short()
    );

    let vpath = VirtualPathBuf::new(&path).context("Invalid path")?;

    daemon
        .resolve_conflict(context::current(), id, vpath, side.into())
        .await??;
    println!("Done");

    Ok(())
}
