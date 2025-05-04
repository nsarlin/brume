use anyhow::Context;
use clap::Args;
use tarpc::context;

use brume_daemon_proto::{BrumeServiceClient, SynchroStatus, VirtualPathBuf};

use crate::{
    get_synchro,
    prompt::{filter_synchro_list, prompt_conflict_path, prompt_side, prompt_synchro},
};

use super::SynchroSide;

#[derive(Args)]
pub struct CommandResolve {
    /// The id of the synchro
    synchro: Option<String>,
    /// The path of the node in conflict, as an absolute path from the root of the synchro
    #[arg(short, long)]
    path: Option<String>,
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
    let list = filter_synchro_list(list, |sync| sync.status() == SynchroStatus::Conflict);

    if list.is_empty() {
        println!("No conflict to resolve");
        return Ok(());
    }

    let (id, sync) = synchro
        .map(|sync| {
            get_synchro(&list, &sync).ok_or_else(|| String::from("Invalid synchro descriptor"))
        })
        .unwrap_or_else(|| prompt_synchro(&list))?;

    let base_path = if let Some(path) = path {
        VirtualPathBuf::new(&path).context("Invalid path")?
    } else {
        let local_vfs = daemon
            .get_vfs(
                context::current(),
                id,
                brume_daemon_proto::SynchroSide::Local,
            )
            .await??;
        let remote_vfs = daemon
            .get_vfs(
                context::current(),
                id,
                brume_daemon_proto::SynchroSide::Remote,
            )
            .await??;

        prompt_conflict_path(&local_vfs, &remote_vfs)?
    };

    let side = side
        .map(|side| side.into())
        .map(Ok)
        .unwrap_or_else(prompt_side)?;

    println!(
        "Resolving conflict in synchro: {} ({:x}), path: {base_path:?}, side: {side:?}",
        sync.name(),
        id.short()
    );

    daemon
        .resolve_conflict(context::current(), id, base_path, side)
        .await??;
    println!("Done");

    Ok(())
}
