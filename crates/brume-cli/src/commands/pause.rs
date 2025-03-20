use anyhow::anyhow;
use clap::Args;
use tarpc::context;

use brume_daemon_proto::{BrumeServiceClient, SynchroState};

use crate::get_synchro;

#[derive(Args)]
pub struct CommandPause {
    synchro: String,
}

pub async fn pause(
    daemon: BrumeServiceClient,
    args: CommandPause,
) -> Result<(), Box<dyn std::error::Error>> {
    let CommandPause { synchro } = args;
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
    Ok(())
}
