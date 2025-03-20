use anyhow::anyhow;
use clap::Args;
use tarpc::context;

use brume_daemon_proto::{BrumeServiceClient, SynchroState};

use crate::get_synchro;

#[derive(Args)]
pub struct CommandResume {
    synchro: String,
}

pub async fn resume(
    daemon: BrumeServiceClient,
    args: CommandResume,
) -> Result<(), Box<dyn std::error::Error>> {
    let CommandResume { synchro } = args;
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
    Ok(())
}
