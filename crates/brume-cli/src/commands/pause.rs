use clap::Args;
use tarpc::context;

use brume_daemon_proto::{BrumeServiceClient, SynchroState};

use crate::{
    get_synchro,
    prompt::{filter_synchro_list, prompt_synchro},
};

#[derive(Args)]
pub struct CommandPause {
    synchro: Option<String>,
}

pub async fn pause(daemon: BrumeServiceClient, args: CommandPause) -> anyhow::Result<()> {
    let CommandPause { synchro } = args;
    let list = daemon.list_synchros(context::current()).await?;
    let list = filter_synchro_list(list, |sync| sync.state() == SynchroState::Running);

    if list.is_empty() {
        println!("No running synchro");
        return Ok(());
    }

    let (id, sync) = synchro
        .map(|sync| {
            get_synchro(&list, &sync).ok_or_else(|| anyhow::anyhow!("Invalid synchro descriptor"))
        })
        .unwrap_or_else(|| prompt_synchro(&list))?;

    println!("Pausing synchro: {} ({:x})", sync.name(), id.short());

    if matches!(sync.state(), SynchroState::Paused) {
        println!("Synchro is already paused");
        return Ok(());
    }

    daemon
        .pause_synchro(context::current(), id)
        .await?
        .map_err(|e| anyhow::anyhow!(e))?;
    println!("Done");
    Ok(())
}
