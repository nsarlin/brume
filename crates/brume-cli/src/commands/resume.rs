use clap::Args;
use tarpc::context;

use brume_daemon_proto::{BrumeServiceClient, SynchroState};

use crate::{
    get_synchro,
    prompt::{filter_synchro_list, prompt_synchro},
};

#[derive(Args)]
pub struct CommandResume {
    synchro: Option<String>,
}

pub async fn resume(daemon: BrumeServiceClient, args: CommandResume) -> anyhow::Result<()> {
    let CommandResume { synchro } = args;
    let list = daemon.list_synchros(context::current()).await?;
    let list = filter_synchro_list(list, |sync| sync.state() == SynchroState::Paused);

    if list.is_empty() {
        println!("No paused synchro to resume");
        return Ok(());
    }

    let (id, sync) = synchro
        .map(|sync| {
            get_synchro(&list, &sync).ok_or_else(|| anyhow::anyhow!("Invalid synchro descriptor"))
        })
        .unwrap_or_else(|| prompt_synchro(&list))?;

    println!("Resuming synchro: {} ({:x})", sync.name(), id.short());

    if matches!(sync.state(), SynchroState::Running) {
        println!("Synchro is already running");
        return Ok(());
    }

    daemon
        .resume_synchro(context::current(), id)
        .await?
        .map_err(|e| anyhow::anyhow!(e))?;
    println!("Done");
    Ok(())
}
