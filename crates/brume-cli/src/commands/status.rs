use clap::Args;
use tarpc::context;

use brume_daemon_proto::BrumeServiceClient;

use crate::{get_synchro, prompt::prompt_synchro};

#[derive(Args)]
pub struct CommandStatus {
    synchro: Option<String>,
}

pub async fn status(daemon: BrumeServiceClient, args: CommandStatus) -> anyhow::Result<()> {
    let CommandStatus { synchro } = args;
    let list = daemon.list_synchros(context::current()).await?;

    if list.is_empty() {
        println!("No active synchro");
        return Ok(());
    }

    let (id, sync) = synchro
        .map(|sync| {
            get_synchro(&list, &sync).ok_or_else(|| anyhow::anyhow!("Invalid synchro descriptor"))
        })
        .unwrap_or_else(|| prompt_synchro(&list))?;

    println!("○ Synchro: {} - {}", sync.name(), id.id());
    for (key, value) in [
        ("Status", sync.status().to_string().as_str()),
        ("State", sync.state().to_string().as_str()),
        ("Local type", sync.local().description().type_name()),
        ("Local", &sync.local().description().to_string()),
        ("Remote type", sync.remote().description().type_name()),
        ("Remote", &sync.remote().description().to_string()),
    ] {
        println!("{key:>15}: {value}");
    }

    //TODO: display more information in case of error/desync
    Ok(())
}
