use clap::Args;
use tarpc::context;

use brume_daemon_proto::BrumeServiceClient;

use crate::{get_synchro, prompt::prompt_synchro};

#[derive(Args)]
pub struct CommandRemove {
    synchro: Option<String>,
}

pub async fn remove(
    daemon: BrumeServiceClient,
    args: CommandRemove,
) -> Result<(), Box<dyn std::error::Error>> {
    let CommandRemove { synchro } = args;
    let list = daemon.list_synchros(context::current()).await?;

    if list.is_empty() {
        println!("No active synchro");
        return Ok(());
    }

    let (id, sync) = synchro
        .map(|sync| {
            get_synchro(&list, &sync).ok_or_else(|| String::from("Invalid synchro descriptor"))
        })
        .unwrap_or_else(|| prompt_synchro(&list))?;

    println!("Removing synchro: {} ({:x})", sync.name(), id.short());

    daemon.delete_synchro(context::current(), id).await??;
    println!("Done");
    Ok(())
}
