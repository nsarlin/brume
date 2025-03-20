use anyhow::anyhow;
use clap::Args;
use tarpc::context;

use brume_daemon_proto::BrumeServiceClient;

use crate::get_synchro_id;

#[derive(Args)]
pub struct CommandRemove {
    synchro: String,
}

pub async fn remove(
    daemon: BrumeServiceClient,
    args: CommandRemove,
) -> Result<(), Box<dyn std::error::Error>> {
    let CommandRemove { synchro } = args;
    println!("Removing synchro: {synchro}");
    let list = daemon.list_synchros(context::current()).await?;

    let id = get_synchro_id(&list, &synchro)
        .await
        .ok_or_else(|| anyhow!("Invalid synchro descriptor"))?;

    daemon.delete_synchro(context::current(), id).await??;
    println!("Done");
    Ok(())
}
