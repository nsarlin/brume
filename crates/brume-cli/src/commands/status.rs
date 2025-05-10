use anyhow::anyhow;
use clap::Args;
use tarpc::context;

use brume_daemon_proto::BrumeServiceClient;

use crate::get_synchro;

#[derive(Args)]
pub struct CommandStatus {
    synchro: String,
}

pub async fn status(
    daemon: BrumeServiceClient,
    args: CommandStatus,
) -> Result<(), Box<dyn std::error::Error>> {
    let CommandStatus { synchro } = args;
    let list = daemon.list_synchros(context::current()).await?;

    let (id, sync) = get_synchro(&list, &synchro)
        .await
        .ok_or_else(|| anyhow!("Invalid synchro descriptor"))?;

    println!("○ Synchro: {} - {}", sync.name(), id.id());
    for (key, value) in [
        ("Status", sync.status().to_string().as_str()),
        ("State", sync.state().to_string().as_str()),
        ("Local type", sync.local().description().type_name()),
        ("Local", &sync.local().description().to_string()),
        ("Remote type", sync.remote().description().type_name()),
        ("Remote", &sync.remote().description().to_string()),
    ] {
        println!("{:>15}: {}", key, value);
    }

    //TODO: display more information in case of error/desync
    Ok(())
}
