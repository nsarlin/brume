use clap::Args;
use inquire::Confirm;
use tarpc::context;

use brume_daemon_proto::{BrumeServiceClient, SynchroStatus};

use crate::{get_synchro, prompt::prompt_synchro};

#[derive(Args)]
pub struct CommandForceResync {
    synchro: Option<String>,
}

pub async fn force_resync(
    daemon: BrumeServiceClient,
    args: CommandForceResync,
) -> Result<(), Box<dyn std::error::Error>> {
    let CommandForceResync { synchro } = args;
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

    println!(
        "Triggering force resync of synchro: {} ({:x})",
        sync.name(),
        id.short()
    );

    if sync.status() != SynchroStatus::Desync {
        println!(
            "WARNING: Synchro {} is not Desync (status: {}). Doing a full resync might take a while.",
            sync.name(),
            sync.status()
        );
    }
    if !Confirm::new("Confirm?").with_default(true).prompt()? {
        println!("Cancelled");
        return Ok(());
    }

    daemon.force_resync(context::current(), id).await??;
    println!("Done");
    Ok(())
}
