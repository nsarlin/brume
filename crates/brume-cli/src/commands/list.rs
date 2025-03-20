use comfy_table::Table;
use tarpc::context;

use brume_daemon_proto::BrumeServiceClient;

pub async fn list(daemon: BrumeServiceClient) -> Result<(), Box<dyn std::error::Error>> {
    let synchros = daemon.list_synchros(context::current()).await?;
    let mut table = Table::new();
    table.set_header(vec!["ID", "Status", "State", "Local", "Remote", "Name"]);

    for (id, synchro) in synchros {
        table.add_row(vec![
            format!("{:08x}", id.short()),
            synchro.status().to_string(),
            synchro.state().to_string(),
            synchro.local().to_string(),
            synchro.remote().to_string(),
            synchro.name().to_string(),
        ]);
    }

    println!("{table}");
    Ok(())
}
