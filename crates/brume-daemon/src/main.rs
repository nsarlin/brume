use std::sync::Arc;

use anyhow::Result;

use brume_daemon::daemon::{Daemon, DaemonConfig};

use brume_daemon_proto::{brume_config_path, load_brume_config};
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

#[tokio::main]
async fn main() -> Result<()> {
    let fmt_layer = fmt::layer().compact();
    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new("info,tarpc=error"))
        .unwrap();
    tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer)
        .init();

    let config = if let Some(config_path) = brume_config_path() {
        if config_path.exists() {
            let config = load_brume_config(config_path)?;
            config.daemon.into()
        } else {
            info!(
                "No config found at path {}, using the default one",
                config_path.display()
            );
            DaemonConfig::default()
        }
    } else {
        info!("No config found, using the default one",);
        DaemonConfig::default()
    };

    let daemon = Arc::new(Daemon::new(config).await?);

    daemon.run().await
}
