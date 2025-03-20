use brume_cli::{
    commands::{self, Commands},
    connect_to_daemon,
};

use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let daemon = connect_to_daemon()
        .await
        .map_err(|_| "Failed to connect to brume daemon. Are your sure it's running ?")?;

    match cli.command {
        Commands::New(args) => commands::new(daemon, args).await,
        Commands::Ls => commands::list(daemon).await,
        Commands::Rm(args) => commands::remove(daemon, args).await,
        Commands::Pause(args) => commands::pause(daemon, args).await,
        Commands::Resume(args) => commands::resume(daemon, args).await,
        Commands::Status(args) => commands::status(daemon, args).await,
        Commands::Resolve(args) => commands::resolve(daemon, args).await,
    }
}
