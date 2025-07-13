mod force_resync;
mod list;
mod new;
mod pause;
mod remove;
mod resolve;
mod resume;
mod status;

use clap::Subcommand;
pub use force_resync::*;
pub use list::*;
pub use new::*;
pub use pause::*;
pub use remove::*;
pub use resolve::*;
pub use resume::*;
pub use status::*;

/// Like [`brume_daemon::protocol::SynchroSide`], but can be parsed by clap
#[derive(clap::ValueEnum, Copy, Clone, Debug)]
enum SynchroSide {
    Local,
    Remote,
}

impl From<SynchroSide> for brume_daemon_proto::SynchroSide {
    fn from(value: SynchroSide) -> Self {
        match value {
            SynchroSide::Local => Self::Local,
            SynchroSide::Remote => Self::Remote,
        }
    }
}

#[derive(Subcommand)]
pub enum Commands {
    /// Create a new synchronization
    #[command(
        after_help = "FILESYSTEM can be any of:
\t- The path to a valid folder on the local machine
\t- An URL to a Nextcloud server, structured as `http://user:password@domain.tld/endpoint`
",
        visible_alias = "add"
    )]
    New(CommandNew),

    /// List all synchronizations
    #[command(visible_alias = "list")]
    Ls,

    /// Remove a synchronization
    #[command(visible_aliases = ["remove", "delete"])]
    Rm(CommandRemove),

    /// Pause a synchronization
    Pause(CommandPause),

    /// Resume a synchronization
    Resume(CommandResume),

    /// Get the status of a synchronization
    Status(CommandStatus),

    /// Resolve a conflict in a synchronization
    Resolve(CommandResolve),

    /// Trigger a force resync. This will completely re-scan the remote and local folder.
    /// This might take a while but can be used to recover from a `Desync` status.
    ForceResync(CommandForceResync),
}
