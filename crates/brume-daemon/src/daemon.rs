//! Handles the list of [`brume::synchro::Synchro`] in the background
//!
//! The daemon can be queried through the [`Server`] by multiple client applications to add or
//! remove filesystem pairs to synchronize. It regularly synchronizes them.

use std::{
    future::Future,
    io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow};

use brume::vfs::VirtualPathBuf;
use futures::StreamExt;
use interprocess::local_socket::{
    GenericNamespaced, ListenerOptions, ToNsName, tokio::Listener, traits::tokio::Listener as _,
};
use tarpc::{
    serde_transport,
    server::{BaseChannel, Channel},
    tokio_serde::formats::Bincode,
    tokio_util::{
        codec::{LengthDelimitedCodec, length_delimited::Builder},
        sync::CancellationToken,
    },
};
use tokio::{
    sync::{
        Mutex,
        mpsc::{UnboundedReceiver, unbounded_channel},
    },
    task::JoinHandle,
    time,
};
use tracing::{error, info, warn};

use brume_daemon_proto::{
    AnySynchroCreationInfo, BRUME_SOCK_NAME, BrumeService, SynchroId, SynchroSide, SynchroState,
};

use crate::{
    db::{Database, DatabaseConfig},
    server::Server,
    synchro_list::ReadWriteSynchroList,
};

/// Configuration of a [`Daemon`]
#[derive(Clone)]
pub struct DaemonConfig {
    /// Time between two synchronizations
    sync_interval: Duration,
    /// How internal errors are handled
    error_mode: ErrorMode,
    /// Name of the unix socket used to communicate with the daemon
    sock_name: String,
    /// Config of the sqlite database
    db: DatabaseConfig,
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            sync_interval: Duration::from_secs(10),
            error_mode: ErrorMode::default(),
            sock_name: BRUME_SOCK_NAME.to_string(),
            db: DatabaseConfig::OnDisk(PathBuf::from("./dev.db")), // TODO: change this
        }
    }
}

impl DaemonConfig {
    pub fn with_sync_interval(self, sync_interval: Duration) -> Self {
        Self {
            sync_interval,
            ..self
        }
    }

    pub fn with_error_mode(self, error_mode: ErrorMode) -> Self {
        Self { error_mode, ..self }
    }

    pub fn with_sock_name(self, sock_name: &str) -> Self {
        Self {
            sock_name: sock_name.to_string(),
            ..self
        }
    }

    pub fn with_db_config(self, db_config: DatabaseConfig) -> Self {
        Self {
            db: db_config,
            ..self
        }
    }
}

/// How errors should be handled by the daemon
#[derive(Default, Copy, Clone, PartialEq, Eq)]
pub enum ErrorMode {
    #[default]
    Log,
    Exit,
}

/// The different commands that can be received from user applications
#[derive(Debug)]
pub enum UserCommand {
    SynchroCreation(SynchroCreationRequest),
    SynchroDeletion(SynchroDeletionRequest),
    StateChange(StateChangeRequest),
    ConflictResolution(ConflictResolutionRequest),
}

/// A command to create a new synchro
#[derive(Debug)]
pub struct SynchroCreationRequest {
    info: AnySynchroCreationInfo,
}

impl SynchroCreationRequest {
    pub fn new(info: AnySynchroCreationInfo) -> Self {
        Self { info }
    }
}

/// A command to delete an existing synchro
#[derive(Debug)]
pub struct SynchroDeletionRequest {
    id: SynchroId,
}

impl SynchroDeletionRequest {
    pub fn new(id: SynchroId) -> Self {
        Self { id }
    }
}

/// A user request to change the [`SynchroState`] of a synchro
#[derive(Debug)]
pub struct StateChangeRequest {
    id: SynchroId,
    state: SynchroState,
}

impl StateChangeRequest {
    pub fn new(id: SynchroId, state: SynchroState) -> Self {
        Self { id, state }
    }
}

/// A user request for a conflict resolution
#[derive(Debug)]
pub struct ConflictResolutionRequest {
    id: SynchroId,
    path: VirtualPathBuf,
    side: SynchroSide,
}

impl ConflictResolutionRequest {
    pub fn new(id: SynchroId, path: VirtualPathBuf, side: SynchroSide) -> Self {
        Self { id, path, side }
    }
}

/// The daemon holds the list of the synchronized folders, and synchronize them regularly.
///
/// It can be queried by client applications through the [`Server`].
pub struct Daemon {
    codec_builder: Builder,
    rpc_listener: Listener,
    synchro_list: ReadWriteSynchroList,
    server: Server,
    commands_chan: Mutex<UnboundedReceiver<UserCommand>>,
    is_running: AtomicBool,
    cancellation_token: CancellationToken,
    database: Database,
    config: DaemonConfig,
}

impl Daemon {
    pub async fn new(config: DaemonConfig) -> Result<Self> {
        let name = config
            .sock_name
            .as_str()
            .to_ns_name::<GenericNamespaced>()
            .context("Invalid name for sock")?;
        let opts = ListenerOptions::new().name(name);
        let listener = match opts.create_tokio() {
            Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
                error!(
                    "Error: could not start server because the socket file is occupied. \
Please check if {} is in use by another process and try again.",
                    config.sock_name
                );
                return Err(e).context("Failed to start server");
            }
            x => x?,
        };

        let codec_builder = LengthDelimitedCodec::builder();
        let (commands_to_daemon, commands_from_server) = unbounded_channel();

        info!("Loading db: {}", config.db.as_str().unwrap());
        let database = Database::new(&config.db).await?;

        let synchro_list = ReadWriteSynchroList::from(database.load_all_synchros().await.unwrap());

        info!("Server running at {}", config.sock_name);
        let server = Server::new(commands_to_daemon, synchro_list.as_read_only());

        Ok(Self {
            codec_builder,
            rpc_listener: listener,
            synchro_list,
            server,
            commands_chan: Mutex::new(commands_from_server),
            is_running: AtomicBool::new(false),
            cancellation_token: CancellationToken::new(),
            config,
            database,
        })
    }

    /// Requests to stop the running daemon
    pub fn stop(&self) {
        self.cancellation_token.cancel()
    }

    /// Logs the error, and properly shutdowns the daemon if [`ErrorMode`] is `Exit`
    pub fn handle_error(&self, err: &anyhow::Error) {
        if self.config.error_mode == ErrorMode::Exit {
            // print user orented multiline backtrace
            error!("{err:?}");
            self.stop();
        } else {
            // print single line backtrace for logs
            error!("{err:#}");
        }
    }

    /// Spawns a new tokio task and runs the daemon inside it
    pub async fn spawn(self: &Arc<Self>) -> JoinHandle<Result<()>> {
        let daemon = self.clone();
        tokio::spawn(async move { daemon.run().await })
    }

    /// Handles client connections and periodically synchronize filesystems
    pub async fn run(self: Arc<Self>) -> Result<()> {
        if self
            .is_running
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return Err(anyhow!("Daemon is already running"));
        }
        // Handle connections from client apps
        {
            let server = self.clone();
            tokio::spawn(async move { server.serve().await });
        }

        // Synchronize all filesystems
        let mut interval = time::interval(self.config.sync_interval);
        interval.tick().await; // The first interval is immediate

        loop {
            info!("Starting full sync for all filesystems");
            let synchro_list = self.synchro_list();

            let results = synchro_list.sync_all(&self.database).await;

            for res in results {
                let _ = res
                    .context("Failed to synchronize filesystems")
                    .inspect_err(|err| self.handle_error(err));
            }
            // Wait and update synchro list with any new sync from user

            loop {
                tokio::select! {
                    _ = interval.tick() => break,
                    Some(command) = self.recv_user_commands() => {
                        let res = self.handle_user_commands(command).await;
                        let _ = res.context("Failed to handle user command").inspect_err(|err| {
                            self.handle_error(err);
                        });
                    }
                    _ = self.cancellation_token.cancelled() => {
                        self.is_running.store(false, Ordering::Relaxed);
                        return Ok(())
                    }
                }
            }
        }
    }

    /// Starts a new rpc server that will handle incoming requests from client applications
    pub async fn serve(&self) {
        async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
            tokio::spawn(fut);
        }

        loop {
            let res = tokio::select! {
                res = self.rpc_listener.accept() => res,
                _ = self.cancellation_token.cancelled() => {
                    return
                }
            };

            let conn = match res {
                Ok(c) => c,
                Err(e) => {
                    warn!("There was an error with an incoming connection: {e}");
                    continue;
                }
            };

            let transport =
                serde_transport::new(self.codec_builder.new_framed(conn), Bincode::default());

            let fut = BaseChannel::with_defaults(transport)
                .execute(self.server.clone().serve())
                .for_each(spawn);

            let token = self.cancellation_token.child_token();
            tokio::spawn(async move {
                tokio::select! {
                    _ = fut => {},
                    _ = token.cancelled() => {}
                }
            });
        }
    }

    /// Returns true if the server is running
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::Relaxed)
    }

    /// The list of the synchronized fs
    pub fn synchro_list(&self) -> ReadWriteSynchroList {
        self.synchro_list.clone()
    }

    /// Receives messages of client applications
    pub async fn recv_user_commands(&self) -> Option<UserCommand> {
        let mut receiver = self.commands_chan.lock().await;
        receiver.recv().await
    }

    /// Creates a new synchro
    pub async fn create_synchro(&self, synchro: SynchroCreationRequest) -> Result<()> {
        let info = synchro.info;
        let created = self.synchro_list.insert(info.clone()).await?;

        self.database.insert_synchro(created, info).await?;

        Ok(())
    }

    /// Deletes a synchro
    pub async fn delete_synchro(&self, synchro: SynchroDeletionRequest) -> Result<()> {
        self.synchro_list.remove(synchro.id).await?;
        self.database.delete_synchro(synchro.id).await?;

        Ok(())
    }

    pub async fn update_synchro_state(&self, state_request: StateChangeRequest) -> Result<()> {
        self.synchro_list
            .read()
            .await
            .set_state(state_request.id, state_request.state)
            .await?;
        self.database
            .set_synchro_state(state_request.id, state_request.state)
            .await?;

        Ok(())
    }

    /// Handles messages of client applications
    pub async fn handle_user_commands(&self, command: UserCommand) -> Result<()> {
        match command {
            UserCommand::SynchroCreation(new_synchro) => {
                if let Err(err) = self.create_synchro(new_synchro).await {
                    let wrapped_err = anyhow!(err);
                    error!("Failed to insert new synchro: {wrapped_err:?}");
                    if self.config.error_mode == ErrorMode::Exit {
                        return Err(wrapped_err);
                    }
                }
            }

            UserCommand::SynchroDeletion(to_delete) => {
                if let Err(err) = self.delete_synchro(to_delete).await {
                    let wrapped_err = anyhow!(err);
                    error!("Failed to delete synchro: {wrapped_err:?}");
                    if self.config.error_mode == ErrorMode::Exit {
                        return Err(wrapped_err);
                    }
                }
            }
            UserCommand::StateChange(state_request) => {
                if let Err(err) = self.update_synchro_state(state_request).await {
                    let wrapped_err = anyhow!(err);
                    error!("Failed to set synchro state: {wrapped_err:?}");
                    if self.config.error_mode == ErrorMode::Exit {
                        return Err(wrapped_err);
                    }
                }
            }
            UserCommand::ConflictResolution(conflict) => {
                // TODO: resolve conflict can be slow and should be performed in another task
                // to not block the receiver thread
                let res = self
                    .synchro_list
                    .resolve_conflict(conflict.id, &conflict.path, conflict.side, &self.database)
                    .await;
                if let Err(err) = res {
                    let wrapped_err = anyhow!(err);
                    error!("Failed resolve conflict: {wrapped_err:?}");
                    if self.config.error_mode == ErrorMode::Exit {
                        return Err(wrapped_err);
                    }
                }
            }
        }

        Ok(())
    }
}
