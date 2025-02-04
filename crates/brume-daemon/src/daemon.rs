//! Handles the list of [`brume::synchro::Synchro`] in the background
//!
//! The daemon can be queried through the [`Server`] by multiple client applications to add or
//! remove filesystem pairs to synchronize. It regularly synchronizes them.

use std::{
    fmt::Display,
    future::Future,
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::{anyhow, Context, Result};

use brume::{synchro::FullSyncStatus, vfs::VirtualPathBuf};
use futures::StreamExt;
use interprocess::local_socket::{
    tokio::Listener, traits::tokio::Listener as _, GenericNamespaced, ListenerOptions, ToNsName,
};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use tarpc::{
    serde_transport,
    server::{BaseChannel, Channel},
    tokio_serde::formats::Bincode,
    tokio_util::codec::{length_delimited::Builder, LengthDelimitedCodec},
};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver},
        Mutex,
    },
    time,
};

use crate::{
    protocol::{AnySynchroCreationInfo, BrumeService, SynchroId, SynchroSide, BRUME_SOCK_NAME},
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
}

impl Default for DaemonConfig {
    fn default() -> Self {
        Self {
            sync_interval: Duration::from_secs(10),
            error_mode: ErrorMode::default(),
            sock_name: BRUME_SOCK_NAME.to_string(),
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
}

/// How errors should be handled by the daemon
#[derive(Default, Copy, Clone, PartialEq, Eq)]
pub enum ErrorMode {
    #[default]
    Log,
    Exit,
}

/// Computed status of the synchro, based on synchronization events
///
/// This status is mostly gotten from the [`FullSyncStatus`] returned by [`full_sync`]. When
/// [`full_sync`] is running, the status is set to [`Self::SyncInProgress`]
///
/// [`full_sync`]: brume::synchro::Synchro::full_sync
#[derive(Default, Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SynchroStatus {
    /// No node in any FS is in Conflict or Error state
    #[default]
    Ok,
    /// At least one node is in Conflict state, but no node is in Error state
    Conflict,
    /// At least one node is in Error state
    Error,
    /// There is some inconsistency in one of the Vfs, likely coming from a bug in brume.
    /// User should re-sync the faulty vfs from scratch
    Desync,
    /// A synchronization is in progress
    SyncInProgress,
}

impl Display for SynchroStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<FullSyncStatus> for SynchroStatus {
    fn from(value: FullSyncStatus) -> Self {
        match value {
            FullSyncStatus::Ok => Self::Ok,
            FullSyncStatus::Conflict => Self::Conflict,
            FullSyncStatus::Error => Self::Error,
            FullSyncStatus::Desync => Self::Desync,
        }
    }
}

impl SynchroStatus {
    /// Returns true if the status allows further synchronization
    pub fn is_synchronizable(self) -> bool {
        !matches!(self, SynchroStatus::Desync | SynchroStatus::SyncInProgress)
    }
}

/// User controlled state of the synchro
#[derive(Default, Copy, Clone, Debug, Serialize, Deserialize)]
pub enum SynchroState {
    #[default]
    Running,
    Paused,
}

impl Display for SynchroState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// A user request to change the [`SynchroState`] of a synchro
pub struct StateChangeRequest {
    id: SynchroId,
    state: SynchroState,
}

impl StateChangeRequest {
    pub fn new(id: SynchroId, state: SynchroState) -> Self {
        Self { id, state }
    }
}

/// A use request for a conflict resolution
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
    creation_chan: Arc<Mutex<UnboundedReceiver<AnySynchroCreationInfo>>>,
    deletion_chan: Arc<Mutex<UnboundedReceiver<SynchroId>>>,
    state_change_chan: Arc<Mutex<UnboundedReceiver<StateChangeRequest>>>,
    conflict_resolution_chan: Arc<Mutex<UnboundedReceiver<ConflictResolutionRequest>>>,
    is_running: AtomicBool,
    config: DaemonConfig,
}

impl Daemon {
    pub fn new(config: DaemonConfig) -> Result<Self> {
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
Please check if {BRUME_SOCK_NAME} is in use by another process and try again."
                );
                return Err(e).context("Failed to start server");
            }
            x => x?,
        };

        info!("Server running at {BRUME_SOCK_NAME}");

        let codec_builder = LengthDelimitedCodec::builder();
        let (creation_to_daemon, creation_from_server) = unbounded_channel();
        let (deletion_to_daemon, deletion_from_server) = unbounded_channel();
        let (state_change_to_daemon, state_change_from_server) = unbounded_channel();
        let (resolution_to_daemon, resolution_from_server) = unbounded_channel();

        let synchro_list = ReadWriteSynchroList::new();

        let server = Server::new(
            creation_to_daemon,
            deletion_to_daemon,
            state_change_to_daemon,
            resolution_to_daemon,
            synchro_list.as_read_only(),
        );

        Ok(Self {
            codec_builder,
            rpc_listener: listener,
            synchro_list,
            server,
            creation_chan: Arc::new(Mutex::new(creation_from_server)),
            deletion_chan: Arc::new(Mutex::new(deletion_from_server)),
            state_change_chan: Arc::new(Mutex::new(state_change_from_server)),
            conflict_resolution_chan: Arc::new(Mutex::new(resolution_from_server)),
            is_running: AtomicBool::new(false),
            config,
        })
    }

    /// Handle client connections and periodically synchronize filesystems
    pub async fn run(self: Arc<Self>) -> Result<()> {
        self.is_running.store(true, Ordering::Relaxed);
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

            let results = synchro_list.sync_all().await;

            for res in results {
                if let Err(err) = res {
                    let wrapped_err = anyhow!(err);
                    error!("Failed to synchronize filesystems: {wrapped_err:?}");
                    if self.config.error_mode == ErrorMode::Exit {
                        self.is_running.store(false, Ordering::Relaxed);
                        return Err(wrapped_err);
                    }
                }
            }

            // Wait and update synchro list with any new sync from user
            loop {
                tokio::select! {
                    _ = interval.tick() => break,
                    res = self.handle_user_commands() => {
                        res.inspect_err(|_| {
                            self.is_running.store(false, Ordering::Relaxed);
                        })?
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
            let conn = match self.rpc_listener.accept().await {
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
            tokio::spawn(fut);
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

    pub async fn recv_creations(&self) -> Option<AnySynchroCreationInfo> {
        let mut receiver = self.creation_chan.lock().await;
        receiver.recv().await
    }

    pub async fn recv_deletions(&self) -> Option<SynchroId> {
        let mut receiver = self.deletion_chan.lock().await;
        receiver.recv().await
    }

    pub async fn recv_state_changes(&self) -> Vec<StateChangeRequest> {
        let mut list = Vec::new();
        let mut receiver = self.state_change_chan.lock().await;
        receiver.recv_many(&mut list, 100).await; // TODO: configure recv size ?
        list
    }

    pub async fn recv_conflict_resolutions(&self) -> Option<ConflictResolutionRequest> {
        let mut receiver = self.conflict_resolution_chan.lock().await;
        receiver.recv().await
    }

    /// Handles messages of client applications
    pub async fn handle_user_commands(&self) -> Result<()> {
        tokio::select! {
            Some(new_synchro) = self.recv_creations() => {
                if let Err(err) = self.synchro_list.insert(new_synchro).await {
                    let wrapped_err = anyhow!(err);
                    error!("Failed insert new synchro: {wrapped_err:?}");
                    if self.config.error_mode == ErrorMode::Exit {
                        return Err(wrapped_err);
                    }
                }
            }
            Some(to_delete) = self.recv_deletions() => {
                if let Err(err) = self.synchro_list.remove(to_delete).await {
                    let wrapped_err = anyhow!(err);
                    error!("Failed delete synchro: {wrapped_err:?}");
                    if self.config.error_mode == ErrorMode::Exit {
                        return Err(wrapped_err);
                    }
                }
            }
                        states_to_change = self.recv_state_changes() => {
                for state_request in states_to_change {
                    if let Err(err) = self.synchro_list
                                                .read()
                                                .await
                                                .set_state(state_request.id, state_request.state).await {
                        let wrapped_err = anyhow!(err);
                        error!("Failed to set synchro state: {wrapped_err:?}");
                        if self.config.error_mode == ErrorMode::Exit {
                            return Err(wrapped_err);
                        }
                                                                                                }
                                }
                        }
            Some(conflict) = self.recv_conflict_resolutions() => {
                let res = self.synchro_list.resolve_conflict(conflict.id, &conflict.path,
                                                             conflict.side).await;
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
