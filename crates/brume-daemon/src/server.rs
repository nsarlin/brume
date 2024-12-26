//! Handle connections to the daemon

use std::{future::Future, io, sync::Arc};

use anyhow::{anyhow, Context, Result};

use futures::StreamExt;
use interprocess::local_socket::{
    tokio::Listener, traits::tokio::Listener as _, GenericNamespaced, ListenerOptions, ToNsName,
};
use log::{error, info, warn};
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
    daemon::BrumeDaemon,
    protocol::{AnyFsCreationInfo, BrumeService, BRUME_SOCK_NAME},
    synchro_list::ReadWriteSynchroList,
};

/// A Server that handle RPC connections from client applications
pub struct Server {
    codec_builder: Builder,
    rpc_listener: Listener,
    synchro_list: ReadWriteSynchroList,
    daemon: BrumeDaemon,
    from_daemon: Arc<Mutex<UnboundedReceiver<(AnyFsCreationInfo, AnyFsCreationInfo)>>>,
}

impl Server {
    pub fn new() -> Result<Self> {
        let name = BRUME_SOCK_NAME
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
        let (to_server, from_daemon) = unbounded_channel();

        let synchro_list = ReadWriteSynchroList::new();

        let daemon = BrumeDaemon::new(to_server, synchro_list.as_read_only());

        Ok(Self {
            codec_builder,
            rpc_listener: listener,
            synchro_list,
            daemon,
            from_daemon: Arc::new(Mutex::new(from_daemon)),
        })
    }

    /// Handle client connections and periodically synchronize filesystems
    pub async fn run(self: Arc<Self>) -> Result<()> {
        // Handle connections from client apps
        {
            let server = self.clone();
            tokio::spawn(async move { server.serve().await });
        }

        // Synchronize all filesystems
        // TODO: configure duration
        let mut interval = time::interval(time::Duration::from_secs(10));
        loop {
            info!("Starting full sync for all filesystems");
            let synchro_list = self.synchro_list();

            let results = synchro_list.sync_all().await;

            for res in results {
                if let Err(err) = res {
                    let wrapped_err = anyhow!(err);
                    error!("Failed to synchronize filesystems: {wrapped_err:?}")
                }
            }

            // Wait and update synchro list with any new sync from user
            loop {
                tokio::select! {
                    _ = interval.tick() => break,
                    _ = self.update_synchro_list() => continue
                }
            }
        }
    }

    /// Start a new rpc server that will handle incoming requests from client applications
    pub async fn serve(&self) -> Result<()> {
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
                .execute(self.daemon.clone().serve())
                .for_each(spawn);
            tokio::spawn(fut);
        }
    }

    /// The list of the synchronized fs
    pub fn synchro_list(&self) -> ReadWriteSynchroList {
        self.synchro_list.clone()
    }

    pub async fn update_synchro_list(&self) {
        let mut new_synchros = Vec::new();
        {
            let mut recver = self.from_daemon.lock().await;
            recver.recv_many(&mut new_synchros, 100).await; // TODO: configure receive size ?
        }

        {
            for (local, remote) in new_synchros {
                if let Err(err) = self.synchro_list.insert(local, remote).await {
                    let wrapped_err = anyhow!(err);
                    error!("Failed insert new synchro: {wrapped_err:?}")
                }
            }
        }
    }
}
