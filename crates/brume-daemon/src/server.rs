//! Handle connections to the daemon

use std::{collections::HashMap, future::Future, io, sync::Arc};

use anyhow::{Context, Result};

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
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver},
    Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard,
};

use crate::{
    daemon::{AnySynchro, BrumeDaemon},
    protocol::{BrumeService, SynchroId, BRUME_SOCK_NAME},
};

#[derive(Clone)]
pub struct ReadOnlySynchroList {
    list: Arc<RwLock<HashMap<SynchroId, Mutex<AnySynchro>>>>,
}

impl ReadOnlySynchroList {
    pub async fn read(&self) -> RwLockReadGuard<HashMap<SynchroId, Mutex<AnySynchro>>> {
        self.list.read().await
    }
}

#[derive(Clone)]
pub struct SynchroList {
    list: Arc<RwLock<HashMap<SynchroId, Mutex<AnySynchro>>>>,
}

impl Default for SynchroList {
    fn default() -> Self {
        Self::new()
    }
}

impl SynchroList {
    pub async fn read(&self) -> RwLockReadGuard<HashMap<SynchroId, Mutex<AnySynchro>>> {
        self.list.read().await
    }

    pub async fn write(&self) -> RwLockWriteGuard<HashMap<SynchroId, Mutex<AnySynchro>>> {
        self.list.write().await
    }

    pub fn as_read_only(&self) -> ReadOnlySynchroList {
        ReadOnlySynchroList {
            list: self.list.clone(),
        }
    }

    pub fn new() -> Self {
        Self {
            list: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

/// A Server that handle RPC connections from client applications
pub struct Server {
    codec_builder: Builder,
    rpc_listener: Listener,
    synchro_list: SynchroList,
    daemon: BrumeDaemon,
    from_daemon: Arc<Mutex<UnboundedReceiver<(SynchroId, AnySynchro)>>>,
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

        let synchro_list = SynchroList::new();

        let daemon = BrumeDaemon::new(to_server, synchro_list.as_read_only());

        Ok(Self {
            codec_builder,
            rpc_listener: listener,
            synchro_list,
            daemon,
            from_daemon: Arc::new(Mutex::new(from_daemon)),
        })
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
    pub fn synchro_list(&self) -> SynchroList {
        self.synchro_list.clone()
    }

    pub async fn update_synchro_list(&self) {
        let mut new_synchros = Vec::new();
        {
            let mut recver = self.from_daemon.lock().await;
            recver.recv_many(&mut new_synchros, 100).await; // TODO: configure receive size ?
        }

        {
            let mut list = self.synchro_list.write().await;
            for (id, synchro) in new_synchros {
                let synchro = Mutex::new(synchro);
                list.insert(id, synchro);
            }
        }
    }
}
