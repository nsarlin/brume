//! Handle connections to the daemon

use std::{future::Future, io};

use anyhow::{Context, Result};

use brume_daemon::BrumeService;
use futures::StreamExt;
use interprocess::local_socket::{
    tokio::Listener, traits::tokio::Listener as _, GenericNamespaced, ListenerOptions, ToNsName,
};
use tarpc::{
    serde_transport,
    server::{BaseChannel, Channel},
    tokio_serde::formats::Bincode,
    tokio_util::codec::{length_delimited::Builder, LengthDelimitedCodec},
};

use crate::{daemon::BrumeDaemon, BRUME_SOCK_NAME};

/// A Server that handle RPC connections from client applications
pub(crate) struct Server {
    codec_builder: Builder,
    listener: Listener,
    daemon: BrumeDaemon,
}

impl Server {
    pub(crate) fn new() -> Result<Self> {
        let name = BRUME_SOCK_NAME
            .to_ns_name::<GenericNamespaced>()
            .context("Invalid name for sock")?;
        let opts = ListenerOptions::new().name(name);
        let listener = match opts.create_tokio() {
            Err(e) if e.kind() == io::ErrorKind::AddrInUse => {
                return Err(e).context(
                    "Error: could not start server because the socket file is occupied. \
Please check if {BRUME_SOCK_NAME} is in use by another process and try again.",
                );
            }
            x => x?,
        };

        eprintln!("Server running at {BRUME_SOCK_NAME}");

        let codec_builder = LengthDelimitedCodec::builder();
        let daemon = BrumeDaemon::new();

        Ok(Self {
            codec_builder,
            listener,
            daemon,
        })
    }

    /// Start a new rpc server that will handle incoming requests from client applications
    pub(crate) async fn serve(&self) -> Result<()> {
        async fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
            tokio::spawn(fut);
        }

        loop {
            let conn = match self.listener.accept().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("There was an error with an incoming connection: {e}");
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

    pub(crate) fn daemon(&self) -> &BrumeDaemon {
        &self.daemon
    }
}
