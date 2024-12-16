// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::NetworkError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::SplitSink;
use futures::stream::StreamExt as _;
use log::{debug, info, warn};
use std::error::Error;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use crypto::{Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::ed25519;

#[cfg(test)]
#[path = "tests/receiver_tests.rs"]
pub mod receiver_tests;

/// Convenient alias for the writer end of the TCP channel.
pub type Writer = SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>;

#[async_trait]
pub trait MessageHandler: Clone + Send + Sync + 'static {
    /// Defines how to handle an incoming message. A typical usage is to define a `MessageHandler` with a
    /// number of `Sender<T>` channels. Then implement `dispatch` to deserialize incoming messages and
    /// forward them through the appropriate delivery channel. Then `writer` can be used to send back
    /// responses or acknowledgements to the sender machine (see unit tests for examples).
    async fn dispatch(&self, writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>>;
}

/// For each incoming request, we spawn a new runner responsible to receive messages and forward them
/// through the provided deliver channel.
pub struct Receiver<Handler: MessageHandler> {
    /// Address to listen to.
    address: SocketAddr,
    /// Struct responsible to define how to handle received messages.
    handler: Handler,
    client_pub_key: PublicKey,
}

impl<Handler: MessageHandler> Receiver<Handler> {
    /// Spawn a new network receiver handling connections from any incoming peer.
    pub fn spawn(address: SocketAddr, handler: Handler, client_pub_key: Option<PublicKey>) {
        tokio::spawn(async move {
            Self { address, handler, client_pub_key}.run().await;
        });
    }

    /// Main loop responsible to accept incoming connections and spawn a new runner to handle it.
    async fn run(&self) {
        let listener = TcpListener::bind(&self.address)
            .await
            .expect("Failed to bind TCP port");

        debug!("Listening on {}", self.address);
        loop {
            let (socket, peer) = match listener.accept().await {
                Ok(value) => value,
                Err(e) => {
                    warn!("{}", NetworkError::FailedToListen(e));
                    continue;
                }
            };
            info!("Incoming connection established with {}", peer);
            Self::spawn_runner(socket, peer, self.client_pub_key.clone(),self.handler.clone()).await;
        }
    }

    /// Spawn a new runner to handle a specific TCP connection. It receives messages and process them
    /// using the provided handler.
    async fn spawn_runner(socket: TcpStream, peer: SocketAddr, pub_key: PublicKey, handler: Handler) {
        tokio::spawn(async move {
            let transport = Framed::new(socket, LengthDelimitedCodec::new());
            let (mut writer, mut reader) = transport.split();
            debug!("network: peer address receive: {}", peer);
            while let Some(frame) = reader.next().await {
                match frame.map_err(|e| NetworkError::FailedToReceiveMessage(peer, e)) {
                    Ok(x) => {
                        // Verify client signature here
                        let (msg, sig) = x.split_at(x.len() - 64); 
                        
                        let digest = msg.digest();

                        let signature = ed25519::signature::Signature::from_bytes(sig).expect("Failed to create sig");
                        let key = ed25519_dalek::PublicKey::from_bytes(&pub_key.0).expect("Failed to load pub key");
                        
                        match key.verify_strict(&digest.0, &signature) {
                            Ok(()) => {
                                debug!("Transaction from {} verified", peer);
                                if let Err(e) = handler.dispatch(&mut writer, msg.freeze()).await {
                                    warn!("{}", e);
                                    return;
                                }
                            }
                            Err(er) => {
                                debug!("Failed to verify client transaction {}", er);
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        warn!("{}", e);
                        return;
                    }
                }
            }
            warn!("Connection closed by peer {}", peer);
        });
    }
}
