use consensus::messages::Transaction;
use futures::stream::StreamExt as _;
use log::{debug, warn};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use crypto::{Hash, PublicKey, Signature, SignatureService};
use ed25519_dalek::ed25519;

pub struct Front {
    address: SocketAddr,
    deliver: Sender<Transaction>,
    client_pub_key: PublicKey
}

impl Front {
    pub fn new(address: SocketAddr, deliver: Sender<Transaction>, client_pub_key: PublicKey) -> Self {
        Self { address, deliver, client_pub_key }
    }

    // For each incoming request, we spawn a new worker responsible to receive
    // messages and replay them through the provided deliver channel.
    pub async fn run(&self) {
        let listener = TcpListener::bind(&self.address)
            .await
            .expect("Failed to bind to TCP port");
        

        warn!("Listening for client transactions on {}", self.address);
        loop {
            let (socket, peer) = match listener.accept().await {
                Ok(value) => value,
                Err(e) => {
                    warn!("Failed to connect with client: {}", e);
                    continue;
                }
            };
            warn!("Connection established with client {}", peer);
            Self::spawn_worker(socket, peer, self.client_pub_key.clone(), self.deliver.clone()).await;
        }
    }

    async fn spawn_worker(socket: TcpStream, peer: SocketAddr, pub_key: PublicKey, deliver: Sender<Transaction>) {
        tokio::spawn(async move {
            let mut transport = Framed::new(socket, LengthDelimitedCodec::new());
            while let Some(frame) = transport.next().await {
                match frame {
                    Ok(x) => {
                        // Verify client signature here
                        let (msg, sig) = x.split_at(x.len() - 64); 
                        
                        let digest = msg.digest();

                        let signature = ed25519::signature::Signature::from_bytes(sig).expect("Failed to create sig");
                        let key = ed25519_dalek::PublicKey::from_bytes(&pub_key.0).expect("Failed to load pub key");
                        
                        match key.verify_strict(&digest.0, &signature) {
                            Ok(()) => {
                                debug!("Client transaction verified");
                                deliver.send(x.to_vec()).await.expect("Core channel closed");
                            }
                            Err(e) => {
                                debug!("Failed to verify client transaction {}", e);
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to receive client transaction: {}", e);
                        return;
                    }
                }
            }
            debug!("Connection closed by client {}", peer);
        });
    }
}
