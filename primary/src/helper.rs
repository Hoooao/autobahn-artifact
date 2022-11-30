// Copyright(C) Facebook, Inc. and its affiliates.
use crate::primary::PrimaryMessage;
use bytes::Bytes;
use config::Committee;
use crypto::{Digest, PublicKey};
use log::{error, warn};
use network::SimpleSender;
use store::Store;
use tokio::sync::mpsc::Receiver;

/// A task dedicated to help other authorities by replying to their certificates requests.
pub struct Helper {
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Input channel to receive certificates requests.
    rx_primaries_certs: Receiver<(Vec<Digest>, PublicKey)>,

    /// Input channel to receive certificates requests.
    rx_primaries_headers: Receiver<(Vec<Digest>, PublicKey)>,
    /// A network sender to reply to the sync requests.
    network: SimpleSender,
}

impl Helper {
    pub fn spawn(
        committee: Committee,
        store: Store,
        rx_primaries_certs: Receiver<(Vec<Digest>, PublicKey)>,
        rx_primaries_headers: Receiver<(Vec<Digest>, PublicKey)>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                store,
                rx_primaries_certs,
                rx_primaries_headers,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        loop{
            tokio::select! {
                Some((digests, origin)) = self.rx_primaries_certs.recv() => {
                    // TODO [issue #195]: Do some accounting to prevent bad nodes from monopolizing our resources.
        
                    // get the requestors address.
                    let address = match self.committee.primary(&origin) {
                        Ok(x) => x.primary_to_primary,
                        Err(e) => {
                            warn!("Unexpected certificate request: {}", e);
                            continue;
                        }
                    };
        
                    // Reply to the request (the best we can).
                    for digest in digests {
                        match self.store.read(digest.to_vec()).await {
                            Ok(Some(data)) => {
                                // TODO: Remove this deserialization-serialization in the critical path.
                                let certificate = bincode::deserialize(&data)
                                    .expect("Failed to deserialize our own certificate");
                                let bytes = bincode::serialize(&PrimaryMessage::Certificate(certificate))
                                    .expect("Failed to serialize our own certificate");
                                self.network.send(address, Bytes::from(bytes)).await;
                            }
                            Ok(None) => (),
                            Err(e) => error!("{}", e),
                        }
                    }
                },
                Some((digests, origin)) = self.rx_primaries_headers.recv() => {
                    // TODO [issue #195]: Do some accounting to prevent bad nodes from monopolizing our resources.
        
                    // get the requestors address.
                    let address = match self.committee.primary(&origin) {
                        Ok(x) => x.primary_to_primary,
                        Err(e) => {
                            warn!("Unexpected certificate request: {}", e);
                            continue;
                        }
                    };
        
                    // Reply to the request (the best we can).
                    for digest in digests {
                        match self.store.read(digest.to_vec()).await {
                                Ok(Some(data)) => {
                                    //TODO: Remove this deserialization-serialization in the critical path.
                                    let header = bincode::deserialize(&data)
                                        .expect("Failed to deserialize our own certificate");
                                    let bytes = bincode::serialize(&PrimaryMessage::Header(header))
                                        .expect("Failed to serialize our own certificate");
                                    self.network.send(address, Bytes::from(bytes)).await;
                                }
                                Ok(None) => (),
                                Err(e) => error!("{}", e),
                        }
                    }
                    
                },
            };
        }
       
    }
}
