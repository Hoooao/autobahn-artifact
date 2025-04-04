// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use crate::worker::WorkerMessage;
use config::WorkerId;
use crypto::Digest;
use crypto::Hash as _;
use ed25519_dalek::ed25519;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use primary::WorkerPrimaryMessage;
use tokio::sync::mpsc;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use log::debug;
use futures::future::try_join_all;

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedBatchMessage = Vec<u8>;

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // Our worker's id.
        id: WorkerId,
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<SerializedBatchMessage>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<SerializedBatchDigestMessage>,    //sender channel connects to PrimaryConnector
        // Whether we are processing our own batches or the batches of other nodes.
        own_digest: bool,
    ) {
        let (tx_verified, mut rx_verified) = mpsc::channel(100);
        tokio::spawn(async move {
            while let Some(serialized) = rx_batch.recv().await {
                // Hash the batch.
                if own_digest {
                    tx_verified
                        .send(serialized)
                        .await
                        .expect("Failed to send batch to be verified");
                    continue;
                }
                if let WorkerMessage::Batch(id, batch) = bincode::deserialize(&serialized).expect("Failed to deserialize batch") {
                    let key = ed25519_dalek::PublicKey::from_bytes(&id.0).expect("Failed to load pub key");
                    let mut handles = Vec::new();
                    for tx in batch.into_iter() {
                        let handle = tokio::task::spawn_blocking(move || {
                            let (msg, sig) = tx.split_at(tx.len() - 64);
                            let digest = msg.digest();
                            let signature = ed25519::signature::Signature::from_bytes(sig)
                                .expect("Failed to create sig");
                            key.verify_strict(&digest.0, &signature)
                                .map_err(|e| format!("Signature failed: {}", e))
                        });
                        handles.push(handle);
                    }
                    // Await all signature verifications
                    let results = try_join_all(handles).await;
                    match results {
                        Ok(verifications) => {
                            if verifications.iter().all(|r| r.is_ok()) {
                                debug!("All client transactions verified");
                                tx_verified.send(serialized).await
                                    .expect("Failed to send batch to be verified");
                            } else {
                                debug!("Some signatures failed: {:?}", verifications);
                                return;
                            }
                        }
                        Err(e) => {
                            debug!("A blocking task panicked or failed: {:?}", e);
                            return;
                        }
                    }
                }
            }
        });
        tokio::spawn(async move {
            while let Some(batch) = rx_verified.recv().await {
                let digest = Digest(Sha512::digest(&batch).as_slice()[..32].try_into().unwrap());
                debug!("Processor received batch {:?}", digest);

                // Store the batch.
                store.write(digest.to_vec(), batch).await;

                // Deliver the batch's digest.
                let message = match own_digest {
                    true => WorkerPrimaryMessage::OurBatch(digest, id),
                    false => WorkerPrimaryMessage::OthersBatch(digest, id),
                };
                let message = bincode::serialize(&message)
                    .expect("Failed to serialize our own worker-primary message");
                tx_digest
                    .send(message)
                    .await
                    .expect("Failed to send digest");
            }
        });
    }
}
