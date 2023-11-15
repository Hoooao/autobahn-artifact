#![allow(dead_code)]
use std::collections::HashMap;

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header, ConsensusMessage};
use crate::primary::Height;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey, SignatureService, Hash};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

/// The proposer creates new headers and send them to the core for broadcasting and further processing.
pub struct Proposer {
    /// The public key of this primary.
    name: PublicKey,
    /// The committee information
    committee: Committee,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The size of the headers' payload.
    header_size: usize,
    /// The maximum delay to wait for batches' digests.
    max_header_delay: u64,

    /// Receives the parents to include in the next header (along with their round number).
    rx_core: Receiver<Certificate>,
    /// Receives the batches' digests from our workers.
    rx_workers: Receiver<(Digest, WorkerId)>,
    // Receives new consensus instance
    rx_instance: Receiver<ConsensusMessage>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header>,
   
    /// The current height of this validator's chain
    height: Height,
    /// Holds the certificate waiting to be included in the next header
    last_parent: Option<Certificate>,
    // Holds the consensus info for the last special header
    consensus_instances: HashMap<Digest, ConsensusMessage>,
    /// Holds the batches' digests waiting to be included in the next header.
    digests: Vec<(Digest, WorkerId)>,
    /// Keeps track of the size (in bytes) of batches' digests that we received so far.
    payload_size: usize,
}

impl Proposer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        header_size: usize,
        max_header_delay: u64,
        rx_core: Receiver<Certificate>,
        rx_workers: Receiver<(Digest, WorkerId)>,
        rx_instance: Receiver<ConsensusMessage>,
        tx_core: Sender<Header>,
    ) {
        /*let genesis: Vec<Digest> = Certificate::genesis(&committee)
            .iter()
            .map(|x| x.digest())
            .collect();*/

        let genesis = Certificate::genesis_cert(&committee);


        tokio::spawn(async move {
            Self {
                name,
                committee,
                signature_service,
                header_size,
                max_header_delay,
                rx_core,
                rx_workers,
                rx_instance,
                tx_core,
                height: 0,
                last_parent: Some(genesis),
                consensus_instances: HashMap::new(),
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
            }
            .run()
            .await;
        });
    }
    
    async fn make_header(&mut self) {
        // Make a new header.
        let header = Header::new(
                self.name,
                self.height,
                self.digests.drain(..).collect(),
                self.last_parent.clone().unwrap(),
                &mut self.signature_service,
                self.consensus_instances.clone(),
            ).await;

        debug!("Created {:?}", header);

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            // NOTE: This log entry is used to compute performance.
            info!("Created {} -> {:?}", header, digest);
        }

        // Reset last parent
        self.last_parent = None;
        // Reset proposed consensus instances
        self.consensus_instances.clear();
      
        // Send the new header to the `Core` that will broadcast and process it.
        self.tx_core
            .send(header)
            .await
            .expect("Failed to send header");
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        debug!("Dag starting at round {}", self.height);

        let timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(timer);

        loop {
            // Check if we can propose a new header. We propose a new header when one of the following
            // conditions is met:
            // 1. We have a quorum of certificates from the previous round and enough batches' digests;
            // 2. We have a quorum of certificates from the previous round and the specified maximum
            // inter-header delay has passed.
            // 3. If it is a special block opportunity. That is when either a QC or TC from the previous view forms,
            // we have a ticket to propose a new block
            // For both normal blocks and special blocks, delegate the actual sending to the consensus module
            // in other words core should not be disseminating headers
            //let enough_parents = !self.last_parent.is_empty();
            let enough_parent = self.last_parent.is_some();
            let enough_digests = self.payload_size >= self.header_size;
            let timer_expired = timer.is_elapsed();

            if (timer_expired || enough_digests) && enough_parent {
                if timer_expired {
                    debug!("Timer expired for height {}", self.height);
                }

                // Make a new header.
                self.make_header().await;
                self.payload_size = 0;

                // Reschedule the timer.
                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);
            }

            tokio::select! {
                // Received info from consensus
                Some(info) = self.rx_instance.recv() => {
                    debug!("received consensus info");
                    self.consensus_instances.insert(info.digest(), info);
                }

                // Receive own certificate from core (we are the author)
                Some(parent) = self.rx_core.recv() => {
                    debug!("   received parent from height {:?}", parent.height);

                    if parent.height < self.height {
                        continue;
                    }

                    // Advance to the next height.
                    self.height += 1;
                    debug!("Chain moved to height {}", self.height);

                    // Signal that we have a parent certificates to propose a new header.
                    self.last_parent = Some(parent.clone());
                }

                Some((digest, worker_id)) = self.rx_workers.recv() => {
                    println!("   received payload from worker {}", worker_id);
                    self.payload_size += digest.size();
                    self.digests.push((digest, worker_id));
                }
                () = &mut timer => {
                    // Nothing to do.
                }
            }
        }
    }
}
