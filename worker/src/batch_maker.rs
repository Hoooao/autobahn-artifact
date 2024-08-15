// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::{Hash, PublicKey};
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
use log::debug;
#[cfg(feature = "benchmark")]
use log::info;
use network::{ReliableSender, SimpleSender};
use std::collections::HashSet;
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
//use timer::Timer;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::{Future, StreamExt};
use std::pin::Pin;
use crate::timer::Timer;
use std::collections::VecDeque;

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

pub type Transaction = Vec<u8>;
pub type Batch = Vec<Transaction>;

#[derive(Clone, PartialEq, std::fmt::Debug)]
pub enum AsyncEffectType {
    Off = 0,
    TempBlip = 1, //Send nothing for x seconds, and then release all messages
    Failure = 2, //Send nothing for x seconds  //TODO: Combine with TempBlip?
    Partition = 3, //Send nothing to partitioned replicas for x seconds, then release all
    Egress = 4,  //For x seconds, delay all outbound messages by some amount
}
fn uint_to_enum(v: u8) -> AsyncEffectType {
    unsafe { std::mem::transmute(v) }
}

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
    // /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    // tx_message: Sender<QuorumWaiterMessage>,
    //tx_message: Sender<QuorumWaiterMessage>,  /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_batch: Sender<Vec<u8>>,   // channel to forward batch digest to processor in order for primary to propose.
    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: SimpleSender,
    // Currently during asynchrony
    during_simulated_asynchrony: bool,
    // Partition public keys
    partition_public_keys: HashSet<PublicKey>,
    // timer futures
    //partition_timer_futures: FuturesUnordered<Pin<Box<dyn Future<Output = ()> + Send>>>,
    //Simulating an async event
    pub simulate_asynchrony: bool, 
    //Type of effects: 0 for delay full async duration, 1 for partition, 2 for  failure, 3 for egress delay. Will start #type many blips.
    pub asynchrony_type: VecDeque<u8>, 
    //Start of async period   //offset from current time (in seconds) when to start next async effect
    pub asynchrony_start: VecDeque<u64>,     
    //Duration of async period
    pub asynchrony_duration: VecDeque<u64>,  
    ////first k nodes experience specified async behavior
    pub affected_nodes: VecDeque<u64>, 
    ////public keys of the other works
    pub keys: Vec<PublicKey>,
    //name of the worker
    pub name: PublicKey,
    // async timer futures
    pub async_timer_futures: FuturesUnordered<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>,
        //tx_message: Sender<QuorumWaiterMessage>, //sender channel to worker.QuorumWaiter
        tx_batch: Sender<Vec<u8>>,   // sender channel to worker.Processor
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
        //partition_public_keys: HashSet<PublicKey>,

        simulate_asynchrony: bool,
        asynchrony_type: VecDeque<u8>,
        asynchrony_start: VecDeque<u64>,
        asynchrony_duration: VecDeque<u64>,
        affected_nodes: VecDeque<u64>,
        keys: Vec<PublicKey>,
        name: PublicKey,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                //tx_message, //previously forwarded batch to Quorum_waiter; now skipping this step.
                tx_batch,  
                workers_addresses,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_size: 0,
                network: SimpleSender::new(),
                during_simulated_asynchrony: false,
                partition_public_keys: HashSet::new(),
                //partition_timer_futures: FuturesUnordered::new(),
                simulate_asynchrony,
                asynchrony_type,
                asynchrony_start,
                asynchrony_duration,
                affected_nodes,
                keys,
                name,
                async_timer_futures: FuturesUnordered::new(),
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);

        if self.simulate_asynchrony {
            for i in 0..self.asynchrony_start.len() {
                let start_offset = self.asynchrony_start[i];
                let end_offset = start_offset +  self.asynchrony_duration[i];
                            
                let async_start = Timer::new(start_offset);
                let async_end = Timer::new(end_offset);

                self.async_timer_futures.push(Box::pin(async_start));
                self.async_timer_futures.push(Box::pin(async_end));
                
                if uint_to_enum(self.asynchrony_type[i]) == AsyncEffectType::Partition {
                    self.keys.sort();
                    let index = self.keys.binary_search(&self.name).unwrap();

                    // Figure out which partition we are in, partition_nodes indicates when the left partition ends
                    let mut start: usize = 0;
                    let mut end: usize = 0;
                
                    // We are in the right partition
                    if index > self.affected_nodes[i] as usize - 1 {
                        start = self.affected_nodes[i] as usize;
                        end = self.keys.len();
                    
                    } else {
                        // We are in the left partition
                        start = 0;
                        end = self.affected_nodes[i] as usize;
                    }

                    // These are the nodes in our side of the partition
                    for j in start..end {
                        self.partition_public_keys.insert(self.keys[j]);
                    }

                    debug!("partition pks are {:?}", self.partition_public_keys);
                }
            }
        }

        /*let timer1 = sleep(Duration::from_secs(10));
        tokio::pin!(timer1);
        let timer2 = sleep(Duration::from_secs(30));
        tokio::pin!(timer2);*/
        /*let timer1 = Timer::new(10_000);
        let timer2 = Timer::new(30_000);
        self.partition_timer_futures.push(Box::pin(timer1));
        self.partition_timer_futures.push(Box::pin(timer2));*/

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction);
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                /*Some(()) = self.partition_timer_futures.next() => {
                    debug!("BatchMaker: partition delay timer triggered");
                    self.during_simulated_asynchrony = !self.during_simulated_asynchrony;
                    //timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                },*/

                Some(()) = self.async_timer_futures.next() => {
                    self.during_simulated_asynchrony = !self.during_simulated_asynchrony;
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        self.seal().await;
                    }
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }

                // If the timer triggers, seal the batch even if it contains few transactions.
                /*() = &mut timer1 => {
                    debug!("BatchMaker: partition delay timer 1 triggered");
                    self.during_simulated_asynchrony = true;
                    timer1.as_mut().reset(Instant::now() + Duration::from_secs(100));
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer2 => {
                    debug!("BatchMaker: partition delay timer 2 triggered");
                    //debug!("partition queue size is {:?}", self.partition_queue.len());
                    self.during_simulated_asynchrony = false;
                    timer2.as_mut().reset(Instant::now() + Duration::from_secs(100));
                },*/
                
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        #[cfg(feature = "benchmark")]
        let size = self.current_batch_size;

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx[0] == 0u8 && tx.len() > 8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        // Serialize the batch.
        self.current_batch_size = 0;
        let batch: Vec<_> = self.current_batch.drain(..).collect();
        let message = WorkerMessage::Batch(batch);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");

        #[cfg(feature = "benchmark")]
        {
            // NOTE: This is one extra hash that is only needed to print the following log entries.
            let digest = Digest(
                Sha512::digest(&serialized).as_slice()[..32]
                    .try_into()
                    .unwrap(),
            );

            for id in tx_ids {
                // NOTE: This log entry is used to compute performance.
                info!(
                    "Batch {:?} contains sample tx {}",
                    digest,
                    u64::from_be_bytes(id)
                );
            }

            // NOTE: This log entry is used to compute performance.
            info!("Batch {:?} contains {} B", digest, size);
        }

        // Broadcast the batch through the network.

        //NEW:
        //Best-effort broadcast only. Any failure is correlated with the primary operating this node (running on same machine)
        //let (_, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        //self.network.broadcast(addresses, bytes).await; 

        self.tx_batch.send(serialized).await.expect("Failed to deliver batch");

        if self.during_simulated_asynchrony {
            debug!("BatchMaker: Simulated asynchrony enabled. Only sending to partitioned keys from broadcast");
            let new_addresses: Vec<_> = self.workers_addresses.iter().filter(|(pk, _)| self.partition_public_keys.contains(pk)).map(|(_, addr)| addr).cloned().collect();
            //let (_, addresses) = new_addresses.iter().cloned().unzip();
            //debug!("addresses is {:?}", new_addresses);
            //self.partition_queue.push_back(message);
            //debug!("partition queue size is {:?}", self.partition_queue.len());
            //self.network.broadcast(new_addresses, bytes).await; 
        } else {
            //debug!("sending batch normally");
            let (_, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
            self.network.broadcast(addresses, bytes).await; 
        }

        //OLD:
        //This uses reliable sender. The receiver worker will reply with an ack. The Reply Handler is passed to Quorum Waiter.

        // let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        // let bytes = Bytes::from(serialized.clone());
        // let handlers = self.network.broadcast(addresses, bytes).await;

        // // Send the batch through the deliver channel for further processing.
        // self.tx_message
        //     .send(QuorumWaiterMessage {
        //         batch: serialized,
        //         handlers: names.into_iter().zip(handlers.into_iter()).collect(),
        //     })
        //     .await
        //     .expect("Failed to deliver batch");
    }
}