// Copyright(C) Facebook, Inc. and its affiliates.
use crate::aggregators::{CertificatesAggregator, VotesAggregator};
use crate::error::{DagError, DagResult};
use crate::messages::{Certificate, Header, Vote};
use crate::primary::{PrimaryMessage, Round};
use crate::timer::Timer;
use crate::synchronizer::Synchronizer;
use async_recursion::async_recursion;
use bytes::Bytes;
use config::Committee;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use futures::stream::FuturesUnordered;
use futures::{Future, StreamExt};
use log::{debug, error, warn};
use network::{CancelHandler, ReliableSender};
use tokio_util::time::DelayQueue;
use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::Instant;


#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

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

pub struct Core {
    /// The public key of this primary.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Handles synchronization with other nodes and our workers.
    synchronizer: Synchronizer,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// The depth of the garbage collector.
    gc_depth: Round,

    /// Receiver for dag messages (headers, votes, certificates).
    rx_primaries: Receiver<PrimaryMessage>,
    /// Receives loopback headers from the `HeaderWaiter`.
    rx_header_waiter: Receiver<Header>,
    /// Receives loopback certificates from the `CertificateWaiter`.
    rx_certificate_waiter: Receiver<Certificate>,
    /// Receives our newly created headers from the `Proposer`.
    rx_proposer: Receiver<Header>,
    /// Output all certificates to the consensus layer.
    tx_consensus: Sender<Certificate>,
    /// Send valid a quorum of certificates' ids to the `Proposer` (along with their round).
    tx_proposer: Sender<(Vec<Certificate>, Round)>,

    /// The last garbage collected round.
    gc_round: Round,
    /// The authors of the last voted headers.
    last_voted: HashMap<Round, HashSet<PublicKey>>,
    /// The set of headers we are currently processing.
    processing: HashMap<Round, HashSet<Digest>>,
    /// The last header we proposed (for which we are waiting votes).
    current_header: Header,
    /// Aggregates votes into a certificate.
    votes_aggregator: VotesAggregator,
    /// Aggregates certificates to use as parents for new headers.
    certificates_aggregators: HashMap<Round, Box<CertificatesAggregator>>,
    /// A network sender to send the batches to the other workers.
    network: ReliableSender,
    /// Keeps the cancel handlers of the messages we sent.
    cancel_handlers: HashMap<Round, Vec<CancelHandler>>,
    // Framework for simulating asynchrony
    simulate_asynchrony: bool, //Simulating an async event
    asynchrony_type: VecDeque<AsyncEffectType>, //Type of effects: 0 for delay full async duration, 1 for partition, 2 for  failure, 3 for egress delay. Will start #type many blips.
    asynchrony_start: VecDeque<u64>,     //Start of async period   //offset from current time (in seconds) when to start next async effect
    asynchrony_duration: VecDeque<u64>,  //Duration of async period
    affected_nodes: VecDeque<u64>, ////first k nodes experience specified async behavior.
    during_simulated_asynchrony: bool,  //Currently in async period?
    current_effect_type: AsyncEffectType, //Currently active effect.

    async_timer_futures: FuturesUnordered<Pin<Box<dyn Future<Output = ()> + Send>>>, //Used to turn on/off period  //Note: (slot, view) are not needed, it's just to re-use existing Timer
    already_set_timers: bool, //To avoid setting multiple timers
    current_time: Instant,

    // For partition
    partition_public_keys: HashSet<PublicKey>,
    partition_delayed_msgs: VecDeque<(PrimaryMessage, u64, Option<PublicKey>)>, //(msg, height, author, consensus/car path)
    //For egress
    egress_penalty: u64, //the number of ms of egress penalty.
    
    egress_delay_queue: DelayQueue<(PrimaryMessage, u64, Option<PublicKey>)>,
    current_egress_end: Instant,
    // Timeouts
    use_exponential_timeouts: bool,
    original_timeout: u64,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        synchronizer: Synchronizer,
        signature_service: SignatureService,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Round,
        rx_primaries: Receiver<PrimaryMessage>,
        rx_header_waiter: Receiver<Header>,
        rx_certificate_waiter: Receiver<Certificate>,
        rx_proposer: Receiver<Header>,
        tx_consensus: Sender<Certificate>,
        tx_proposer: Sender<(Vec<Certificate>, Round)>,
        simulate_asynchrony: bool,
        async_type: VecDeque<u8>,
        asynchrony_start: VecDeque<u64>,
        asynchrony_duration: VecDeque<u64>,
        affected_nodes: VecDeque<u64>,
        egress_penalty: u64,
        use_exponential_timeouts: bool,
        max_header_delay: u64,

    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                synchronizer,
                signature_service,
                consensus_round,
                gc_depth,
                rx_primaries,
                rx_header_waiter,
                rx_certificate_waiter,
                rx_proposer,
                tx_consensus,
                tx_proposer,
                gc_round: 0,
                last_voted: HashMap::with_capacity(2 * gc_depth as usize),
                processing: HashMap::with_capacity(2 * gc_depth as usize),
                current_header: Header::default(),
                votes_aggregator: VotesAggregator::new(),
                certificates_aggregators: HashMap::with_capacity(2 * gc_depth as usize),
                network: ReliableSender::new(),
                cancel_handlers: HashMap::with_capacity(2 * gc_depth as usize),
                // simulating async
                simulate_asynchrony,
                asynchrony_type: async_type.iter().map(|v| uint_to_enum(*v)).collect(),
                asynchrony_start,
                asynchrony_duration,
                affected_nodes,
                async_timer_futures: FuturesUnordered::new(),
                during_simulated_asynchrony: false,
                current_effect_type: AsyncEffectType::Off,
                already_set_timers: false,
                current_time: Instant::now(),
                partition_public_keys: HashSet::new(),
                partition_delayed_msgs: VecDeque::new(),
                egress_penalty,
                egress_delay_queue: DelayQueue::new(),
                current_egress_end: Instant::now(),
                use_exponential_timeouts,
                original_timeout: max_header_delay,
            }
            .run()
            .await;
        });
    }

    async fn process_own_header(&mut self, header: Header) -> DagResult<()> {
        // Start simulating async
        if self.simulate_asynchrony && header.round == 2 && !self.already_set_timers {
            debug!("added async timers");
            self.already_set_timers = true;
            
            debug!("asynchrony start is {:?}", self.asynchrony_start);
            for i in 0..self.asynchrony_start.len() {
                if self.asynchrony_type[i] == AsyncEffectType::Egress {
                    let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
                    keys.sort();
                    let index = keys.binary_search(&self.name).unwrap();
                    // Skip nodes that are not affected by the asynchrony
                    if index >= self.affected_nodes[i] as usize {
                        continue;
                    } else {
                        debug!("egress affects this node");
                    }
                }

                if self.asynchrony_type[i] == AsyncEffectType::Failure {
                    let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
                    keys.sort();
                    let index = keys.binary_search(&self.name).unwrap();
                    // Skip nodes that are not affected by the asynchrony
                    if index >= self.affected_nodes[i] as usize {
                        continue;
                    }
                }
                        
                let start_offset = self.asynchrony_start[i];
                let end_offset = start_offset +  self.asynchrony_duration[i];
                        
                let async_start = Timer::new(start_offset);
                let async_end = Timer::new(end_offset);

                self.async_timer_futures.push(Box::pin(async_start));
                self.async_timer_futures.push(Box::pin(async_end));

                if self.asynchrony_type[i] == AsyncEffectType::Partition {
                    let mut keys: Vec<_> = self.committee.authorities.keys().cloned().collect();
                    keys.sort();
                    let index = keys.binary_search(&self.name).unwrap();

                    // Figure out which partition we are in, partition_nodes indicates when the left partition ends
                    let mut start: usize = 0;
                    let mut end: usize = 0;
                        
                    // We are in the right partition
                    if index > self.affected_nodes[i] as usize - 1 {
                        start = self.affected_nodes[i] as usize;
                        end = keys.len();
                            
                    } else {
                        // We are in the left partition
                        start = 0;
                        end = self.affected_nodes[i] as usize;
                    }

                    // These are the nodes in our side of the partition
                    for j in start..end {
                        self.partition_public_keys.insert(keys[j]);
                    }

                    debug!("partition pks are {:?}", self.partition_public_keys);
                }
            }
        }
        // Reset the votes aggregator.
        self.current_header = header.clone();
        self.votes_aggregator = VotesAggregator::new();

        // Broadcast the new header in a reliable manner.
        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        let bytes = bincode::serialize(&PrimaryMessage::Header(header.clone()))
            .expect("Failed to serialize our own header");
        let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
        self.cancel_handlers
            .entry(header.round)
            .or_insert_with(Vec::new)
            .extend(handlers);

        // Process the header.
        self.process_header(&header).await
    }

    #[async_recursion]
    async fn process_header(&mut self, header: &Header) -> DagResult<()> {
        debug!("Processing {:?}", header);
        // Indicate that we are processing this header.
        self.processing
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.id.clone());

        // Ensure we have the parents. If at least one parent is missing, the synchronizer returns an empty
        // vector; it will gather the missing parents (as well as all ancestors) from other nodes and then
        // reschedule processing of this header.
        let parents = self.synchronizer.get_parents(header).await?;
        if parents.is_empty() {
            debug!("Processing of {} suspended: missing parent(s)", header.id);
            return Ok(());
        }

        // Check the parent certificates. Ensure the parents form a quorum and are all from the previous round.
        let mut stake = 0;
        for x in parents {
            ensure!(
                x.round() + 1 == header.round,
                DagError::MalformedHeader(header.id.clone())
            );
            stake += self.committee.stake(&x.origin());
        }
        ensure!(
            stake >= self.committee.quorum_threshold(),
            DagError::HeaderRequiresQuorum(header.id.clone())
        );

        // Ensure we have the payload. If we don't, the synchronizer will ask our workers to get it, and then
        // reschedule processing of this header once we have it.
        if self.synchronizer.missing_payload(header).await? {
            debug!("Processing of {} suspended: missing payload", header);
            return Ok(());
        }

        // Store the header.
        let bytes = bincode::serialize(header).expect("Failed to serialize header");
        self.store.write(header.id.to_vec(), bytes).await;

        // Check if we can vote for this header.
        if self
            .last_voted
            .entry(header.round)
            .or_insert_with(HashSet::new)
            .insert(header.author)
        {
            // Make a vote and send it to the header's creator.
            let vote = Vote::new(header, &self.name, &mut self.signature_service).await;
            debug!("Created {:?}", vote);
            if vote.origin == self.name {
                self.process_vote(vote)
                    .await
                    .expect("Failed to process our own vote");
            } else {
                let address = self
                    .committee
                    .primary(&header.author)
                    .expect("Author of valid header is not in the committee")
                    .primary_to_primary;
                let bytes = bincode::serialize(&PrimaryMessage::Vote(vote))
                    .expect("Failed to serialize our own vote");
                let handler = self.network.send(address, Bytes::from(bytes)).await;
                self.cancel_handlers
                    .entry(header.round)
                    .or_insert_with(Vec::new)
                    .push(handler);
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn process_vote(&mut self, vote: Vote) -> DagResult<()> {
        debug!("Processing {:?}", vote);

        // Add it to the votes' aggregator and try to make a new certificate.
        if let Some(certificate) =
            self.votes_aggregator
                .append(vote, &self.committee, &self.current_header)?
        {
            debug!("Assembled {:?}", certificate);

            // Broadcast the certificate.
            let addresses = self
                .committee
                .others_primaries(&self.name)
                .iter()
                .map(|(_, x)| x.primary_to_primary)
                .collect();
            let bytes = bincode::serialize(&PrimaryMessage::Certificate(certificate.clone()))
                .expect("Failed to serialize our own certificate");
            let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
            self.cancel_handlers
                .entry(certificate.round())
                .or_insert_with(Vec::new)
                .extend(handlers);

            // Process the new certificate.
            self.process_certificate(certificate)
                .await
                .expect("Failed to process valid certificate");
        }
        Ok(())
    }

    #[async_recursion]
    async fn process_certificate(&mut self, certificate: Certificate) -> DagResult<()> {
        debug!("Processing {:?}", certificate);

        // Process the header embedded in the certificate if we haven't already voted for it (if we already
        // voted, it means we already processed it). Since this header got certified, we are sure that all
        // the data it refers to (ie. its payload and its parents) are available. We can thus continue the
        // processing of the certificate even if we don't have them in store right now.
        if !self
            .processing
            .get(&certificate.header.round)
            .map_or_else(|| false, |x| x.contains(&certificate.header.id))
        {
            // This function may still throw an error if the storage fails.
            self.process_header(&certificate.header).await?;
        }

        // Ensure we have all the ancestors of this certificate yet. If we don't, the synchronizer will gather
        // them and trigger re-processing of this certificate.
        if !self.synchronizer.deliver_certificate(&certificate).await? {
            debug!(
                "Processing of {:?} suspended: missing ancestors",
                certificate
            );
            return Ok(());
        }

        // Store the certificate.
        let bytes = bincode::serialize(&certificate).expect("Failed to serialize certificate");
        self.store.write(certificate.digest().to_vec(), bytes).await;

        // Check if we have enough certificates to enter a new dag round and propose a header.
        if let Some(parents) = self
            .certificates_aggregators
            .entry(certificate.round())
            .or_insert_with(|| Box::new(CertificatesAggregator::new()))
            .append(certificate.clone(), &self.committee)?
        {
            // Send it to the `Proposer`.
            self.tx_proposer
                .send((parents, certificate.round()))
                .await
                .expect("Failed to send certificate");
        }

        // Send it to the consensus layer.
        let id = certificate.header.id.clone();
        if let Err(e) = self.tx_consensus.send(certificate).await {
            warn!(
                "Failed to deliver certificate {} to the consensus: {}",
                id, e
            );
        }
        Ok(())
    }

    fn sanitize_header(&mut self, header: &Header) -> DagResult<()> {
        ensure!(
            self.gc_round <= header.round,
            DagError::TooOld(header.id.clone(), header.round)
        );

        // Verify the header's signature.
        header.verify(&self.committee)?;

        // TODO [issue #3]: Prevent bad nodes from sending junk headers with high round numbers.

        Ok(())
    }

    fn sanitize_vote(&mut self, vote: &Vote) -> DagResult<()> {
        ensure!(
            self.current_header.round <= vote.round,
            DagError::TooOld(vote.digest(), vote.round)
        );

        // Ensure we receive a vote on the expected header.
        ensure!(
            vote.id == self.current_header.id
                && vote.origin == self.current_header.author
                && vote.round == self.current_header.round,
            DagError::UnexpectedVote(vote.id.clone())
        );

        // Verify the vote.
        vote.verify(&self.committee).map_err(DagError::from)
    }

    fn sanitize_certificate(&mut self, certificate: &Certificate) -> DagResult<()> {
        ensure!(
            self.gc_round <= certificate.round(),
            DagError::TooOld(certificate.digest(), certificate.round())
        );

        // Verify the certificate (and the embedded header).
        certificate.verify(&self.committee).map_err(DagError::from)
    }

    pub async fn send_msg(&mut self, message: PrimaryMessage, height: u64, author: Option<PublicKey>) {
        
        //go through enums
        match self.current_effect_type {
            AsyncEffectType::Off => {
                debug!("message sent normally");
                self.send_msg_normal(message, height, author).await;
            }
            AsyncEffectType::TempBlip => { //Our old handling
                //add message
                /*match message {
                     PrimaryMessage::ConsensusMessage(m) => {
                         match m.clone() {
                            ConsensusMessage::Prepare {slot, view, tc, qc_ticket: _, proposals} => {
                                debug!("Simulating Asynchrony: skip sending Prepare for slot {} view {}. This will trigger a view change", slot, view);
                                self.async_delayed_prepare = Some(m);
                            }
                            _ => {}
                            }
                     }
                
                    _ => { debug!("send all other messages")}
                }*/
                //panic!("TempBlip currently deprecated");
                /*if self.round >= 400 && self.round <= 401 && self.name == self.leader_elector.get_leader(self.round) {
                    debug!("dropping message");
                    return;
                } else {
                    debug!("message sent normally");
                    self.send_msg_normal(message, author).await;
                }*/
            }
            AsyncEffectType::Failure => {
                //drop message
                debug!("dropping message");
            }
            AsyncEffectType::Partition => {
                match author {
                    Some(author) => {
                        if self.partition_public_keys.contains(&author) {
                            // The receiver is in our partition, so we can send the message directly
                            debug!("single message during partition, sent normally");
                            self.send_msg_normal(message, height, Some(author)).await;
                        } else {
                            // The receiver is not in our partition, so we buffer for later
                            debug!("single message during partition, buffered");
                            self.partition_delayed_msgs.push_back((message, height, Some(author)));
                        }
                    }
                    None => {
                        // Send the message to all nodes in our side of the partition
                        if self.partition_public_keys.len() > 1 {
                            self.send_msg_partition(&message, height, true).await;
                            debug!("broadcast message during partition, sent to nodes in our partition");
                        }
                        
                        // Buffer the message for the other side of the partition
                        self.partition_delayed_msgs.push_back((message, height, None));
                    }
                }
            }
            AsyncEffectType::Egress => {
                //self.egress_delayed_msgs.push_back((message, author));
                /*let curr = Instant::now().elapsed().as_millis();
                let wake_time = curr + self.egress_penalty as u128;
                self.egress_delayed_msgs.push_back((wake_time, message, author));

                if self.egress_timer_futures.is_empty() {
                    //start timer
                    let next_wake = Timer::new(self.egress_penalty);
                    self.egress_timer_futures.push(Box::pin(next_wake));
                }*/
                let egress_end_time = Instant::now() + Duration::from_millis(self.egress_penalty);
                debug!("current time is {:?}", Instant::now());
                debug!("egress penalty is {:?}", self.egress_penalty);
                debug!("msg egress end time is {:?}", egress_end_time);
                let actual_send_time = egress_end_time.min(self.current_egress_end);
                debug!("msg actual send time is {:?}", actual_send_time);
                self.egress_delay_queue.insert_at((message, height, author), actual_send_time);
            }

            _ => {
                panic!("not a valid effect")
            }
        }
    }

    pub async fn send_msg_partition(&mut self, message: &PrimaryMessage, height: u64, our_partition: bool) {
        let addresses = self
            .committee
            .others_primaries(&self.name)
            .iter()
            .filter(|(pk, _)| (our_partition && self.partition_public_keys.contains(pk)) || (!our_partition && !self.partition_public_keys.contains(pk)))
            .map(|(_, x)| x.primary_to_primary)
            .collect();
        debug!("addresses for partition are are {:?}, our partition is {}", addresses, our_partition);        

        let bytes = bincode::serialize(message).expect("Failed to serialize message");
        let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
       
        self.cancel_handlers
            .entry(height)
            .or_insert_with(Vec::new)
            .extend(handlers);
        
    }

    pub async fn send_msg_normal(&mut self, message: PrimaryMessage, height: u64, author: Option<PublicKey>) {
        match author {
            Some(author) => {
                let address = self
                    .committee
                    .primary(&author)
                    .expect("Author of valid header is not in the committee")
                    .primary_to_primary;
                let bytes = bincode::serialize(&message).expect("Failed to serialize message");
                let handler = self.network.send(address, Bytes::from(bytes)).await;
                
                self.cancel_handlers
                    .entry(height)
                    .or_insert_with(Vec::new)
                    .push(handler);
            }
            None => {
                let addresses = self
                    .committee
                    .others_primaries(&self.name)
                    .iter()
                    .map(|(_, x)| x.primary_to_primary)
                    .collect();

                let bytes = bincode::serialize(&message).expect("Failed to serialize message");
                let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;

                self.cancel_handlers
                    .entry(height)
                    .or_insert_with(Vec::new)
                    .extend(handlers);
            }
        }
        
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        loop {
            let result = tokio::select! {
                // We receive here messages from other primaries.
                Some(message) = self.rx_primaries.recv() => {
                    match message {
                        PrimaryMessage::Header(header) => {
                            match self.sanitize_header(&header) {
                                Ok(()) => self.process_header(&header).await,
                                error => error
                            }

                        },
                        PrimaryMessage::Vote(vote) => {
                            match self.sanitize_vote(&vote) {
                                Ok(()) => self.process_vote(vote).await,
                                error => error
                            }
                        },
                        PrimaryMessage::Certificate(certificate) => {
                            match self.sanitize_certificate(&certificate) {
                                Ok(()) =>  self.process_certificate(certificate).await,
                                error => error
                            }
                        },
                        _ => panic!("Unexpected core message")
                    }
                },

                Some(()) = self.async_timer_futures.next() => {
                    self.during_simulated_asynchrony = !self.during_simulated_asynchrony; 

                    debug!("Time elapsed is {:?}", self.current_time.elapsed()); 
                    self.current_time = Instant::now();

                    if self.during_simulated_asynchrony {
                        debug!("asynchrony type is {:?}", self.asynchrony_type);
                        self.current_effect_type = self.asynchrony_type.pop_front().unwrap();

                        if self.current_effect_type == AsyncEffectType::Egress {
                            // Start the first egress timer
                            //self.egress_timer.reset();
                            let async_duration = self.asynchrony_duration.pop_front().unwrap();
                            self.current_egress_end = Instant::now() + Duration::from_millis(async_duration);
                            debug!("End of egress is {:?}", self.current_egress_end);
                        }
                    }

                    if !self.during_simulated_asynchrony {

                        if self.current_effect_type == AsyncEffectType::TempBlip {
                              //Send all blocked messages
                            /*if self.async_delayed_prepare.is_some() {
                                let last_prop = self.async_delayed_prepare.clone().unwrap();
                                let still_relevant = match &last_prop { //check whether we're still in a relevant view.
                                    ConsensusMessage::Prepare {slot, view, tc: _, qc_ticket: _, proposals: _} => view == self.views.get(slot).unwrap_or(&0),
                                    _ => false,
                                };
                                if still_relevant { //try sending it now.
                                    let _ = self.send_consensus_req(last_prop).await;
                                }
                                self.async_delayed_prepare = None;
                            }*/
                        }
                        //Failure
                        if self.current_effect_type == AsyncEffectType::Failure {
                            /*if self.async_delayed_prepare.is_some() {
                                let _ = self.send_consensus_req(self.async_delayed_prepare.clone().unwrap()).await;
                            }
                            self.async_delayed_prepare = None;*/
                            //do nothing
                        }
                        //Partition
                        if self.current_effect_type == AsyncEffectType::Partition {
                            while !self.partition_delayed_msgs.is_empty() {
                                debug!("sending messages to other side of partition");
                                let (msg, height, author) = self.partition_delayed_msgs.pop_front().unwrap();
                                match author {
                                    Some(author) => self.send_msg_normal(msg, height, Some(author)).await,
                                    None => self.send_msg_partition(&msg, height, false).await,
                                }
                            }
                        }
                        //Egress delay
                        if self.current_effect_type == AsyncEffectType::Egress {
                            //Send all.
                            /*while !self.egress_delayed_msgs.is_empty() {
                                let (msg, height, author, consensus_handler) = self.egress_delayed_msgs.pop_front().unwrap();
                                debug!("sending delayed egress message");
                                self.send_msg_normal(msg, height, author, consensus_handler).await;
                            }*/
                        }

                        // Turn off the async effect type
                        self.current_effect_type = AsyncEffectType::Off;
                      
                        //Start another async event if available
                        /*if !self.asynchrony_start.is_empty() {
                            self.current_effect_type = self.asynchrony_type.pop_front().unwrap();
                            let start_offset = self.asynchrony_start.pop_front().unwrap();
                            let end_offset = start_offset +  self.asynchrony_duration.pop_front().unwrap();
                            
                            let async_start = Timer::new(0, 0, start_offset);
                            let async_end = Timer::new(0, 0, end_offset);
    
                            self.async_timer_futures.push(Box::pin(async_start));
                            self.async_timer_futures.push(Box::pin(async_end));
                        }*/
                    
                        
                    }
                    Ok(())
                },

                Some(Ok(item)) = self.egress_delay_queue.next() => {
                    debug!("egress msg expired, sending normally");
                    let (message, height, author) = item.into_inner();
                    self.send_msg_normal(message, height, author).await;
                    Ok(())
                },

                // We receive here loopback headers from the `HeaderWaiter`. Those are headers for which we interrupted
                // execution (we were missing some of their dependencies) and we are now ready to resume processing.
                Some(header) = self.rx_header_waiter.recv() => self.process_header(&header).await,

                // We receive here loopback certificates from the `CertificateWaiter`. Those are certificates for which
                // we interrupted execution (we were missing some of their ancestors) and we are now ready to resume
                // processing.
                Some(certificate) = self.rx_certificate_waiter.recv() => self.process_certificate(certificate).await,

                // We also receive here our new headers created by the `Proposer`.
                Some(header) = self.rx_proposer.recv() => self.process_own_header(header).await,
            };
            match result {
                Ok(()) => (),
                Err(DagError::StoreError(e)) => {
                    error!("{}", e);
                    panic!("Storage failure: killing node.");
                }
                Err(e @ DagError::TooOld(..)) => debug!("{}", e),
                Err(e) => warn!("{}", e),
            }

            // Cleanup internal state.
            let round = self.consensus_round.load(Ordering::Relaxed);
            if round > self.gc_depth {
                let gc_round = round - self.gc_depth;
                self.last_voted.retain(|k, _| k >= &gc_round);
                self.processing.retain(|k, _| k >= &gc_round);
                self.certificates_aggregators.retain(|k, _| k >= &gc_round);
                self.cancel_handlers.retain(|k, _| k >= &gc_round);
                self.gc_round = gc_round;
            }
        }
    }
}
