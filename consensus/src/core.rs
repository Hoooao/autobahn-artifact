use crate::aggregator::Aggregator;
use crate::config::{Committee, Parameters};
use crate::error::{ConsensusError, ConsensusResult};
use crate::leader::LeaderElector;
use crate::mempool::MempoolDriver;
use crate::messages::{Block, Timeout, Vote, QC, TC};
use crate::synchronizer::Synchronizer;
use crate::timer::Timer;
use async_recursion::async_recursion;
use crypto::Hash as _;
use crypto::{Digest, PublicKey, SignatureService};
use futures::{Future, StreamExt};
use futures::stream::FuturesUnordered;
use log::{debug, error, info, warn};
use network::NetMessage;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::{HashSet, VecDeque};
use std::pin::Pin;
use std::time::Instant;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration};

#[cfg(test)]
#[path = "tests/core_tests.rs"]
pub mod core_tests;

pub type RoundNumber = u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum ConsensusMessage {
    Propose(Block),
    Vote(Vote),
    Timeout(Timeout),
    TC(TC),
    LoopBack(Block),
    SyncRequest(Digest, PublicKey),
}

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
    name: PublicKey,
    committee: Committee,
    parameters: Parameters,
    store: Store,
    signature_service: SignatureService,
    leader_elector: LeaderElector,
    mempool_driver: MempoolDriver,
    synchronizer: Synchronizer,
    core_channel: Receiver<ConsensusMessage>,
    network_channel: Sender<NetMessage>,
    commit_channel: Sender<Block>,
    round: RoundNumber,
    last_voted_round: RoundNumber,
    preferred_round: RoundNumber,
    last_committed_round: RoundNumber,
    high_qc: QC,
    timer: Timer,
    aggregator: Aggregator,
    
    /*simulate_asynchrony: bool,
    asynchrony_start: u64,
    asynchrony_duration: u64,
    during_simulated_asynchrony: bool,
    async_timer_futures: FuturesUnordered<Pin<Box<dyn Future<Output = ()> + Send>>>,
    current_time: Instant,
    async_last_tc: Option<TC>,*/

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
    partition_delayed_msgs: VecDeque<(ConsensusMessage, Option<PublicKey>)>, //(msg, height, author, consensus/car path)
    //For egress
    egress_penalty: u64, //the number of ms of egress penalty.
    egress_timer: Timer,
    egress_delayed_msgs: VecDeque<(ConsensusMessage, Option<PublicKey>)>,
    // Timeouts
    use_exponential_timeouts: bool,
}

impl Core {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        signature_service: SignatureService,
        store: Store,
        leader_elector: LeaderElector,
        mempool_driver: MempoolDriver,
        synchronizer: Synchronizer,
        core_channel: Receiver<ConsensusMessage>,
        network_channel: Sender<NetMessage>,
        commit_channel: Sender<Block>,
    ) -> Self {
        let aggregator = Aggregator::new(committee.clone());
        let simulate_asynchrony = parameters.simulate_asynchrony;
        let async_type = parameters.async_type.clone();
        let asynchrony_start = parameters.asynchrony_start.clone();
        let asynchrony_duration = parameters.asynchrony_duration.clone();
        let affected_nodes = parameters.affected_nodes.clone();
        let egress_penalty = parameters.egress_penalty;
        let use_exponential_timeouts = parameters.use_exponential_timeouts;
        
        let timer = Timer::new(parameters.timeout_delay);
        Self {
            name,
            committee,
            parameters,
            signature_service,
            store,
            leader_elector,
            mempool_driver,
            synchronizer,
            network_channel,
            commit_channel,
            core_channel,
            round: 1,
            last_voted_round: 0,
            preferred_round: 0,
            last_committed_round: 0,
            high_qc: QC::genesis(),
            timer,
            aggregator,
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
            //current_time: Instant::now(),
            //async_last_tc: None,
            partition_public_keys: HashSet::new(),
            partition_delayed_msgs: VecDeque::new(),
            egress_penalty,
            egress_timer: Timer::new(egress_penalty),
            egress_delayed_msgs: VecDeque::new(),
            use_exponential_timeouts,
        }
    }

    async fn store_block(&mut self, block: &Block) {
        let key = block.digest().to_vec();
        let value = bincode::serialize(block).expect("Failed to serialize block");
        self.store.write(key, value).await;
    }

    // -- Start Safety Module --
    fn increase_last_voted_round(&mut self, target: RoundNumber) {
        self.last_voted_round = max(self.last_voted_round, target);
    }

    fn update_preferred_round(&mut self, target: RoundNumber) {
        self.preferred_round = max(self.preferred_round, target);
    }

    async fn make_vote(&mut self, block: &Block) -> Option<Vote> {
        // Check if we can vote for this block.
        let safety_rule_1 = block.round > self.last_voted_round;
        let safety_rule_2 = block.qc.round >= self.preferred_round;
        if !(safety_rule_1 && safety_rule_2) {
            return None;
        }

        // Ensure we won't vote for contradicting blocks.
        self.increase_last_voted_round(block.round);

        // TODO [issue #15]: Write to storage preferred_round and last_voted_round.

        Some(Vote::new(&block, self.name, self.signature_service.clone()).await)
    }

    async fn commit(&mut self, block: Block) -> ConsensusResult<()> {
        if self.last_committed_round >= block.round {
            return Ok(());
        }

        //Simulate asynchrony duration:
        /*if self.simulate_asynchrony && block.round == 2 {
            debug!("added async timers");
            let async_start = Timer::new(self.asynchrony_start);
            let async_end = Timer::new(self.asynchrony_start + self.asynchrony_duration);
            self.current_time = Instant::now();
            self.async_timer_futures.push(Box::pin(async_start));
            self.async_timer_futures.push(Box::pin(async_end));
        }*/

        // Start simulating asynchrony
        if self.simulate_asynchrony && block.round == 2 && !self.already_set_timers {
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

        let mut to_commit = VecDeque::new();
        to_commit.push_back(block.clone());

        // Ensure we commit the entire chain. This is needed after view-change.
        let mut parent = block.clone();
        while self.last_committed_round + 1 < parent.round {
            let ancestor = self
                .synchronizer
                .get_parent_block(&parent)
                .await?
                .expect("We should have all the ancestors by now");
            to_commit.push_front(ancestor.clone());
            parent = ancestor;
        }

        // Save the last committed block.
        self.last_committed_round = block.round;

        // Send all the newly committed blocks to the node's application layer.
        while let Some(block) = to_commit.pop_back() {
            //if block.digest != Digest::default() {
                info!("Committed {}", block);

                for (digest, _) in block.payload.iter() {
                #[cfg(feature = "benchmark")]
                // NOTE: This log entry is used to compute performance.
                info!("Committed B{}({})", block.round, base64::encode(digest));
                }
                
            //}
            debug!("Committed {:?}", block);
            if let Err(e) = self.commit_channel.send(block).await {
                warn!("Failed to send block through the commit channel: {}", e);
            }
        }
        Ok(())
    }
    // -- End Safety Module --

    // -- Start Pacemaker --
    fn update_high_qc(&mut self, qc: &QC) {
        if qc.round > self.high_qc.round {
            self.high_qc = qc.clone();
        }
    }

    async fn local_timeout_round(&mut self) -> ConsensusResult<()> {
        warn!("Timeout reached for round {}", self.round);
        self.increase_last_voted_round(self.round);
        let timeout = Timeout::new(
            self.high_qc.clone(),
            self.round,
            self.name,
            self.signature_service.clone(),
        )
        .await;
        debug!("Created {:?}", timeout);
        if self.use_exponential_timeouts {
            // Double the timeout
            self.parameters.timeout_delay *= 2;
            debug!("new timeout value is {}", self.parameters.timeout_delay);
            self.timer = Timer::new(self.parameters.timeout_delay);
        } else {
            self.timer.reset();
        }
        
        let message = ConsensusMessage::Timeout(timeout.clone());
        /*Synchronizer::transmit(
            &message,
            &self.name,
            None,
            &self.network_channel,
            &self.committee,
        )
        .await?;*/
        self.send_msg(message, None).await;
        self.handle_timeout(&timeout).await
    }

    #[async_recursion]
    async fn handle_vote(&mut self, vote: &Vote) -> ConsensusResult<()> {
        debug!("Processing {:?}", vote);
        if vote.round < self.round {
            return Ok(());
        }

        // Ensure the vote is well formed.
        vote.verify(&self.committee)?;

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(qc) = self.aggregator.add_vote(vote.clone())? {
            debug!("Assembled {:?}", qc);

            // Process the QC.
            self.process_qc(&qc).await;

            // Make a new block if we are the next leader.
            if self.name == self.leader_elector.get_leader(self.round) {
                self.generate_proposal(None).await?;
            }
        }
        Ok(())
    }

    async fn handle_timeout(&mut self, timeout: &Timeout) -> ConsensusResult<()> {
        debug!("Processing {:?}", timeout);
        if timeout.round < self.round {
            return Ok(());
        }

        // Ensure the timeout is well formed.
        timeout.verify(&self.committee)?;

        // Process the QC embedded in the timeout.
        self.process_qc(&timeout.high_qc).await;

        // Add the new vote to our aggregator and see if we have a quorum.
        if let Some(tc) = self.aggregator.add_timeout(timeout.clone())? {
            debug!("Assembled {:?}", tc);

            // Try to advance the round.
            self.advance_round(tc.round).await;

            // Broadcast the TC.
            let message = ConsensusMessage::TC(tc.clone());
            /*Synchronizer::transmit(
                &message,
                &self.name,
                None,
                &self.network_channel,
                &self.committee,
            )
            .await?;*/
            self.send_msg(message, None).await;

            // Make a new block if we are the next leader.
            if self.name == self.leader_elector.get_leader(self.round) {
                self.generate_proposal(Some(tc)).await?;
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn advance_round(&mut self, round: RoundNumber) {
        if round < self.round {
            return;
        }
        // Reset the timer and advance round.
        self.timer.reset();
        self.round = round + 1;
        debug!("Moved to round {}", self.round);

        // Cleanup the vote aggregator.
        self.aggregator.cleanup(&self.round);
    }
    // -- End Pacemaker --

    #[async_recursion]
    async fn generate_proposal(&mut self, tc: Option<TC>) -> ConsensusResult<()> {
        /*if self.during_simulated_asynchrony {
            debug!("Simulating asynchrony skipping sending a block in round {:?}, will trigger view change", self.round);
            self.async_last_tc = tc;
            return Ok(())
        }*/
        // Make a new block.
        let payload = self 
            .mempool_driver
            .get(self.parameters.max_payload_size)
            .await;
        let block = Block::new(
            self.high_qc.clone(),
            tc,
            self.name,
            self.round,
            payload,
            self.signature_service.clone(),
        )
        .await;
        //if block.digest != Digest::default() {
        info!("Created {}", block);

        #[cfg(feature = "benchmark")]
        for (digest, _) in block.payload.iter() {
            // NOTE: This log entry is used to compute performance.
            info!("Created B{}({})", block.round, base64::encode(digest));
        }
        
        //}
        debug!("Created {:?}", block);

        // Process our new block and broadcast it.
        let message = ConsensusMessage::Propose(block.clone());
        /*Synchronizer::transmit(
            &message,
            &self.name,
            None,
            &self.network_channel,
            &self.committee,
        )
        .await?;*/
        self.send_msg(message, None).await;
        self.process_block(&block).await?;

        // Wait for the minimum block delay.
        sleep(Duration::from_millis(self.parameters.min_block_delay)).await;
        Ok(())
    }

    async fn process_qc(&mut self, qc: &QC) {
        self.advance_round(qc.round).await;
        self.update_high_qc(qc);
    }

    #[async_recursion]
    async fn process_block(&mut self, block: &Block) -> ConsensusResult<()> {
        debug!("Processing {:?}", block);

        // Let's see if we have the last three ancestors of the block, that is:
        //      b0 <- |qc0; b1| <- |qc1; block|
        // If we don't, the synchronizer asks for them to other nodes. It will
        // then ensure we process all three ancestors in the correct order, and
        // finally make us resume processing this block.
        let (b0, b1, b2) = match self.synchronizer.get_ancestors(block).await? {
            Some(ancestors) => ancestors,
            None => {
                debug!("Processing of {} suspended: missing parent", block.digest());
                return Ok(());
            }
        };

        // Store the block only if we have already processed all its ancestors.
        self.store_block(block).await;

        // Check if we can commit the head of the 2-chain.
        // Note that we commit blocks only if we have all its ancestors.
        let mut commit_rule = b0.round + 1 == b1.round;
        commit_rule &= b1.round + 1 == b2.round;
        if commit_rule {
            self.commit(b0.clone()).await?;
        }
        self.update_preferred_round(b1.round);

        // Cleanup the mempool.
        self.mempool_driver.cleanup(&b0, &b1, &b2, &block).await;

        // Ensure the block's round is as expected.
        // This check is important: it prevents bad leaders from producing blocks
        // far in the future that may cause overflow on the round number.
        if block.round != self.round {
            return Ok(());
        }

        // See if we can vote for this block.
        if let Some(vote) = self.make_vote(block).await {
            debug!("Created {:?}", vote);
            let next_leader = self.leader_elector.get_leader(self.round + 1);
            if next_leader == self.name {
                self.handle_vote(&vote).await?;
            } else {
                let message = ConsensusMessage::Vote(vote);
                /*Synchronizer::transmit(
                    &message,
                    &self.name,
                    Some(&next_leader),
                    &self.network_channel,
                    &self.committee,
                )
                .await?;*/
                self.send_msg(message, Some(next_leader)).await;
            }
        }
        Ok(())
    }

    async fn handle_proposal(&mut self, block: &Block) -> ConsensusResult<()> {
        let digest = block.digest();

        // Ensure the block proposer is the right leader for the round.
        ensure!(
            block.author == self.leader_elector.get_leader(block.round),
            ConsensusError::WrongLeader {
                digest,
                leader: block.author,
                round: block.round
            }
        );

        // Check the block is correctly formed.
        block.verify(&self.committee)?;

        // Process the QC. This may allow us to advance round.
        self.process_qc(&block.qc).await;

        // Process the TC (if any). This may also allow us to advance round.
        if let Some(ref tc) = block.tc {
            self.advance_round(tc.round).await;
        }

        // Let's see if we have the block's data. If we don't, the mempool
        // will get it and then make us resume processing this block.
        if !self.mempool_driver.verify(block.clone()).await? {
            debug!("Processing of {} suspended: missing payload", digest);
            return Ok(());
        }

        // All check pass, we can process this block.
        self.process_block(block).await
    }

    async fn handle_sync_request(
        &mut self,
        digest: Digest,
        sender: PublicKey,
    ) -> ConsensusResult<()> {
        if let Some(bytes) = self.store.read(digest.to_vec()).await? {
            let block = bincode::deserialize(&bytes)?;
            let message = ConsensusMessage::Propose(block);
            /*Synchronizer::transmit(
                &message,
                &self.name,
                Some(&sender),
                &self.network_channel,
                &self.committee,
            )
            .await?;*/
            self.send_msg(message, Some(sender)).await;
        }
        Ok(())
    }

    async fn handle_tc(&mut self, tc: TC) -> ConsensusResult<()> {
        self.advance_round(tc.round).await;
        if self.name == self.leader_elector.get_leader(self.round) {
            self.generate_proposal(Some(tc)).await?;
        }
        Ok(())
    }

    pub async fn send_msg(&mut self, message: ConsensusMessage, author: Option<PublicKey>) {
        
        //go through enums
        match self.current_effect_type {
            AsyncEffectType::Off => {
                debug!("message sent normally");
                self.send_msg_normal(message, author).await;
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
                panic!("TempBlip currently deprecated");
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
                            self.send_msg_normal(message, Some(author)).await;
                        } else {
                            // The receiver is not in our partition, so we buffer for later
                            debug!("single message during partition, buffered");
                            self.partition_delayed_msgs.push_back((message, Some(author)));
                        }
                    }
                    None => {
                        // Send the message to all nodes in our side of the partition
                        if self.partition_public_keys.len() > 1 {
                            self.send_msg_partition(&message, true).await;
                            debug!("broadcast message during partition, sent to nodes in our partition");
                        }
                        
                        // Buffer the message for the other side of the partition
                        self.partition_delayed_msgs.push_back((message, None));
                    }
                }
            }
            AsyncEffectType::Egress => {
                self.egress_delayed_msgs.push_back((message, author));
            }

            _ => {
                panic!("not a valid effect")
            }
        }
    }

    pub async fn send_msg_normal(&mut self, message: ConsensusMessage, author: Option<PublicKey>) {
        let _ = Synchronizer::transmit(
            &message,
            &self.name,
            author.as_ref(),
            &self.network_channel,
            &self.committee,
        )
        .await;
    }

    pub async fn send_msg_partition(&mut self, message: &ConsensusMessage, our_partition: bool) {
        let _ = Synchronizer::transmit_partition(
            message,
            &self.name,
            &self.network_channel,
            &self.committee,
            our_partition,
            &self.partition_public_keys,
        )
        .await;
        
    }

    pub async fn run(&mut self) {
        //Simulate asynchrony duration:
        /*if self.simulate_asynchrony && self.round == 1 {
            debug!("added async timers");
            let async_start = Timer::new(self.asynchrony_start);
            let async_end = Timer::new(self.asynchrony_start + self.asynchrony_duration);
            self.current_time = Instant::now();
            self.async_timer_futures.push(Box::pin(async_start));
            self.async_timer_futures.push(Box::pin(async_end));
        }*/

        // Upon booting, generate the very first block (if we are the leader).
        // Also, schedule a timer in case we don't hear from the leader.
        self.timer.reset();
        if self.name == self.leader_elector.get_leader(self.round) {
            self.generate_proposal(None)
                .await
                .expect("Failed to send the first block");
        }

        // This is the main loop: it processes incoming blocks and votes,
        // and receive timeout notifications from our Timeout Manager.
        loop {
            let result = tokio::select! {
                Some(message) = self.core_channel.recv() => {
                    match message {
                        ConsensusMessage::Propose(block) => self.handle_proposal(&block).await,
                        ConsensusMessage::Vote(vote) => self.handle_vote(&vote).await,
                        ConsensusMessage::Timeout(timeout) => self.handle_timeout(&timeout).await,
                        ConsensusMessage::TC(tc) => self.handle_tc(tc).await,
                        ConsensusMessage::LoopBack(block) => self.process_block(&block).await,
                        ConsensusMessage::SyncRequest(digest, sender) => self.handle_sync_request(digest, sender).await
                    }
                },
                () = &mut self.timer => self.local_timeout_round().await,

                Some(()) = self.async_timer_futures.next() => {
                    self.during_simulated_asynchrony = !self.during_simulated_asynchrony; 

                    debug!("Time elapsed is {:?}", self.current_time.elapsed()); 
                    self.current_time = Instant::now();

                    if self.during_simulated_asynchrony {
                        debug!("asynchrony type is {:?}", self.asynchrony_type);
                        self.current_effect_type = self.asynchrony_type.pop_front().unwrap();

                        if self.current_effect_type == AsyncEffectType::Egress {
                            // Start the first egress timer
                            self.egress_timer.reset();
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
                            //do nothing
                        }
                        //Partition
                        if self.current_effect_type == AsyncEffectType::Partition {
                            while !self.partition_delayed_msgs.is_empty() {
                                debug!("sending messages to other side of partition");
                                let (msg, author) = self.partition_delayed_msgs.pop_front().unwrap();
                                match author {
                                    Some(author) => self.send_msg_normal(msg, Some(author)).await,
                                    None => self.send_msg_partition(&msg, false).await,
                                }
                            }
                        }
                        //Egress delay
                        if self.current_effect_type == AsyncEffectType::Egress {
                            //Send all.
                            while !self.egress_delayed_msgs.is_empty() {
                                let (msg, author) = self.egress_delayed_msgs.pop_front().unwrap();
                                debug!("sending delayed egress message");
                                self.send_msg_normal(msg, author).await;
                            }
                        }

                        // Turn off the async effect type
                        self.current_effect_type = AsyncEffectType::Off;                      
                    }
                    Ok(())
                },

                () = &mut self.egress_timer => {
                    if self.during_simulated_asynchrony {
                        //Send all.
                        while !self.egress_delayed_msgs.is_empty() {
                            let (msg, author) = self.egress_delayed_msgs.pop_front().unwrap();
                            debug!("sending delayed egress message");
                            self.send_msg_normal(msg, author).await;
                        }
                        self.egress_timer.reset();
                    }
                    Ok(())
                },

                /*Some(()) = self.async_timer_futures.next() => {
                    self.during_simulated_asynchrony = !self.during_simulated_asynchrony; 
                    debug!("Time elapsed is {:?}", self.current_time.elapsed()); 
                    self.current_time = Instant::now();

                    if !self.during_simulated_asynchrony {
                        let async_start = Timer::new(self.asynchrony_start);
                        let async_end = Timer::new(self.asynchrony_start + self.asynchrony_duration);

                        self.async_timer_futures.push(Box::pin(async_start));
                        self.async_timer_futures.push(Box::pin(async_end));

                        if self.async_last_tc.is_some() {
                            let tc = self.async_last_tc.clone().unwrap();
                            if self.round == tc.round + 1 { //still in that round
                                    let _ = self.generate_proposal(Some(tc)).await;
                            }
                            self.async_last_tc = None;
                            
                        }
                    }
                    Ok(())
                },*/

                else => break,
            };
            match result {
                Ok(()) => (),
                Err(ConsensusError::StoreError(e)) => error!("{}", e),
                Err(ConsensusError::SerializationError(e)) => error!("Store corrupted. {}", e),
                Err(e) => warn!("{}", e),
            }
        }
    }
}
