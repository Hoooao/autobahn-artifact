use crate::committer::Committer;
use crate::core::Core;
use crate::error::ConsensusError;
use crate::helper::Helper;
use crate::leader::LeaderElector;
use crate::mempool::MempoolDriver;
//use crate::messages::{Block, Timeout, Vote, TC};
use primary::messages::{Header, Block, Certificate, Timeout, AcceptVote, QC, TC, Ticket, Vote};
use crate::proposer::Proposer;
use crate::synchronizer::Synchronizer;
use async_trait::async_trait;
use bytes::Bytes;
use config::{Committee, Parameters};
use crypto::{Digest, PublicKey, SignatureService};
use futures::SinkExt as _;
use log::info;
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use serde::{Deserialize, Serialize};
use std::error::Error;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[cfg(test)]
#[path = "tests/consensus_tests.rs"]
pub mod consensus_tests;

/// The default channel capacity for each channel of the consensus.
pub const CHANNEL_CAPACITY: usize = 1_000;

/// The consensus view number.
pub type View= u64;
/// The Dag round number.
pub type Round= u64;

#[derive(Serialize, Deserialize, Debug)]
pub enum ConsensusMessage {
    Propose(Block), //No longer used //FIXME needed to uncomment to compile
    Vote(Vote),     //No longer used
    AcceptVote(AcceptVote),
    QC(QC),
    Timeout(Timeout),
    TC(TC),
    SyncRequest(Digest, PublicKey), //Note: These Digests are now for Headers
    SyncRequestCert(Digest, PublicKey),
    Header(Header),
    Certificate(Certificate),
}

pub struct Consensus;

impl Consensus {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        parameters: Parameters,
        signature_service: SignatureService,
        store: Store,
        rx_mempool: Receiver<Certificate>,
        rx_committer: Receiver<Certificate>,
        tx_mempool: Sender<Certificate>,
        tx_output: Sender<Header>,
        tx_ticket: Sender<(View, Round, Ticket)>,
        tx_validation: Sender<(Header, u8, Option<QC>, Option<TC>)>,
        rx_sailfish: Receiver<Header>,
        tx_pushdown_cert: Sender<Certificate>,
    ) {
        // NOTE: This log entry is used to compute performance.
        parameters.log();

        let (tx_consensus, rx_consensus) = channel(CHANNEL_CAPACITY);
        let (tx_proposer, rx_proposer) = channel(CHANNEL_CAPACITY);
        let (tx_helper_header, rx_helper_header) = channel(CHANNEL_CAPACITY);
        let (tx_helper_cert, rx_helper_cert) = channel(CHANNEL_CAPACITY);
        let (tx_commit, rx_commit) = channel(CHANNEL_CAPACITY);
        //let (tx_mempool_copy, rx_mempool_copy) = channel(CHANNEL_CAPACITY);
        let (tx_message, rx_message) = channel(CHANNEL_CAPACITY);
        let (tx_consensus_header, rx_consensus_header) = channel(CHANNEL_CAPACITY);
        let (tx_loop_header, rx_loop_header) = channel(CHANNEL_CAPACITY);
        let (tx_loop_cert, rx_loop_cert) = channel(CHANNEL_CAPACITY);
        let (tx_special, rx_special) = channel(CHANNEL_CAPACITY);

        //let (tx_sailfish, rx_sailfish) = channel(CHANNEL_CAPACITY);
        //let (tx_dag, rx_dag) = channel(CHANNEL_CAPACITY);

        // Spawn the network receiver.
        let mut address = committee
            .consensus(&name)
            .expect("Our public key is not in the committee")
            .consensus_to_consensus;
        address.set_ip("0.0.0.0".parse().unwrap());
        NetworkReceiver::spawn(
            address,
            /* handler */
            ConsensusReceiverHandler {
                tx_consensus,
                tx_helper_header,
                tx_helper_cert
            },
        );
        info!(
            "Node {} listening to consensus messages on {}",
            name, address
        );

        // Make the leader election module.
        let leader_elector = LeaderElector::new(committee.clone());

        // Make the mempool driver.
        let mempool_driver = MempoolDriver::new(committee.clone(), tx_mempool);

        // Make the synchronizer.
        let synchronizer = Synchronizer::new(
            name,
            committee.clone(),
            store.clone(),
            tx_loop_header.clone(),
            tx_loop_cert,
            parameters.sync_retry_delay,
        );

        // Spawn the consensus core.
        Core::spawn(
            name,
            committee.clone(),
            signature_service.clone(),
            store.clone(),
            leader_elector,
            mempool_driver,
            synchronizer,
            parameters.timeout_delay,
            rx_message,
            rx_consensus_header,
            rx_loop_header,
            rx_loop_cert,
            tx_pushdown_cert,
            tx_proposer,
            tx_commit,
            tx_validation,
            tx_ticket,
            /*rx_special */ rx_special,
        );

        // Commits the mempool certificates and their sub-dag.
        Committer::spawn(
            committee.clone(),
            store.clone(),
            parameters.gc_depth,
            /* rx_mempool */ rx_committer, //Receive certs directly.
            rx_commit,
            tx_output,
        );

        // Spawn the block proposer.
        // Proposer::spawn(
        //     name,
        //     committee.clone(),
        //     signature_service,
        //     /* rx_consensus */ rx_sailfish,
        //     rx_mempool,
        //     /* rx_message */ rx_proposer,
        //     tx_loop_header,
        //     tx_mempool_copy,
        // );

        // Spawn the helper module.
        Helper::spawn(committee, store, /* rx_requests */ rx_helper_header, rx_helper_cert);
    }
}

/// Defines how the network receiver handles incoming primary messages.
#[derive(Clone)]
struct ConsensusReceiverHandler {
    tx_consensus: Sender<ConsensusMessage>,
    tx_helper_header: Sender<(Digest, PublicKey)>,
    tx_helper_cert: Sender<(Digest, PublicKey)>,
}

#[async_trait]
impl MessageHandler for ConsensusReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // Deserialize and parse the message.
        match bincode::deserialize(&serialized).map_err(ConsensusError::SerializationError)? {
            ConsensusMessage::SyncRequest(missing, origin) => self
                .tx_helper_header
                .send((missing, origin))
                .await
                .expect("Failed to send consensus message"),
            ConsensusMessage::SyncRequestCert(missing, origin) => self
                .tx_helper_cert
                .send((missing, origin))
                .await
                .expect("Failed to send consensus message"),
            message @ ConsensusMessage::Propose(..) => {
                // Reply with an ACK.
                let _ = writer.send(Bytes::from("Ack")).await;

                // Pass the message to the consensus core.
                self.tx_consensus
                    .send(message)
                    .await
                    .expect("Failed to consensus message")
            }
            message => self
                .tx_consensus
                .send(message)
                .await
                .expect("Failed to consensus message"),
        }
        Ok(())
    }
}
