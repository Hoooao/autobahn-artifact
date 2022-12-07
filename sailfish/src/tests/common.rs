use super::*;
use config::Committee;
use primary::error::{ConsensusError};
use primary::messages::Ticket;
use crate::consensus::ConsensusMessage;
//use primary::config::Committee;
use crate::consensus::Round;
use crate::consensus::View;
use primary::messages::{Block, Timeout, AcceptVote, QC};
use bytes::Bytes;
use crypto::Hash as _;
use crypto::{generate_keypair, Digest, PublicKey, SecretKey, Signature};
use futures::sink::SinkExt as _;
use futures::stream::StreamExt as _;
use primary::messages::Header;
use rand::rngs::StdRng;
use rand::SeedableRng as _;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

// Fixture.
pub fn keys() -> Vec<(PublicKey, SecretKey)> {
    let mut rng = StdRng::from_seed([0; 32]);
    (0..4).map(|_| generate_keypair(&mut rng)).collect()
}

// Fixture.
pub fn committee() -> Committee {
    Committee::new(
        keys()
            .into_iter()
            .enumerate()
            .map(|(i, (name, _))| {
                let address = format!("127.0.0.1:{}", i).parse().unwrap();
                let stake = 1;
                (name, stake, address)
            })
            .collect(),
        /* epoch */ //100,
    )
}

// Fixture.
// pub fn committee_with_base_port(base_port: u16) -> Committee {
//     let mut committee = committee();
//     for authority in committee.authorities.values_mut() {
//         //let port = authority.address.port();
//         let port = authority.consensus.consensus_to_consensus.port();
//         //authority.address.set_port(base_port + port);
//         authority.consensus.consensus_to_consensus.set_port(base_port + port);
//     }
//     committee
// }

// Fixture.
pub fn committee_with_base_port(base_port: u16) -> Committee {
    let mut committee = committee();
    for authority in committee.authorities.values_mut() {
        let primary = &mut authority.primary;

        let port = primary.primary_to_primary.port();
        primary.primary_to_primary.set_port(base_port + port);

        let port = primary.worker_to_primary.port();
        primary.worker_to_primary.set_port(base_port + port);

        for worker in authority.workers.values_mut() {
            let port = worker.primary_to_worker.port();
            worker.primary_to_worker.set_port(base_port + port);

            let port = worker.transactions.port();
            worker.transactions.set_port(base_port + port);

            let port = worker.worker_to_worker.port();
            worker.worker_to_worker.set_port(base_port + port);
        }
    }
    committee
}


/*impl Block {
    pub fn new_from_key_block(
        qc: QC,
        author: PublicKey,
        view: View,
        payload: Vec<Certificate>,
        secret: &SecretKey,
    ) -> Block {
        let block = Block {
            qc,
            tc: None,
            author,
            view,
            payload,
            signature: Signature::default(),
        };
        let signature = Signature::new(&block.digest(), secret);
        Block { signature, ..block }
    }
}*/

/*impl PartialEq for Block {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}*/

/*impl Vote {
    pub fn new_from_key(id: Digest, round: Round, author: PublicKey, secret: &SecretKey) -> Self {
        let vote = Vote {
            id,
            round,
            origin: author,
            author,
            signature: Signature::default(),
            view: round,
            special_valid: 0,
            qc: None,
            tc: None,
        };
        let signature = Signature::new(&vote.digest(), &secret);
        Self { signature, ..vote }
    }
}

impl PartialEq for Vote {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}*/

/*impl Timeout {
    pub fn new_from_key_timeout(high_qc: QC, round: Round, author: PublicKey, secret: &SecretKey) -> Self {
        let timeout = Timeout {
            high_qc,
            view: round,
            author,
            signature: Signature::default(),
        };
        let signature = Signature::new(&timeout.digest(), &secret);
        Timeout {
            signature,
            ..timeout
        }
    }
}

impl PartialEq for Timeout {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}*/

// // Fixture.
// pub fn block() -> Block {
//     let (public_key, secret_key) = keys().pop().unwrap();
//     Block::new_from_key(QC::genesis(&committee()), public_key, 1, Vec::new(), &secret_key)
// }


// Fixture
pub fn special_header() -> Header {
    let (author, secret) = keys().pop().unwrap();
    //let par = vec![header().id];
    //let par = vec![Header::default().id];
    let header = Header {
        author,
        round: 1,
        //parents: par.iter().cloned().collect(),

        is_special: true,
        view: 1,
        //special parent
        special_parent: Some(Header::genesis(&committee()).id),
        special_parent_round: 0,
        //consensus parent
        ticket: Some(Ticket::genesis(&committee())),
        prev_view_round: 0,
        consensus_parent: Some(Header::genesis(&committee()).id),

        //defaults: payload, parent, 
        ..Header::default()
    };
    Header {
        id: header.digest(),
        signature: Signature::new(&header.digest(), &secret),
        ..header
    }
}

// Fixture.
pub fn accept_vote() -> AcceptVote {
    let (public_key, secret_key) = keys().pop().unwrap();
    AcceptVote::new_from_key(special_header().digest(), 1, 1, public_key, &secret_key)
}

// Fixture.
pub fn qc() -> QC {
    let qc = QC {
        hash: special_header().id, //Digest::default(),
        view: 1,
        view_round: 1,
        votes: Vec::new(),
    };
    let digest = qc.digest();
    let mut keys = keys();
    let votes: Vec<_> = (0..3)
        .map(|_| {
            let (public_key, secret_key) = keys.pop().unwrap();
            (public_key, Signature::new(&digest, &secret_key))
        })
        .collect();
    QC { votes, ..qc }
}

// Fixture.
pub fn chain(keys: Vec<(PublicKey, SecretKey)>) -> Vec<Block> {
    let mut latest_qc = QC::genesis(&committee());
    keys.iter()
        .enumerate()
        .map(|(i, key)| {
            // Make a block.
            let (public_key, secret_key) = key;
            let block = Block::new_from_key(
                latest_qc.clone(),
                *public_key,
                1 + i as Round,
                Vec::new(),
                secret_key,
            );

            // Make a qc for that block (it will be used for the next block).
            let qc = QC {
                hash: block.digest(),
                view: block.view,
                view_round: 1,
                votes: Vec::new(),
            };
            let digest = qc.digest();
            let votes: Vec<_> = keys
                .iter()
                .map(|(public_key, secret_key)| (*public_key, Signature::new(&digest, secret_key)))
                .collect();
            latest_qc = QC { votes, ..qc };

            // Return the block.
            block
        })
        .collect()
}

// Fixture
pub fn listener(address: SocketAddr, expected: Option<Bytes>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let listener = TcpListener::bind(&address).await.unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let transport = Framed::new(socket, LengthDelimitedCodec::new());
        let (mut writer, mut reader) = transport.split();
        match reader.next().await {
            Some(Ok(received)) => {
                writer.send(Bytes::from("Ack")).await.unwrap();
                if let Some(expected) = expected {
                    // println!("received message");
                    // println!("expected: {:?}", expected);
                    // println!("received: {:?}", received);
                    assert_eq!(received.freeze(), expected);
                }
            }
            _ => panic!("Failed to receive network message"),
        }
    })
}