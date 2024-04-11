use crate::error::{ConsensusError, ConsensusResult};
use crypto::PublicKey;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;

pub type Stake = u32;
pub type EpochNumber = u128;

#[derive(Serialize, Deserialize)]
pub struct Parameters {
    pub timeout_delay: u64,
    pub sync_retry_delay: u64,
    pub max_payload_size: usize,
    pub min_block_delay: u64,

    //asynchrony framework parameters:
    pub simulate_asynchrony: bool,
    pub async_type: VecDeque<u8>,
    pub asynchrony_start: VecDeque<u64>,
    pub asynchrony_duration: VecDeque<u64>,
    pub affected_nodes: VecDeque<u64>,
    pub egress_penalty: u64,
    pub use_exponential_timeouts: bool,
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            timeout_delay: 5000,
            sync_retry_delay: 10_000,
            max_payload_size: 500,
            min_block_delay: 100,
            simulate_asynchrony: false,
            async_type: vec![0].into(),
            asynchrony_start: vec![20_000].into(),
            asynchrony_duration: vec![10_000].into(),
            affected_nodes: vec![0].into(),
            egress_penalty: 0,
            use_exponential_timeouts: false,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Authority {
    pub name: PublicKey,
    pub stake: Stake,
    pub address: SocketAddr,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Committee {
    pub authorities: HashMap<PublicKey, Authority>,
    pub epoch: EpochNumber,
}

impl Committee {
    pub fn new(info: Vec<(PublicKey, Stake, SocketAddr)>, epoch: EpochNumber) -> Self {
        Self {
            authorities: info
                .into_iter()
                .map(|(name, stake, address)| {
                    let authority = Authority {
                        name,
                        stake,
                        address,
                    };
                    (name, authority)
                })
                .collect(),
            epoch,
        }
    }

    pub fn size(&self) -> usize {
        self.authorities.len()
    }

    pub fn stake(&self, name: &PublicKey) -> Stake {
        self.authorities.get(&name).map_or_else(|| 0, |x| x.stake)
    }

    pub fn quorum_threshold(&self) -> Stake {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (2 N + 3) / 3 = 2f + 1 + (2k + 2)/3 = 2f + 1 + k = N - f
        let total_votes: Stake = self.authorities.values().map(|x| x.stake).sum();
        2 * total_votes / 3 + 1
    }

    pub fn address(&self, name: &PublicKey) -> ConsensusResult<SocketAddr> {
        self.authorities
            .get(name)
            .map(|x| x.address)
            .ok_or_else(|| ConsensusError::NotInCommittee(*name))
    }

    pub fn broadcast_addresses(&self, myself: &PublicKey) -> Vec<SocketAddr> {
        self.authorities
            .values()
            .filter(|x| x.name != *myself)
            .map(|x| x.address)
            .collect()
    }

    pub fn partition_broadcast_addresses(&self, myself: &PublicKey, our_partition: bool, partition_public_keys: &HashSet<PublicKey>) -> Vec<SocketAddr> {
        self.authorities
            .values()
            .filter(|x| x.name != *myself)
            .filter(|x| (our_partition && partition_public_keys.contains(&x.name)) || (!our_partition && !partition_public_keys.contains(&x.name)))
            .map(|x| x.address)
            .collect()
    }
}
