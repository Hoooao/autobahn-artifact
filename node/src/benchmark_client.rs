// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::BytesMut;
use clap::{crate_name, crate_version, App, AppSettings};
use env_logger::Env;
use futures::channel;
use futures::future::join_all;
use futures::sink::SinkExt as _;
use log::{info, warn};
use rand::Rng;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::{interval, sleep, Duration, Instant};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio::sync::mpsc;

use crypto::SignatureService;
use crypto::Hash;

use config::KeyPair;
use config::Import as _;


#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("Benchmark client for Sailfish.")
        .args_from_usage("-v... 'Sets the level of verbosity'")
        .args_from_usage("<ADDR> 'The network address of the node where to send txs'")
        .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
        .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
        .args_from_usage("--nodes=[ADDR]... 'Network addresses that must be reachable before starting the benchmark.'")
        .args_from_usage("--keys=<FILE> 'The file containing the key information for the benchmark.'")
        .get_matches();
    let log_level = match matches.occurrences_of("v") {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    env_logger::Builder::from_env(Env::default().default_filter_or(log_level))
        .format_timestamp_millis()
        .init();

    let target = matches
        .value_of("ADDR")
        .unwrap()
        .parse::<SocketAddr>()
        .context("Invalid socket address format")?;
    let size = matches
        .value_of("size")
        .unwrap()
        .parse::<usize>()
        .context("The size of transactions must be a non-negative integer")?;
    let rate = matches
        .value_of("rate")
        .unwrap()
        .parse::<u64>()
        .context("The rate of transactions must be a non-negative integer")?;
    let nodes = matches
        .values_of("nodes")
        .unwrap_or_default()
        .into_iter()
        .map(|x| x.parse::<SocketAddr>())
        .collect::<Result<Vec<_>, _>>()
        .context("Invalid socket address format")?;

    let key_file = matches.value_of("keys").unwrap();
    

    info!("Node address: {}", target);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions size: {} B", size);

    // NOTE: This log entry is used to compute performance.
    info!("Transactions rate: {} tx/s", rate);
    info!("Key file provided: {}", key_file);


    let secret = KeyPair::import(key_file).context("Failed to load the node's keypair")?;
    let secret_key = secret.secret;

    // Make the data store.
    let signature_service = SignatureService::new(secret_key);
    
    let mut client = Client {
        target,
        size,
        rate,
        nodes,
        signature_service
    };

    // Wait for all nodes to be online and synchronized.
    client.wait().await;

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

struct Client {
    target: SocketAddr,  //specifies the worker to connect to
    size: usize,         //specifies the bit size of transactions
    rate: u64,
    nodes: Vec<SocketAddr>,
    // ========= Added for Evaluation purposes ========= 
    signature_service: SignatureService,
}

async fn sign(signature_service: &mut SignatureService, tx: &BytesMut) -> [u8; 64]
{
    let digest = tx.as_ref().digest();
    let signature = signature_service.request_signature(digest).await;
    signature.flatten()
}


impl Client {
    pub async fn send(&mut self) -> Result<()> {
        const PRECISION: u64 = 20; // Sample precision.
        const BURST_DURATION: u64 = 500 / PRECISION;

        // The transaction size must be at least 16 bytes to ensure all txs are different.
        if self.size < 9 {
            return Err(anyhow::Error::msg(
                "Transaction size must be at least 16 bytes",
            ));
        }

        // Connect to the mempool.
        let stream = TcpStream::connect(self.target)
            .await
            .context(format!("failed to connect to {}", self.target))?;

        // Create a channel so we can sign transactions concurrently and send from a single task
        let (channel_tx, mut channel_rx) = mpsc::channel(100);

        // Submit all transactions.
        let burst = self.rate / PRECISION;
        let tx = BytesMut::with_capacity(self.size + 64); // + 64 for signatures
        let mut counter = 0;
        let mut r :u64 = rand::thread_rng().gen();
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());
        let interval = interval(Duration::from_millis(BURST_DURATION));
        tokio::pin!(interval);


        // Spawn a task to read from channel and send transactions
        tokio::spawn(async move {
            while let Some(message) = channel_rx.recv().await {
                //Note: Does not sign transactions. Transaction id-s are not unique w.r.t to content.
                if let Err(e) = transport.send(message).await { //Uses TCP connection to send request to assigned worker. Note: Optimistically only sending to one worker.
                    warn!("Failed to send transaction: {}", e);
                    return;
                }
            }
        });

        // NOTE: This log entry is used to compute performance.
        info!("Start sending transactions");
        'main: loop {
            interval.as_mut().tick().await;
            let now = Instant::now();

            let mut tx = tx.clone();     
            let counter_copy = counter.clone();
            let mut r_copy = r.clone();
            let size = self.size;
            let mut sig_copy = self.signature_service.clone();
            let channel_tx = channel_tx.clone();
            tokio::spawn(async move {
                for x in 0..burst {
                    let msg = if x == counter_copy % burst {
                        // NOTE: This log entry is used to compute performance.
                        info!("Sending sample transaction {}", counter_copy);

                        tx.put_u8(0u8); // Sample txs start with 0.
                        tx.put_u64(counter_copy); // This counter identifies the tx.
                        tx.resize(size, 0u8);

                        
                        for b in sign(&mut sig_copy, &tx).await {
                            tx.put_u8(b);
                        }

                        tx.split().freeze()
                    } else {
                        r_copy += 1;
                        tx.put_u8(1u8); // Standard txs start with 1.
                        tx.put_u64(r_copy); // Ensures all clients send different txs.
                        tx.resize(size, 0u8);

                        for b in sign(&mut sig_copy, &tx).await {
                            tx.put_u8(b);
                        }

                        tx.split().freeze()
                    };
                    if let Err(e) = channel_tx.send(msg).await {
                        warn!("Failed to send message to sender: {}", e);
                        std::process::exit(0);
                    }
                }
            });
            
            if now.elapsed().as_millis() > BURST_DURATION as u128 {
                // NOTE: This log entry is used to compute performance.
                warn!("Transaction rate too high for this client");
            }

            r += burst;
            counter += 1;
        }
        Ok(())
    }

    pub async fn wait(&self) {
        // Wait for all nodes to be online.
        info!("Waiting for all nodes to be online...");
        join_all(self.nodes.iter().cloned().map(|address| {
            tokio::spawn(async move {
                while TcpStream::connect(address).await.is_err() {
                    sleep(Duration::from_millis(10)).await;
                }
            })
        }))
        .await;
    }
}
