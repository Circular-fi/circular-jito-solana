//! Sends signed transfer transactions straight to a validator's TPU QUIC
//! port, bypassing RPC. Used to exercise the sigverify → Circular export
//! path of a node that receives no organic TPU traffic (e.g. an unstaked
//! testnet validator).
//!
//! ```text
//! tpu-injector --tpu 127.0.0.1:8002 \
//!              --rpc http://127.0.0.1:8899 \
//!              --keypair /path/to/payer.json \
//!              --count 10
//! ```
//!
//! Prints one line per transaction: `<base58 signature> <sha256 of wire bytes>`.

#![allow(clippy::arithmetic_side_effects)]

use {
    solana_client::connection_cache::ConnectionCache,
    solana_connection_cache::client_connection::ClientConnection,
    solana_keypair::{Keypair, read_keypair_file},
    solana_rpc_client::rpc_client::RpcClient,
    solana_signer::Signer,
    std::net::SocketAddr,
};

fn main() {
    let mut tpu: SocketAddr = "127.0.0.1:8002".parse().unwrap();
    let mut rpc = "http://127.0.0.1:8899".to_string();
    let mut keypair_path = String::new();
    let mut count = 10u64;
    let mut corrupt = false;

    let mut iter = std::env::args().skip(1);
    while let Some(flag) = iter.next() {
        let mut value = || iter.next().expect("flag requires a value");
        match flag.as_str() {
            "--tpu" => tpu = value().parse().expect("invalid --tpu address"),
            "--rpc" => rpc = value(),
            "--keypair" => keypair_path = value(),
            "--count" => count = value().parse().expect("invalid --count"),
            // Corrupt the primary signature after signing: these must be
            // discarded by sigverify and never reach the Circular export.
            "--corrupt" => corrupt = true,
            other => panic!("unknown flag: {other}"),
        }
    }
    assert!(!keypair_path.is_empty(), "--keypair is required");

    let payer: Keypair = read_keypair_file(&keypair_path).expect("failed to read keypair");
    let rpc_client = RpcClient::new(rpc);
    let blockhash = rpc_client
        .get_latest_blockhash()
        .expect("failed to fetch a recent blockhash");

    // Distinct lamport amounts make every signature unique under a shared
    // blockhash. Self-transfers keep the payer balance intact minus fees
    // (never charged unless a leader includes them).
    let wire_transactions: Vec<Vec<u8>> = (0..count)
        .map(|index| {
            let transaction = solana_system_transaction::transfer(
                &payer,
                &payer.pubkey(),
                index + 1,
                blockhash,
            );
            let mut wire = bincode::serialize(&transaction).unwrap();
            if corrupt {
                // Byte 1 is inside the primary signature (byte 0 is the
                // compact-u16 signature count).
                wire[1] ^= 0xff;
            }
            println!(
                "{} {}",
                transaction.signatures[0],
                hex::encode(solana_sha256_hasher::hash(&wire).to_bytes()),
            );
            wire
        })
        .collect();

    let cache = ConnectionCache::new_quic("circular-tpu-injector", 1);
    let connection = cache.get_connection(&tpu);
    connection
        .send_data_batch(&wire_transactions)
        .expect("failed to send transactions over QUIC");
    eprintln!("sent {count} transactions to {tpu}");
}
