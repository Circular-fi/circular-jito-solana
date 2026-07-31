//! Synthetic client: pushes deterministic payloads through the real
//! Circular exporter to a receiver, for manual protocol validation.
//!
//! ```text
//! circular-grpc-test-receiver --listen 127.0.0.1:50051 --output recv.jsonl &
//! synthetic-client http://127.0.0.1:50051
//! # then compare the printed sha256 list against recv.jsonl
//! ```

#![allow(clippy::arithmetic_side_effects)]

use {
    circular_transaction_exporter::{
        CircularExportConfig, CircularTransactionExporter, TransactionSource, VerifiedPacket,
        VerifiedPacketBatch, unix_nanos_now,
    },
    std::time::Duration,
};

fn main() {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:50051".to_string());
    let api_key = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "synthetic".to_string());

    let config = CircularExportConfig {
        url: url.clone(),
        api_key,
        ..CircularExportConfig::default()
    };
    let (sender, exporter) =
        CircularTransactionExporter::spawn(config, "synthetic-client".to_string());

    println!("sending 10 transactions to {url}:");
    for index in 0..10u8 {
        let payload: Vec<u8> = (0..64 + index as usize)
            .map(|byte| (byte as u8).wrapping_mul(index + 1))
            .collect();
        println!(
            "  sequence={index} wire_size={} wire_sha256={}",
            payload.len(),
            hex::encode(solana_sha256_hasher::hash(&payload).to_bytes()),
        );
        sender.try_send(VerifiedPacketBatch {
            packets: vec![VerifiedPacket {
                transaction: payload,
                source: TransactionSource::Tpu,
            }],
            received_at_unix_nanos: unix_nanos_now(),
        });
        std::thread::sleep(Duration::from_millis(20));
    }

    drop(sender);
    exporter.join().expect("exporter thread panicked");
    println!("done; compare the sha256 list above against the receiver's JSONL output");
}
