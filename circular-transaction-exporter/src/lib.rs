//! Circular Fast transaction exporter.
//!
//! Streams BAM sigverify-verified transactions to Circular Fast without ever
//! blocking the validator's transaction pipeline. The BAM receive path hands
//! verified packet batches to a bounded queue via `try_send`; a dedicated
//! exporter thread submits them over unary gRPC `SendTransaction` calls.
//! When the queue is full or the consumer is unavailable, events are dropped
//! and counted — the validator never waits on the exporter.

#![allow(clippy::arithmetic_side_effects)]

pub mod config;
pub mod event;
pub mod metrics;
pub mod sender;
pub mod service;

pub mod proto {
    #![allow(clippy::missing_const_for_fn)]
    // The tonic-generated server code spells out `Default::default()`.
    #![allow(clippy::default_trait_access)]
    include!(concat!(env!("OUT_DIR"), "/fast_tx.rs"));
}

pub use {
    config::CircularExportConfig,
    event::{
        ExportItem, SharedVerifiedBatch, TransactionSource, VerifiedPacket, VerifiedPacketBatch,
        build_owned_batch, unix_nanos_now,
    },
    sender::CircularExportSender,
    service::CircularTransactionExporter,
};

#[cfg(feature = "dev-context-only-utils")]
pub use sender::HookMode;
