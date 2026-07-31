//! A/B benchmark of the sigverify-thread export hand-off cost.
//!
//! Compares the two strategies that hand verified packets to the exporter:
//! - `copy`: [`build_owned_batch`] copies every wire payload into an owned
//!   `VerifiedPacketBatch` (the legacy `try_send` path);
//! - `arc`: [`CircularExportSender::try_send_shared`] only clones the shared
//!   `Arc<Vec<PacketBatch>>` and defers the copy to the exporter thread.
//!
//! Two scenarios matter for a mainnet spike:
//! - `queue_ok`: the exporter keeps up, the queue has room;
//! - `queue_full`: the exporter is saturated (Fast slow/down). The legacy path
//!   still copies before dropping; the shared path skips the work entirely.
//!
//! Run with: `cargo bench -p circular-transaction-exporter --features dev-context-only-utils`

use {
    circular_transaction_exporter::{CircularExportSender, build_owned_batch},
    criterion::{Criterion, criterion_group, criterion_main},
    solana_perf::packet::{PacketBatch, to_packet_batches},
    std::{hint::black_box, sync::Arc, thread},
};

/// Build one `Arc<Vec<PacketBatch>>` of `num_tx` packets of ~`tx_size` bytes,
/// mirroring what sigverify hands downstream.
fn make_batches(num_tx: usize, tx_size: usize) -> Arc<Vec<PacketBatch>> {
    let payloads: Vec<Vec<u8>> = (0..num_tx)
        .map(|index| vec![(index % 251) as u8; tx_size])
        .collect();
    Arc::new(to_packet_batches(&payloads, num_tx.max(1)))
}

fn bench_handoff(c: &mut Criterion) {
    // Representative TPU batch shapes: 1024 tx of 256 and 1024 bytes.
    for (num_tx, tx_size) in [(1024usize, 256usize), (1024usize, 1024usize)] {
        let batches = make_batches(num_tx, tx_size);

        // Scenario 1: the queue has room; a drain thread consumes items so
        // `try_send` always succeeds. The bench thread pays the allocation
        // (copy) or the Arc clone; the drain thread pays the deallocation,
        // exactly like sigverify vs the exporter thread in production.
        {
            let (sender, receiver) = CircularExportSender::new_for_tests(4096);
            let drain = thread::spawn(move || {
                while let Ok(item) = receiver.recv() {
                    black_box(&item);
                }
            });

            let mut group = c.benchmark_group(format!("queue_ok/{num_tx}x{tx_size}"));
            group.bench_function("copy", |b| {
                b.iter(|| {
                    let (owned, _bytes) = build_owned_batch(&batches, false).unwrap();
                    sender.try_send(black_box(owned));
                });
            });
            group.bench_function("arc", |b| {
                b.iter(|| {
                    sender.try_send_shared(black_box(Arc::clone(&batches)), false);
                });
            });
            group.finish();

            drop(sender);
            drain.join().unwrap();
        }

        // Scenario 2: the queue is full (exporter saturated). The receiver is
        // kept alive (so sends report Full, not Disconnected) but never drained.
        {
            let (sender, _receiver) = CircularExportSender::new_for_tests(1);
            // Occupy the single slot so the queue is full for every iteration.
            sender.try_send_shared(Arc::clone(&batches), false);

            let mut group = c.benchmark_group(format!("queue_full/{num_tx}x{tx_size}"));
            // Legacy path: builds the full copy, then drops it on the full queue.
            group.bench_function("copy", |b| {
                b.iter(|| {
                    let (owned, _bytes) = build_owned_batch(&batches, false).unwrap();
                    sender.try_send(black_box(owned));
                });
            });
            // Shared path: sees the full queue and returns before any work.
            group.bench_function("arc", |b| {
                b.iter(|| {
                    sender.try_send_shared(black_box(Arc::clone(&batches)), false);
                });
            });
            group.finish();
        }
    }
}

criterion_group!(benches, bench_handoff);
criterion_main!(benches);
