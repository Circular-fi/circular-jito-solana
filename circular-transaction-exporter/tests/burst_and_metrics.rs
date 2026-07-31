//! Priority regression tests for the Circular exporter, closing the gaps
//! identified in the exporter audit:
//! - metrics counters are never asserted directly by any other test;
//! - the burst/`max_in_flight` exhaustion behavior (multiple waves >=
//!   `max_in_flight` arriving almost simultaneously) is not characterized;
//! - recovery after saturation (no permit leak) is never verified.
//!
//! Requires `--features dev-context-only-utils` (for `CircularExportSender::metrics()`).

#![allow(clippy::arithmetic_side_effects)]

use {
    circular_transaction_exporter::{
        CircularExportConfig, CircularTransactionExporter, TransactionSource, VerifiedPacket,
        VerifiedPacketBatch,
        metrics::CircularExportMetrics,
        proto::{
            SendTransactionRequest, SendTransactionResponse,
            fast_tx_server::{FastTx, FastTxServer},
        },
        unix_nanos_now,
    },
    std::{
        net::SocketAddr,
        sync::{Arc, atomic::Ordering},
        time::{Duration, Instant},
    },
    tonic::{
        Request, Response, Status,
        transport::{Server, server::TcpIncoming},
    },
};

const POLL_INTERVAL: Duration = Duration::from_millis(5);
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const JOIN_TIMEOUT: Duration = Duration::from_secs(15);

/// Test server that waits `delay` before answering every call. Unlike the
/// `stall: bool` server in `grpc_export.rs` (which never answers at all),
/// a bounded delay lets tests create a *deterministic* saturation window and
/// then observe recovery once the delay elapses.
struct DelayedService {
    delay: Duration,
}

#[tonic::async_trait]
impl FastTx for DelayedService {
    async fn send_transaction(
        &self,
        _request: Request<SendTransactionRequest>,
    ) -> Result<Response<SendTransactionResponse>, Status> {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        Ok(Response::new(SendTransactionResponse {
            signature: "sig".to_string(),
            bundle_id: None,
            request_id: "req".to_string(),
        }))
    }
}

fn spawn_server(delay: Duration) -> SocketAddr {
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = incoming.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(FastTxServer::new(DelayedService { delay }))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    addr
}

fn test_config(addr: SocketAddr) -> CircularExportConfig {
    CircularExportConfig {
        url: format!("http://{addr}"),
        api_key: "test-key".to_string(),
        connect_timeout: Duration::from_millis(500),
        request_timeout: Duration::from_secs(5),
        ..CircularExportConfig::default()
    }
}

/// A batch of `count` distinct, tiny transactions tagged with `wave` so
/// different waves are distinguishable if ever needed for debugging.
fn packet_batch(count: usize, wave: u8) -> VerifiedPacketBatch {
    VerifiedPacketBatch {
        packets: (0..count)
            .map(|index| VerifiedPacket {
                transaction: vec![wave, index as u8, (index >> 8) as u8],
                source: TransactionSource::Tpu,
            })
            .collect(),
        received_at_unix_nanos: unix_nanos_now(),
    }
}

/// Poll `metrics` until `predicate` holds, or panic after `WAIT_TIMEOUT`.
/// Avoids flaky fixed `sleep`s around asynchronous gRPC completions.
async fn wait_until(metrics: &Arc<CircularExportMetrics>, predicate: impl Fn(&CircularExportMetrics) -> bool) {
    let start = Instant::now();
    while !predicate(metrics) {
        assert!(
            start.elapsed() < WAIT_TIMEOUT,
            "condition not met within {WAIT_TIMEOUT:?}",
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn join_exporter(exporter: CircularTransactionExporter) {
    tokio::time::timeout(
        JOIN_TIMEOUT,
        tokio::task::spawn_blocking(move || exporter.join().unwrap()),
    )
    .await
    .expect("exporter did not shut down in time")
    .unwrap();
}

/// Test A: on a healthy, instantly-responding endpoint, every metric must
/// land on an exact, predictable value. A regression that breaks
/// `record_hook`/`enqueued_transactions`/`sent_ok` bookkeeping would not be
/// caught by any of the existing behavior-only tests.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_are_exact_on_the_happy_path() {
    const N: u64 = 50;

    let addr = spawn_server(Duration::ZERO);
    let (sender, exporter) =
        CircularTransactionExporter::spawn(test_config(addr), "test-identity".to_string());
    let metrics = sender.metrics();

    sender.try_send(packet_batch(N as usize, 0));

    wait_until(&metrics, |m| m.sent_ok.load(Ordering::Relaxed) == N).await;

    assert_eq!(metrics.received_transactions.load(Ordering::Relaxed), N);
    assert_eq!(metrics.enqueued_transactions.load(Ordering::Relaxed), N);
    assert_eq!(metrics.sent_ok.load(Ordering::Relaxed), N);
    assert_eq!(metrics.sent_err.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.dropped_batches.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.dropped_transactions.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.dropped_no_permit.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.dropped_timeout.load(Ordering::Relaxed), 0);

    drop(sender);
    join_exporter(exporter).await;
}

/// Test B + C: reproduces the "several waves >= `max_in_flight` arriving
/// almost simultaneously" scenario traced in the exporter audit, then
/// verifies recovery once the in-flight budget frees back up.
///
/// The response delay is long enough that all three waves are dispatched by
/// the exporter's single-threaded extraction loop well before any response
/// can land — mirroring a burst arriving within one Fast round-trip.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn burst_exceeding_max_in_flight_drops_overflow_then_recovers() {
    const RESPONSE_DELAY: Duration = Duration::from_millis(300);
    const MAX_IN_FLIGHT: u64 = 16;
    const WAVE_SIZE: u64 = 16;
    const NUM_WAVES: u64 = 3;
    const TOTAL: u64 = WAVE_SIZE * NUM_WAVES;

    let addr = spawn_server(RESPONSE_DELAY);
    let config = CircularExportConfig {
        max_in_flight: MAX_IN_FLIGHT as usize,
        queue_capacity: 256,
        ..test_config(addr)
    };
    let (sender, exporter) = CircularTransactionExporter::spawn(config, "identity".to_string());
    let metrics = sender.metrics();

    // --- Phase B: fire all three waves back-to-back, no pause in between. ---
    for wave in 0..NUM_WAVES {
        sender.try_send(packet_batch(WAVE_SIZE as usize, wave as u8));
    }

    // Every transaction must reach the hook...
    wait_until(&metrics, |m| {
        m.received_transactions.load(Ordering::Relaxed) == TOTAL
    })
    .await;

    // ...and, once every delayed response has resolved, be accounted for
    // exactly once: either sent, or dropped for lack of a permit. Nothing
    // must vanish silently.
    wait_until(&metrics, |m| {
        m.sent_ok.load(Ordering::Relaxed) + m.dropped_no_permit.load(Ordering::Relaxed) == TOTAL
    })
    .await;

    let sent_ok_after_burst = metrics.sent_ok.load(Ordering::Relaxed);
    let dropped_no_permit_after_burst = metrics.dropped_no_permit.load(Ordering::Relaxed);
    assert_eq!(
        sent_ok_after_burst + dropped_no_permit_after_burst,
        TOTAL,
        "every transaction must be either sent or accounted as dropped, never silently lost"
    );
    // Only the first wave should fit in the in-flight budget; allow a little
    // slack for early permits that may already have started to free up by
    // the time later waves are processed.
    assert!(
        (MAX_IN_FLIGHT..=MAX_IN_FLIGHT + 4).contains(&sent_ok_after_burst),
        "expected sent_ok close to max_in_flight ({MAX_IN_FLIGHT}), got {sent_ok_after_burst}"
    );
    assert!(
        dropped_no_permit_after_burst >= TOTAL - MAX_IN_FLIGHT - 4,
        "expected most of the overflow to be dropped for lack of a permit, got {dropped_no_permit_after_burst}"
    );
    assert_eq!(metrics.dropped_batches.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.dropped_timeout.load(Ordering::Relaxed), 0);

    // --- Phase C: once the burst's delayed responses have all landed and
    // released their permits, a fresh wave must go through cleanly. If a
    // permit were ever leaked (e.g. a code path that acquires but never
    // drops its guard), the semaphore would stay exhausted and this wave
    // would either be dropped or time out waiting for `sent_ok` to advance. ---
    sender.try_send(packet_batch(WAVE_SIZE as usize, 99));

    wait_until(&metrics, |m| {
        m.sent_ok.load(Ordering::Relaxed) == sent_ok_after_burst + WAVE_SIZE
    })
    .await;

    assert_eq!(
        metrics.dropped_no_permit.load(Ordering::Relaxed),
        dropped_no_permit_after_burst,
        "no additional drops expected once permits have been released back to the semaphore"
    );

    drop(sender);
    join_exporter(exporter).await;
}
