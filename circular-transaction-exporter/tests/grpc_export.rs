//! Gate 2 integration tests: exporter → Fast gRPC → in-process test server.
//!
//! Validates byte-for-byte delivery of the wire transaction, the mandatory
//! `forward = false` flag, the `x-api-key` metadata, resilience against dead
//! and stalled endpoints, and clean shutdown. The sigverify side (which
//! transactions reach the exporter at all) is covered in `solana-core`'s
//! sigverify tests.

#![allow(clippy::arithmetic_side_effects)]

use {
    circular_transaction_exporter::{
        CircularExportConfig, CircularTransactionExporter, TransactionSource, VerifiedPacket,
        VerifiedPacketBatch,
        proto::{
            SendTransactionResponse,
            fast_tx_server::{FastTx, FastTxServer},
        },
        unix_nanos_now,
    },
    std::{
        collections::HashSet,
        net::SocketAddr,
        time::{Duration, Instant},
    },
    tokio::sync::mpsc,
    tonic::{
        Request, Response, Status,
        transport::{Server, server::TcpIncoming},
    },
};

const RECV_TIMEOUT: Duration = Duration::from_secs(10);
const JOIN_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug)]
struct Received {
    api_key: Option<String>,
    transaction: Vec<u8>,
    forward: Option<bool>,
}

struct RecordingService {
    events: mpsc::UnboundedSender<Received>,
    /// Accept the call but never return, simulating a stalled consumer.
    stall: bool,
}

#[tonic::async_trait]
impl FastTx for RecordingService {
    async fn send_transaction(
        &self,
        request: Request<circular_transaction_exporter::proto::SendTransactionRequest>,
    ) -> Result<Response<SendTransactionResponse>, Status> {
        let api_key = request
            .metadata()
            .get("x-api-key")
            .and_then(|value| value.to_str().ok())
            .map(String::from);
        let message = request.into_inner();
        let _ = self.events.send(Received {
            api_key,
            transaction: message.transaction,
            forward: message.forward,
        });

        if self.stall {
            std::future::pending::<()>().await;
        }

        Ok(Response::new(SendTransactionResponse {
            signature: "sig".to_string(),
            bundle_id: None,
            request_id: "req".to_string(),
        }))
    }
}

fn spawn_server(stall: bool) -> (SocketAddr, mpsc::UnboundedReceiver<Received>) {
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = incoming.local_addr().unwrap();
    let (events, event_receiver) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(FastTxServer::new(RecordingService { events, stall }))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    (addr, event_receiver)
}

fn test_config(addr: SocketAddr) -> CircularExportConfig {
    CircularExportConfig {
        url: format!("http://{addr}"),
        api_key: "test-key".to_string(),
        connect_timeout: Duration::from_millis(500),
        request_timeout: Duration::from_secs(1),
        ..CircularExportConfig::default()
    }
}

fn packet_batch(payloads: &[(&[u8], TransactionSource)]) -> VerifiedPacketBatch {
    VerifiedPacketBatch {
        packets: payloads
            .iter()
            .map(|(bytes, source)| VerifiedPacket {
                transaction: bytes.to_vec(),
                source: *source,
            })
            .collect(),
        received_at_unix_nanos: unix_nanos_now(),
    }
}

async fn next_event(receiver: &mut mpsc::UnboundedReceiver<Received>) -> Received {
    tokio::time::timeout(RECV_TIMEOUT, receiver.recv())
        .await
        .expect("timed out waiting for server event")
        .expect("server event channel closed")
}

/// Join the exporter thread with a timeout so a regression can never hang the
/// test suite.
async fn join_exporter(exporter: CircularTransactionExporter) {
    tokio::time::timeout(
        JOIN_TIMEOUT,
        tokio::task::spawn_blocking(move || exporter.join().unwrap()),
    )
    .await
    .expect("exporter did not shut down in time")
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn roundtrip_bytes_flags_and_api_key() {
    let (addr, mut events) = spawn_server(false);
    let (sender, exporter) =
        CircularTransactionExporter::spawn(test_config(addr), "test-identity".to_string());

    let payloads: Vec<Vec<u8>> = vec![
        (0u8..=255).collect(),
        vec![0u8; 1232],
        b"\x01circular".to_vec(),
    ];
    sender.try_send(packet_batch(
        &payloads
            .iter()
            .map(|bytes| (bytes.as_slice(), TransactionSource::Tpu))
            .collect::<Vec<_>>(),
    ));

    let mut received = HashSet::new();
    for _ in 0..payloads.len() {
        let event = next_event(&mut events).await;
        // Core Gate 2 assertions: exact wire bytes and mandatory Fast flags.
        assert_eq!(event.api_key.as_deref(), Some("test-key"));
        assert_eq!(event.forward, Some(false));
        received.insert(event.transaction);
    }

    let expected: HashSet<Vec<u8>> = payloads.into_iter().collect();
    assert_eq!(received, expected);

    drop(sender);
    join_exporter(exporter).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_transactions_are_submitted_before_shutdown() {
    let (addr, mut events) = spawn_server(false);
    let (sender, exporter) =
        CircularTransactionExporter::spawn(test_config(addr), "identity".to_string());

    // Drop the sender immediately after enqueueing: the exporter must still
    // deliver the transaction before shutting down.
    sender.try_send(packet_batch(&[(b"last-words", TransactionSource::Tpu)]));
    drop(sender);

    let event = next_event(&mut events).await;
    assert_eq!(event.transaction, b"last-words");
    join_exporter(exporter).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dead_endpoint_never_blocks_the_sender_and_shuts_down_cleanly() {
    // Reserve a port and close it again: nothing listens there.
    let dead_addr = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };
    let (sender, exporter) =
        CircularTransactionExporter::spawn(test_config(dead_addr), "identity".to_string());

    // The sender must absorb load instantly while calls fail against the dead
    // endpoint.
    let start = Instant::now();
    for index in 0..10_000u32 {
        sender.try_send(packet_batch(&[(
            index.to_le_bytes().as_slice(),
            TransactionSource::Tpu,
        )]));
    }
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "try_send must never block, took {:?}",
        start.elapsed()
    );

    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(sender);
    join_exporter(exporter).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_consumer_never_blocks_and_shuts_down() {
    let (addr, _events) = spawn_server(true);
    // Small in-flight limit: once saturated by the stalled consumer, further
    // transactions are dropped rather than queued.
    let config = CircularExportConfig {
        max_in_flight: 4,
        queue_capacity: 16,
        ..test_config(addr)
    };
    let (sender, exporter) = CircularTransactionExporter::spawn(config, "identity".to_string());

    // Flood: the sender must never block even though every in-flight slot is
    // held by the stalled consumer.
    let start = Instant::now();
    for index in 0..10_000u32 {
        sender.try_send(packet_batch(&[(
            index.to_le_bytes().as_slice(),
            TransactionSource::Tpu,
        )]));
    }
    assert!(
        start.elapsed() < Duration::from_secs(1),
        "try_send must never block, took {:?}",
        start.elapsed()
    );

    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(sender);
    join_exporter(exporter).await;
}
