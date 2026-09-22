//! Availability regression test: a Fast endpoint that is unreachable at
//! validator boot must NOT permanently disable the exporter. The exporter has
//! to keep retrying with backoff and start delivering on its own once the
//! endpoint comes online — without restarting the validator.

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
        net::SocketAddr,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    },
    tokio::sync::mpsc,
    tonic::{
        Request, Response, Status,
        transport::{Server, server::TcpIncoming},
    },
};

const RECV_TIMEOUT: Duration = Duration::from_secs(10);
const JOIN_TIMEOUT: Duration = Duration::from_secs(15);

struct Received {
    transaction: Vec<u8>,
}

struct RecordingService {
    events: mpsc::UnboundedSender<Received>,
}

#[tonic::async_trait]
impl FastTx for RecordingService {
    async fn send_transaction(
        &self,
        request: Request<circular_transaction_exporter::proto::SendTransactionRequest>,
    ) -> Result<Response<SendTransactionResponse>, Status> {
        let message = request.into_inner();
        let _ = self.events.send(Received {
            transaction: message.transaction,
        });
        Ok(Response::new(SendTransactionResponse {
            signature: "sig".to_string(),
            bundle_id: None,
            request_id: "req".to_string(),
        }))
    }
}

/// Bind the mock Fast server on a *specific* address, so it can be brought up
/// on the same port the exporter has been failing to reach.
fn spawn_server_on(addr: SocketAddr) -> mpsc::UnboundedReceiver<Received> {
    let incoming = TcpIncoming::bind(addr).unwrap();
    let (events, event_receiver) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        Server::builder()
            .add_service(FastTxServer::new(RecordingService { events }))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });
    event_receiver
}

fn test_config(addr: SocketAddr) -> CircularExportConfig {
    CircularExportConfig {
        url: format!("http://{addr}"),
        api_key: "test-key".to_string(),
        connect_timeout: Duration::from_millis(200),
        request_timeout: Duration::from_secs(1),
        ..CircularExportConfig::default()
    }
}

fn packet_batch(payload: &[u8]) -> VerifiedPacketBatch {
    VerifiedPacketBatch {
        packets: vec![VerifiedPacket {
            transaction: payload.to_vec(),
            source: TransactionSource::Tpu,
        }],
        received_at_unix_nanos: unix_nanos_now(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovers_when_endpoint_comes_up_after_start() {
    // Reserve a port, then free it: nothing is listening at startup.
    let addr = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };

    let (sender, exporter) = CircularTransactionExporter::spawn_with_dedup_ttl(
        test_config(addr),
        "identity".to_string(),
        Duration::ZERO,
    );
    let metrics = sender.metrics();

    // Let the exporter fail its initial connection and enter the backoff loop.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        metrics.connected.load(Ordering::Relaxed),
        0,
        "exporter should report disconnected while the endpoint is down"
    );
    assert!(
        metrics.reconnect_attempts.load(Ordering::Relaxed) >= 1,
        "exporter should have recorded at least one failed connection attempt"
    );

    // Bring the endpoint up on the same port.
    let mut events = spawn_server_on(addr);

    // Keep feeding transactions across the reconnect window; dedup is disabled
    // so every copy that reaches Fast is delivered. A stop flag ends the feeder
    // as soon as delivery is confirmed, to keep the test fast.
    let stop = Arc::new(AtomicBool::new(false));
    let feeder_stop = stop.clone();
    let feeder = sender.clone();
    let feeder_handle = std::thread::spawn(move || {
        while !feeder_stop.load(Ordering::Relaxed) {
            feeder.try_send(packet_batch(b"after-reconnect"));
            std::thread::sleep(Duration::from_millis(50));
        }
    });

    // A submission must arrive without any validator/exporter restart.
    let event = tokio::time::timeout(RECV_TIMEOUT, events.recv())
        .await
        .expect("exporter did not reconnect and deliver in time")
        .expect("event channel closed");
    assert_eq!(event.transaction, b"after-reconnect");

    assert_eq!(
        metrics.connected.load(Ordering::Relaxed),
        1,
        "exporter should report connected after recovery"
    );

    stop.store(true, Ordering::Relaxed);
    feeder_handle.join().unwrap();
    drop(sender);
    join_exporter(exporter).await;
}
