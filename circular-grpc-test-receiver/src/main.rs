//! Circular Salsa: standalone Fast gRPC test receiver.
//!
//! Implements `fast_tx.FastTx` (the production Fast contract) and logs every
//! received `SendTransaction` as one JSONL record, so the export stream of a
//! running validator can be validated byte-for-byte (via `wire_sha256`)
//! against the transactions that were injected.
//!
//! ```text
//! circular-grpc-test-receiver \
//!     --listen 127.0.0.1:50051 \
//!     --output received.jsonl \
//!     [--expect-api-key SECRET] \
//!     [--slow-ms 1000]        # simulate a slow consumer
//! ```

#![allow(clippy::arithmetic_side_effects)]

use {
    circular_transaction_exporter::proto::{
        SendTransactionRequest, SendTransactionResponse,
        fast_tx_server::{FastTx, FastTxServer},
    },
    std::{
        fs::File,
        io::{BufWriter, Write},
        net::SocketAddr,
        path::PathBuf,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering},
        },
        time::Duration,
    },
    tokio::time::sleep,
    tonic::{Request, Response, Status, transport::Server},
};

#[derive(Default)]
struct Counters {
    requests_received: AtomicU64,
    transactions_recorded: AtomicU64,
    bytes_received: AtomicU64,
    auth_failures: AtomicU64,
    bad_flags: AtomicU64,
}

impl Counters {
    fn report(&self) {
        eprintln!(
            "receiver: requests_received={} transactions_recorded={} bytes_received={} \
             auth_failures={} bad_flags={}",
            self.requests_received.load(Ordering::Relaxed),
            self.transactions_recorded.load(Ordering::Relaxed),
            self.bytes_received.load(Ordering::Relaxed),
            self.auth_failures.load(Ordering::Relaxed),
            self.bad_flags.load(Ordering::Relaxed),
        );
    }
}

struct ReceiverService {
    counters: Arc<Counters>,
    output: Mutex<Box<dyn Write + Send>>,
    slow: Duration,
    expected_api_key: Option<String>,
}

impl ReceiverService {
    fn record(&self, request: &SendTransactionRequest) -> Result<Option<String>, Status> {
        let wire = &request.transaction;
        let signature = primary_signature(wire);
        let record = serde_json::json!({
            "wire_size": wire.len(),
            "wire_sha256": hex::encode(solana_sha256_hasher::hash(wire).to_bytes()),
            "primary_signature": signature,
            "forward": request.forward,
            "memo": request.memo,
            "cashback_address": request.cashback_address,
        });

        {
            let mut output = self.output.lock().expect("output writer poisoned");
            writeln!(output, "{record}")
                .map_err(|err| Status::internal(format!("failed to write record: {err}")))?;
            output
                .flush()
                .map_err(|err| Status::internal(format!("failed to flush records: {err}")))?;
        }

        self.counters
            .transactions_recorded
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .bytes_received
            .fetch_add(wire.len() as u64, Ordering::Relaxed);
        Ok(signature)
    }
}

#[tonic::async_trait]
impl FastTx for ReceiverService {
    async fn send_transaction(
        &self,
        request: Request<SendTransactionRequest>,
    ) -> Result<Response<SendTransactionResponse>, Status> {
        if let Some(expected) = &self.expected_api_key {
            let authorized = request
                .metadata()
                .get("x-api-key")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value == expected);
            if !authorized {
                self.counters.auth_failures.fetch_add(1, Ordering::Relaxed);
                return Err(Status::unauthenticated("invalid or missing x-api-key"));
            }
        }

        self.counters
            .requests_received
            .fetch_add(1, Ordering::Relaxed);

        let message = request.get_ref();
        // The validator integration must always disable full Fast processing.
        if message.forward != Some(false) {
            self.counters.bad_flags.fetch_add(1, Ordering::Relaxed);
            eprintln!("receiver: unexpected flags forward={:?}", message.forward);
        }

        if !self.slow.is_zero() {
            sleep(self.slow).await;
        }

        let signature = self.record(message)?;
        let request_id = self
            .counters
            .requests_received
            .load(Ordering::Relaxed)
            .to_string();

        Ok(Response::new(SendTransactionResponse {
            signature: signature.unwrap_or_default(),
            bundle_id: None,
            request_id,
        }))
    }
}

/// Base58 of the first signature of a Solana wire-format transaction: a
/// compact-u16 signature count followed by 64-byte signatures.
fn primary_signature(wire: &[u8]) -> Option<String> {
    let (count, offset) = decode_compact_u16(wire)?;
    if count == 0 {
        return None;
    }
    let signature = wire.get(offset..offset + 64)?;
    Some(bs58::encode(signature).into_string())
}

fn decode_compact_u16(bytes: &[u8]) -> Option<(usize, usize)> {
    let mut value = 0usize;
    for (index, byte) in bytes.iter().take(3).enumerate() {
        value |= ((byte & 0x7f) as usize) << (7 * index);
        if byte & 0x80 == 0 {
            return Some((value, index + 1));
        }
    }
    None
}

struct Args {
    listen: SocketAddr,
    output: Option<PathBuf>,
    slow_ms: u64,
    expect_api_key: Option<String>,
    stats_interval_secs: u64,
}

fn parse_args() -> Args {
    let mut args = Args {
        listen: "127.0.0.1:50051".parse().unwrap(),
        output: None,
        slow_ms: 0,
        expect_api_key: None,
        stats_interval_secs: 5,
    };
    let mut iter = std::env::args().skip(1);
    while let Some(flag) = iter.next() {
        let mut value = |flag: &str| {
            iter.next()
                .unwrap_or_else(|| fail(&format!("{flag} requires a value")))
        };
        match flag.as_str() {
            "--listen" => {
                let raw = value("--listen");
                args.listen = raw
                    .parse()
                    .unwrap_or_else(|err| fail(&format!("invalid --listen address {raw}: {err}")));
            }
            "--output" => args.output = Some(PathBuf::from(value("--output"))),
            "--slow-ms" => {
                let raw = value("--slow-ms");
                args.slow_ms = raw
                    .parse()
                    .unwrap_or_else(|err| fail(&format!("invalid --slow-ms {raw}: {err}")));
            }
            "--expect-api-key" => args.expect_api_key = Some(value("--expect-api-key")),
            "--stats-interval-secs" => {
                let raw = value("--stats-interval-secs");
                args.stats_interval_secs = raw.parse().unwrap_or_else(|err| {
                    fail(&format!("invalid --stats-interval-secs {raw}: {err}"))
                });
            }
            "--help" | "-h" => {
                eprintln!(
                    "usage: circular-grpc-test-receiver [--listen ADDR] [--output PATH] \
                     [--slow-ms MS] [--expect-api-key KEY] [--stats-interval-secs SECS]"
                );
                std::process::exit(0);
            }
            other => fail(&format!("unknown flag: {other}")),
        }
    }
    args
}

fn fail(message: &str) -> ! {
    eprintln!("error: {message}");
    std::process::exit(1);
}

#[tokio::main]
async fn main() {
    let args = parse_args();

    // Append: a receiver restart must never wipe previously collected records.
    let output: Box<dyn Write + Send> = match &args.output {
        Some(path) => Box::new(BufWriter::new(
            File::options()
                .create(true)
                .append(true)
                .open(path)
                .unwrap_or_else(|err| fail(&format!("cannot open {}: {err}", path.display()))),
        )),
        None => Box::new(std::io::stdout()),
    };

    let counters = Arc::new(Counters::default());
    let service = ReceiverService {
        counters: counters.clone(),
        output: Mutex::new(output),
        slow: Duration::from_millis(args.slow_ms),
        expected_api_key: args.expect_api_key,
    };

    let reporter_counters = counters.clone();
    let reporter = tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(Duration::from_secs(args.stats_interval_secs.max(1)));
        interval.tick().await; // immediate first tick
        loop {
            interval.tick().await;
            reporter_counters.report();
        }
    });

    eprintln!("receiver: listening on {} (fast_tx.FastTx)", args.listen);
    if args.slow_ms > 0 {
        eprintln!(
            "receiver: simulating a slow consumer, {}ms per request",
            args.slow_ms
        );
    }
    let shutdown_counters = counters.clone();
    let shutdown = async move {
        let _ = tokio::signal::ctrl_c().await;
        eprintln!("receiver: shutting down");
        shutdown_counters.report();
    };
    Server::builder()
        .add_service(FastTxServer::new(service))
        .serve_with_shutdown(args.listen, shutdown)
        .await
        .unwrap_or_else(|err| fail(&format!("server failed: {err}")));

    reporter.abort();
    counters.report();
}
