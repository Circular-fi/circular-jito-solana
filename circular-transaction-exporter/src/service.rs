use {
    crate::{
        config::CircularExportConfig,
        event::ExportItem,
        metrics::CircularExportMetrics,
        proto::{SendTransactionRequest, fast_tx_client::FastTxClient},
        sender::CircularExportSender,
    },
    log::{debug, error, info, warn},
    std::{
        sync::{Arc, atomic::Ordering},
        thread::{Builder, JoinHandle},
        time::{Duration, Instant},
    },
    tokio::{
        sync::{Semaphore, mpsc},
        time::timeout,
    },
    tonic::{
        Request,
        metadata::{Ascii, MetadataValue},
        transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
    },
};

/// Capacity, in packet batches, of the bridge between the blocking queue
/// reader and the async submission loop. Small on purpose: the large buffer
/// is the crossbeam queue; this one only crosses the sync/async boundary.
const BRIDGE_CHANNEL_CAPACITY: usize = 16;
const METRICS_REPORT_INTERVAL: Duration = Duration::from_secs(2);
/// How long to wait for in-flight submissions to drain on shutdown.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Background service owning the exporter thread. Dropping the last
/// [`CircularExportSender`] lets the thread drain and exit; call [`join`]
/// afterwards to wait for it.
///
/// [`join`]: CircularTransactionExporter::join
pub struct CircularTransactionExporter {
    thread_hdl: JoinHandle<()>,
}

impl CircularTransactionExporter {
    /// Spawn the exporter thread and return the non-blocking sender handle to
    /// wire into the sigverify workers.
    pub fn spawn(
        config: CircularExportConfig,
        validator_identity: String,
    ) -> (CircularExportSender, Self) {
        let (batch_sender, batch_receiver) =
            crossbeam_channel::bounded::<ExportItem>(config.queue_capacity);
        let metrics = Arc::new(CircularExportMetrics::default());
        let sender = CircularExportSender::new(batch_sender, metrics.clone(), config.include_votes);

        let thread_hdl = Builder::new()
            .name("circExporter".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build circular exporter runtime");
                runtime.block_on(run_exporter(
                    config,
                    validator_identity,
                    batch_receiver,
                    metrics,
                ));
            })
            .expect("failed to spawn circular exporter thread");

        (sender, Self { thread_hdl })
    }

    pub fn join(self) -> std::thread::Result<()> {
        self.thread_hdl.join()
    }
}

async fn run_exporter(
    config: CircularExportConfig,
    _validator_identity: String,
    batch_receiver: crossbeam_channel::Receiver<ExportItem>,
    metrics: Arc<CircularExportMetrics>,
) {
    let api_key: MetadataValue<Ascii> = match config.api_key.parse() {
        Ok(value) => value,
        Err(err) => {
            error!("circular exporter: invalid API key, exporter disabled: {err}");
            return;
        }
    };

    // Lazy channel: never blocks here and reconnects on demand. A dead Fast
    // endpoint surfaces as per-call errors, never as a stall.
    let channel = match build_channel(&config) {
        Ok(channel) => channel,
        Err(err) => {
            error!("circular exporter: invalid endpoint {}: {err}", config.url);
            return;
        }
    };
    let client = FastTxClient::new(channel);
    let in_flight = Arc::new(Semaphore::new(config.max_in_flight));

    // Bridge the blocking crossbeam queue into the async world. Ends when
    // every CircularExportSender is dropped (validator shutdown).
    let (bridge_sender, mut bridge_receiver) =
        mpsc::channel::<ExportItem>(BRIDGE_CHANNEL_CAPACITY);
    let queue_receiver = batch_receiver.clone();
    let bridge_task = tokio::task::spawn_blocking(move || {
        while let Ok(batch) = queue_receiver.recv() {
            if bridge_sender.blocking_send(batch).is_err() {
                break;
            }
        }
    });

    let reporter_metrics = metrics.clone();
    let reporter_in_flight = in_flight.clone();
    let queue_capacity = config.queue_capacity;
    let max_in_flight = config.max_in_flight;
    let reporter_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(METRICS_REPORT_INTERVAL);
        loop {
            interval.tick().await;
            let live = max_in_flight.saturating_sub(reporter_in_flight.available_permits());
            reporter_metrics.report(batch_receiver.len(), queue_capacity, live);
        }
    });

    info!(
        "circular exporter: submitting verified transactions to Fast at {} (max_in_flight={})",
        config.url, config.max_in_flight
    );

    let submit_ctx = SubmitContext {
        client,
        in_flight: in_flight.clone(),
        metrics: metrics.clone(),
        api_key,
        memo: config.memo.clone(),
        cashback_address: config.cashback_address.clone(),
        request_timeout: config.request_timeout,
    };

    while let Some(item) = bridge_receiver.recv().await {
        match item {
            // Legacy owned path: the bytes were already copied on the
            // sigverify thread.
            ExportItem::Owned(batch) => {
                for packet in batch.packets {
                    submit_ctx.submit(packet.transaction);
                }
            }
            // Shared path: the wire-byte copy and discard / vote filtering
            // happen here, off the sigverify hot path.
            ExportItem::Shared(shared) => {
                for packet_batch in shared.batches.iter() {
                    for packet in packet_batch.iter() {
                        if packet.meta().discard() {
                            continue;
                        }
                        if shared.filter_simple_votes
                            && packet
                                .meta()
                                .flags
                                .contains(solana_packet::PacketFlags::SIMPLE_VOTE_TX)
                        {
                            continue;
                        }
                        let Some(data) = packet.data(..) else {
                            continue;
                        };
                        submit_ctx.submit(data.to_vec());
                    }
                }
            }
        }
    }

    // Shutdown: sigverify is gone. Give in-flight submissions a short window
    // to finish, then abandon the rest.
    let _ = timeout(
        SHUTDOWN_DRAIN_TIMEOUT,
        in_flight.acquire_many(config.max_in_flight as u32),
    )
    .await;

    reporter_task.abort();
    let _ = bridge_task.await;
    info!("circular exporter: shut down");
}

/// Shared context for submitting one transaction, reused by both the owned and
/// the shared consume paths.
struct SubmitContext {
    client: FastTxClient<Channel>,
    in_flight: Arc<Semaphore>,
    metrics: Arc<CircularExportMetrics>,
    api_key: MetadataValue<Ascii>,
    memo: Option<String>,
    cashback_address: Option<String>,
    request_timeout: Duration,
}

impl SubmitContext {
    /// Fire one unary `SendTransaction`. Never awaits here: the backpressure
    /// valve is the in-flight semaphore, and a saturated exporter drops rather
    /// than queues.
    fn submit(&self, transaction: Vec<u8>) {
        self.metrics
            .received_transactions
            .fetch_add(1, Ordering::Relaxed);

        // Backpressure valve: if all in-flight slots are busy (Fast slow or
        // down), drop rather than queue. The validator is never held back.
        let Ok(permit) = self.in_flight.clone().try_acquire_owned() else {
            self.metrics.dropped_no_permit.fetch_add(1, Ordering::Relaxed);
            return;
        };

        let mut client = self.client.clone();
        let api_key = self.api_key.clone();
        let memo = self.memo.clone();
        let cashback_address = self.cashback_address.clone();
        let request_timeout = self.request_timeout;
        let metrics = self.metrics.clone();

        tokio::spawn(async move {
            let _permit = permit;
            let transaction_bytes = transaction.len() as u64;

            let mut request = Request::new(SendTransactionRequest {
                // Wire payload = bincode VersionedTransaction, forwarded
                // unchanged.
                transaction,
                memo,
                forward: Some(false),
                cashback_address,
            });
            request.metadata_mut().insert("x-api-key", api_key);

            let start = Instant::now();
            let outcome = timeout(request_timeout, client.send_transaction(request)).await;
            metrics
                .send_us
                .fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);

            match outcome {
                Ok(Ok(_response)) => {
                    metrics.sent_ok.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .sent_bytes
                        .fetch_add(transaction_bytes, Ordering::Relaxed);
                }
                Ok(Err(status)) => {
                    metrics.sent_err.fetch_add(1, Ordering::Relaxed);
                    debug!(
                        "circular exporter: SendTransaction failed [{}]: {}",
                        status.code(),
                        status.message()
                    );
                }
                Err(_elapsed) => {
                    metrics.dropped_timeout.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    }
}

fn build_channel(config: &CircularExportConfig) -> Result<Channel, Box<dyn std::error::Error>> {
    let mut endpoint = Endpoint::from_shared(config.url.clone())?
        .connect_timeout(config.connect_timeout)
        .tcp_nodelay(true)
        .user_agent(format!("circular-jito/{}", env!("CARGO_PKG_VERSION")))?;

    if config.url.starts_with("https://") || config.tls_ca_path.is_some() {
        let mut tls = ClientTlsConfig::new().with_native_roots();
        if let Some(ca_path) = &config.tls_ca_path {
            let pem = std::fs::read(ca_path)?;
            tls = tls.ca_certificate(Certificate::from_pem(pem));
        }
        endpoint = endpoint.tls_config(tls)?;
    }

    if config.url.starts_with("https://") {
        warn!("circular exporter: TLS endpoint configured");
    }

    // Never blocks and reconnects transparently on failure.
    Ok(endpoint.connect_lazy())
}
