use {
    crate::{
        config::{self, CircularExportConfig},
        dedup::ExportDedup,
        event::{BatchOrigin, ExportItem, TransactionSource, unix_nanos_now},
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
        transport::{Channel, ClientTlsConfig, Endpoint},
    },
};

/// Capacity, in packet batches, of the bridge between the blocking queue
/// reader and the async submission loop. Small on purpose: the large buffer
/// is the crossbeam queue; this one only crosses the sync/async boundary.
const BRIDGE_CHANNEL_CAPACITY: usize = 16;
const METRICS_REPORT_INTERVAL: Duration = Duration::from_secs(2);
/// How long to wait for in-flight submissions to drain on shutdown.
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
/// First delay between failed connection attempts; doubles on each failure up
/// to [`RECONNECT_BACKOFF_MAX`].
const RECONNECT_BACKOFF_INITIAL: Duration = Duration::from_millis(500);
/// Cap on the delay between failed connection attempts.
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Granularity at which the reconnect backoff checks for shutdown, so a
/// dropped sender is noticed promptly even mid-backoff.
const RECONNECT_SHUTDOWN_POLL: Duration = Duration::from_millis(100);

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
    /// wire into the sigverify workers. Deduplication uses the fixed
    /// [`config::DEDUP_TTL`] window; it is a correctness safeguard, not an
    /// operator-tunable setting.
    pub fn spawn(
        config: CircularExportConfig,
        validator_identity: String,
    ) -> (CircularExportSender, Self) {
        Self::spawn_inner(config, validator_identity, config::DEDUP_TTL)
    }

    /// Like [`spawn`](Self::spawn) but lets the caller pick the dedup TTL.
    /// Only exposed for tests/benches, which need to disable deduplication
    /// (`Duration::ZERO`) to assert on exact send counts, or exercise it with
    /// a short TTL. Production always goes through [`spawn`](Self::spawn).
    #[cfg(feature = "dev-context-only-utils")]
    pub fn spawn_with_dedup_ttl(
        config: CircularExportConfig,
        validator_identity: String,
        dedup_ttl: Duration,
    ) -> (CircularExportSender, Self) {
        Self::spawn_inner(config, validator_identity, dedup_ttl)
    }

    fn spawn_inner(
        config: CircularExportConfig,
        validator_identity: String,
        dedup_ttl: Duration,
    ) -> (CircularExportSender, Self) {
        let (batch_sender, batch_receiver) =
            crossbeam_channel::bounded::<ExportItem>(config.queue_capacity);
        let metrics = Arc::new(CircularExportMetrics::default());
        let sender = CircularExportSender::new(
            batch_sender,
            metrics.clone(),
            config.forward_tpu,
            config.forward_preconf,
            config.forward_jito_bundle,
        );

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
                    dedup_ttl,
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
    dedup_ttl: Duration,
) {
    let api_key: MetadataValue<Ascii> = match config.api_key.parse() {
        Ok(value) => value,
        Err(err) => {
            // A malformed key never repairs itself: retrying is pointless, so
            // this is the one path that stays a permanent, clearly-labelled
            // exit (distinct from a network outage, which is retried below).
            error!("circular exporter: invalid API key, exporter disabled: {err}");
            return;
        }
    };

    let in_flight = Arc::new(Semaphore::new(config.max_in_flight));

    // Start the metrics reporter *before* connecting, so the `connected=0`
    // gauge and `reconnect_attempts` counter stay visible to operators
    // throughout an outage — including one that spans the whole validator
    // boot, which is exactly when the exporter used to go silently offline.
    let reporter_metrics = metrics.clone();
    let reporter_in_flight = in_flight.clone();
    let reporter_receiver = batch_receiver.clone();
    let queue_capacity = config.queue_capacity;
    let max_in_flight = config.max_in_flight;
    let reporter_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(METRICS_REPORT_INTERVAL);
        loop {
            interval.tick().await;
            let live = max_in_flight.saturating_sub(reporter_in_flight.available_permits());
            reporter_metrics.report(reporter_receiver.len(), queue_capacity, live);
        }
    });

    // Eager channel: handshake at boot so the first exported tx does not pay
    // TCP/H2 setup (this is what keeps the send rate smooth, no initial
    // burst). Unlike a single attempt, a transient failure at boot (DNS,
    // route, gRPC handshake) must NOT permanently disable the exporter: retry
    // with capped exponential backoff until connected, or until the validator
    // shuts down. Tonic still reconnects transparently on later per-call
    // failures once the channel exists.
    let Some(channel) = connect_with_backoff(&config, &metrics, &batch_receiver).await else {
        info!("circular exporter: shut down before initial connection");
        reporter_task.abort();
        return;
    };
    let client = FastTxClient::new(channel);

    // Bridge the blocking crossbeam queue into the async world. Ends when
    // every CircularExportSender is dropped (validator shutdown).
    let (bridge_sender, mut bridge_receiver) = mpsc::channel::<ExportItem>(BRIDGE_CHANNEL_CAPACITY);
    let queue_receiver = batch_receiver;
    let bridge_task = tokio::task::spawn_blocking(move || {
        while let Ok(batch) = queue_receiver.recv() {
            if bridge_sender.blocking_send(batch).is_err() {
                break;
            }
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
        cashback_address: config.cashback_address.clone(),
        request_timeout: config.request_timeout,
    };

    // Exact byte-level dedup, single-threaded (this loop is the only
    // consumer), fixed TTL sliding window. See `dedup.rs` for the design.
    let mut dedup = ExportDedup::new(dedup_ttl);

    while let Some(item) = bridge_receiver.recv().await {
        match item {
            // Legacy owned path: the bytes were already copied on the
            // sigverify thread. Vote transactions never reach this point
            // (filtered out when the `VerifiedPacket`s were built).
            ExportItem::Owned(batch) => {
                for packet in batch.packets {
                    if dedup.check_and_insert(&packet.transaction) {
                        metrics.dropped_duplicates.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    submit_ctx.submit(packet.transaction, packet.source);
                }
            }
            // Shared path: the wire-byte copy and the (unconditional) vote
            // filtering happen here, off the sigverify hot path. `origin`
            // selects how the source is resolved: BAM tags every packet in
            // the batch alike, native TPU resolves each packet individually
            // (forwarded or not).
            ExportItem::Shared(shared) => {
                let fixed_source = match shared.origin {
                    BatchOrigin::Bam { is_bundle: true } => Some(TransactionSource::BamBundle),
                    BatchOrigin::Bam { is_bundle: false } => Some(TransactionSource::Bam),
                    BatchOrigin::JitoBundle => Some(TransactionSource::JitoBundle),
                    BatchOrigin::Tpu => None,
                };
                for packet in shared.batches.iter() {
                    if packet.meta().discard() {
                        continue;
                    }
                    if packet
                        .meta()
                        .flags
                        .contains(solana_packet::PacketFlags::SIMPLE_VOTE_TX)
                    {
                        continue;
                    }
                    let Some(data) = packet.data(..) else {
                        continue;
                    };
                    if dedup.check_and_insert(data) {
                        metrics.dropped_duplicates.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    let source = fixed_source.unwrap_or_else(|| {
                        if packet.meta().forwarded() {
                            TransactionSource::Forwarded
                        } else {
                            TransactionSource::Tpu
                        }
                    });
                    submit_ctx.submit(data.to_vec(), source);
                }
            }
        }
        metrics
            .dedup_table_size
            .store(dedup.len() as u64, Ordering::Relaxed);
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
    cashback_address: Option<String>,
    request_timeout: Duration,
}

impl SubmitContext {
    /// Fire one unary `SendTransaction`. Never awaits here: the backpressure
    /// valve is the in-flight semaphore, and a saturated exporter drops rather
    /// than queues.
    ///
    /// The request's `memo` is always [`source.memo()`](TransactionSource::memo)
    /// — there is no operator-provided override.
    fn submit(&self, transaction: Vec<u8>, source: TransactionSource) {
        self.metrics
            .received_transactions
            .fetch_add(1, Ordering::Relaxed);

        // Backpressure valve: if all in-flight slots are busy (Fast slow or
        // down), drop rather than queue. The validator is never held back.
        let Ok(permit) = self.in_flight.clone().try_acquire_owned() else {
            self.metrics
                .dropped_no_permit
                .fetch_add(1, Ordering::Relaxed);
            return;
        };

        let mut client = self.client.clone();
        let api_key = self.api_key.clone();
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
                memo: Some(source.memo().to_string()),
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
                    // Health signal: timestamp of the last accepted submission,
                    // drives `secs_since_last_successful_send`.
                    metrics
                        .last_send_unix_nanos
                        .store(unix_nanos_now(), Ordering::Relaxed);
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

async fn build_channel(
    config: &CircularExportConfig,
) -> Result<Channel, Box<dyn std::error::Error>> {
    let mut endpoint = Endpoint::from_shared(config.url.clone())?
        .connect_timeout(config.connect_timeout)
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true)
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .user_agent(format!("circular-jito/{}", env!("CARGO_PKG_VERSION")))?;

    if config.url.starts_with("https://") {
        let tls = ClientTlsConfig::new().with_native_roots();
        endpoint = endpoint.tls_config(tls)?;
        warn!("circular exporter: TLS endpoint configured");
    }

    Ok(endpoint.connect().await?)
}

/// Establish the gRPC channel, retrying with capped exponential backoff and
/// jitter until it succeeds. Returns `None` if the validator shuts down (all
/// [`CircularExportSender`]s dropped, detected via `batch_receiver`) before a
/// connection is made, so the exporter thread can exit cleanly instead of
/// hanging.
///
/// This is the fix for the availability bug where a single failed connection
/// at boot permanently disabled the exporter with no recovery.
async fn connect_with_backoff(
    config: &CircularExportConfig,
    metrics: &Arc<CircularExportMetrics>,
    batch_receiver: &crossbeam_channel::Receiver<ExportItem>,
) -> Option<Channel> {
    let mut backoff = RECONNECT_BACKOFF_INITIAL;
    let mut attempt: u64 = 0;
    loop {
        match build_channel(config).await {
            Ok(channel) => {
                metrics.connected.store(1, Ordering::Relaxed);
                if attempt > 0 {
                    info!(
                        "circular exporter: connected to Fast at {} after {attempt} failed \
                         attempt(s)",
                        config.url
                    );
                }
                return Some(channel);
            }
            Err(err) => {
                metrics.connected.store(0, Ordering::Relaxed);
                metrics.reconnect_attempts.fetch_add(1, Ordering::Relaxed);
                attempt = attempt.saturating_add(1);
                warn!(
                    "circular exporter: connect {} failed (attempt {attempt}): {err}; retrying \
                     in ~{backoff:?}",
                    config.url
                );
                if wait_or_shutdown(batch_receiver, with_jitter(backoff)).await {
                    return None;
                }
                backoff = (backoff * 2).min(RECONNECT_BACKOFF_MAX);
            }
        }
    }
}

/// Sleep for `duration`, but return `true` early as soon as every sender has
/// been dropped (validator shutdown). Any batches queued during the outage are
/// drained and discarded — they cannot be sent, and the bounded queue would
/// drop them anyway; draining is also what lets us observe the disconnected
/// state of the channel promptly.
async fn wait_or_shutdown(
    batch_receiver: &crossbeam_channel::Receiver<ExportItem>,
    duration: Duration,
) -> bool {
    let deadline = Instant::now() + duration;
    loop {
        // Drain everything currently queued; a `Disconnected` result means all
        // senders are gone (shutdown), an `Empty` result means keep waiting.
        loop {
            match batch_receiver.try_recv() {
                Ok(_) => continue,
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => return true,
            }
        }
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        let slice = RECONNECT_SHUTDOWN_POLL.min(deadline - now);
        tokio::time::sleep(slice).await;
    }
}

/// Add up to +25% jitter to a backoff delay so many validators reconnecting at
/// once do not synchronize, without pulling in an RNG crate: derived from the
/// low bits of the wall clock.
fn with_jitter(base: Duration) -> Duration {
    let base_ms = base.as_millis() as u64;
    let span = base_ms / 4 + 1;
    let jitter = unix_nanos_now() % span;
    base + Duration::from_millis(jitter)
}
