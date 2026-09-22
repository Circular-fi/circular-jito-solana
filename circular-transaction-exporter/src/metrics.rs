use {
    solana_metrics::datapoint_info,
    std::{
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    },
};

/// Counters shared between the sigverify hook, the export queue and the Fast
/// gRPC sender tasks. Reported and reset periodically by the exporter thread.
#[derive(Default)]
pub struct CircularExportMetrics {
    /// Valid transactions seen by the sigverify hook.
    pub received_transactions: AtomicU64,
    /// Transactions accepted into the export queue.
    pub enqueued_transactions: AtomicU64,
    /// Batches dropped because the export queue was full or closed.
    pub dropped_batches: AtomicU64,
    /// Transactions dropped because the export queue was full or closed.
    pub dropped_transactions: AtomicU64,
    /// Transactions dropped because the in-flight limit was saturated (Fast
    /// slow or unreachable).
    pub dropped_no_permit: AtomicU64,
    /// Transactions dropped because their `SendTransaction` call timed out.
    pub dropped_timeout: AtomicU64,
    /// `SendTransaction` calls the Fast endpoint accepted (OK).
    pub sent_ok: AtomicU64,
    /// `SendTransaction` calls that returned a gRPC error status.
    pub sent_err: AtomicU64,
    /// Bytes of transaction payload accepted by Fast.
    pub sent_bytes: AtomicU64,
    /// Sum of per-call `SendTransaction` latencies (micros).
    pub send_us: AtomicU64,
    /// Time spent in the sigverify hook (copy + try_send).
    pub hook_us: AtomicU64,
    /// Bytes copied out of packet buffers by the sigverify hook.
    pub copy_bytes: AtomicU64,
    /// Transactions dropped because they were an exact byte-for-byte
    /// duplicate of one already sent within the dedup TTL window.
    pub dropped_duplicates: AtomicU64,
    /// Current number of distinct transactions tracked by the dedup table.
    /// A gauge, not a cumulative counter: reported as-is, never reset.
    pub dedup_table_size: AtomicU64,
    /// Health gauge: `1` once the exporter has an established gRPC channel,
    /// `0` while it is (re)connecting. Reported as-is, never reset. This is
    /// the operator-visible signal that the exporter is enabled but offline.
    pub connected: AtomicU64,
    /// Number of failed connection attempts since the last report. A counter,
    /// reset every report window. Non-zero means the endpoint is flapping or
    /// unreachable.
    pub reconnect_attempts: AtomicU64,
    /// Unix nanoseconds of the last `SendTransaction` accepted by Fast. A
    /// gauge, never reset; drives `secs_since_last_successful_send`. `0` means
    /// nothing has ever been sent successfully.
    pub last_send_unix_nanos: AtomicU64,
}

impl CircularExportMetrics {
    pub fn report(&self, queue_depth: usize, queue_capacity: usize, in_flight: usize) {
        datapoint_info!(
            "circular_transaction_exporter",
            (
                "received_transactions",
                self.received_transactions.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "enqueued_transactions",
                self.enqueued_transactions.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "dropped_batches",
                self.dropped_batches.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "dropped_transactions",
                self.dropped_transactions.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "dropped_no_permit",
                self.dropped_no_permit.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "dropped_timeout",
                self.dropped_timeout.swap(0, Ordering::Relaxed),
                i64
            ),
            ("sent_ok", self.sent_ok.swap(0, Ordering::Relaxed), i64),
            ("sent_err", self.sent_err.swap(0, Ordering::Relaxed), i64),
            (
                "sent_bytes",
                self.sent_bytes.swap(0, Ordering::Relaxed),
                i64
            ),
            ("send_us", self.send_us.swap(0, Ordering::Relaxed), i64),
            ("hook_us", self.hook_us.swap(0, Ordering::Relaxed), i64),
            (
                "copy_bytes",
                self.copy_bytes.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "dropped_duplicates",
                self.dropped_duplicates.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "dedup_table_size",
                self.dedup_table_size.load(Ordering::Relaxed),
                i64
            ),
            ("queue_depth", queue_depth as i64, i64),
            ("queue_capacity", queue_capacity as i64, i64),
            ("in_flight", in_flight as i64, i64),
            ("connected", self.connected.load(Ordering::Relaxed) as i64, i64),
            (
                "reconnect_attempts",
                self.reconnect_attempts.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "secs_since_last_successful_send",
                self.secs_since_last_successful_send(),
                i64
            ),
        );
    }

    /// Seconds elapsed since the last `SendTransaction` accepted by Fast, or
    /// `-1` if none has ever succeeded. A steadily growing value while
    /// transactions are being received is the durable signal that the
    /// exporter is offline (endpoint dead, disconnected, or stalled).
    fn secs_since_last_successful_send(&self) -> i64 {
        let last = self.last_send_unix_nanos.load(Ordering::Relaxed);
        if last == 0 {
            return -1;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos() as u64)
            .unwrap_or_default();
        (now.saturating_sub(last) / 1_000_000_000) as i64
    }
}
