use {
    crate::{
        event::{ExportItem, SharedVerifiedBatch, VerifiedPacketBatch, unix_nanos_now},
        metrics::CircularExportMetrics,
    },
    crossbeam_channel::TrySendError,
    solana_perf::packet::PacketBatch,
    std::{
        sync::{Arc, atomic::Ordering},
        time::Instant,
    },
};

/// Strategy used by [`CircularExportSender::export_verified`]. Only exists in
/// test/bench builds: production always takes the `Arc` (shared) path. Lets the
/// 3-way benchmark drive the real sigverify hook through each strategy.
#[cfg(feature = "dev-context-only-utils")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HookMode {
    /// Sender present but the hook does nothing (isolates the branch cost).
    Disabled,
    /// Legacy path: copy the wire bytes on the sigverify thread.
    Copy,
    /// Production path: clone the shared `Arc`, defer the copy to the exporter.
    Arc,
}

/// Handle given to the sigverify workers. Sending never blocks and never
/// returns an error to the caller: a full queue or a dead exporter results
/// in a counted drop, the validator pipeline is unaffected.
#[derive(Clone)]
pub struct CircularExportSender {
    sender: crossbeam_channel::Sender<ExportItem>,
    metrics: Arc<CircularExportMetrics>,
    include_votes: bool,
    /// Test/bench only: which strategy `export_verified` uses. Production is
    /// hardwired to the shared path (no field, no branch).
    #[cfg(feature = "dev-context-only-utils")]
    mode: HookMode,
}

impl CircularExportSender {
    pub(crate) fn new(
        sender: crossbeam_channel::Sender<ExportItem>,
        metrics: Arc<CircularExportMetrics>,
        include_votes: bool,
    ) -> Self {
        Self {
            sender,
            metrics,
            include_votes,
            #[cfg(feature = "dev-context-only-utils")]
            mode: HookMode::Arc,
        }
    }

    /// Build a sender backed by a plain bounded channel, without spawning an
    /// exporter thread. Lets tests and benchmarks inspect exactly what the
    /// sigverify hook hands to the exporter.
    #[cfg(feature = "dev-context-only-utils")]
    pub fn new_for_tests(
        queue_capacity: usize,
        include_votes: bool,
    ) -> (Self, crossbeam_channel::Receiver<ExportItem>) {
        let (sender, receiver) = crossbeam_channel::bounded(queue_capacity);
        (Self::new(sender, Arc::default(), include_votes), receiver)
    }

    /// Like [`new_for_tests`] but pins the [`HookMode`] driven by
    /// [`export_verified`]. Used by the 3-way sigverify benchmark.
    ///
    /// [`new_for_tests`]: CircularExportSender::new_for_tests
    /// [`export_verified`]: CircularExportSender::export_verified
    #[cfg(feature = "dev-context-only-utils")]
    pub fn new_for_tests_with_mode(
        queue_capacity: usize,
        include_votes: bool,
        mode: HookMode,
    ) -> (Self, crossbeam_channel::Receiver<ExportItem>) {
        let (sender, receiver) = crossbeam_channel::bounded(queue_capacity);
        let mut sender = Self::new(sender, Arc::default(), include_votes);
        sender.mode = mode;
        (sender, receiver)
    }

    /// Whether TPU vote transactions should be exported.
    #[inline]
    pub fn include_votes(&self) -> bool {
        self.include_votes
    }

    /// Test/bench only: access the shared metrics counters directly, without
    /// waiting for the periodic `datapoint_info!` report.
    #[cfg(feature = "dev-context-only-utils")]
    pub fn metrics(&self) -> Arc<CircularExportMetrics> {
        self.metrics.clone()
    }

    /// Entry point called by the sigverify hook. Production always takes the
    /// shared `Arc` path (an atomic refcount bump; the wire-byte copy and
    /// discard filtering happen later, on the exporter thread). In test/bench
    /// builds the strategy is selected by [`HookMode`].
    #[inline]
    pub fn export_verified(&self, batches: &Arc<Vec<PacketBatch>>, is_tpu_vote: bool) {
        #[cfg(feature = "dev-context-only-utils")]
        {
            match self.mode {
                HookMode::Disabled => {}
                HookMode::Copy => {
                    if let Some((owned, _copy_bytes)) =
                        crate::event::build_owned_batch(batches, is_tpu_vote)
                    {
                        self.try_send(owned);
                    }
                }
                HookMode::Arc => self.try_send_shared(batches.clone(), is_tpu_vote),
            }
        }
        #[cfg(not(feature = "dev-context-only-utils"))]
        {
            self.try_send_shared(batches.clone(), is_tpu_vote);
        }
    }

    /// Legacy owned path: the caller has already copied the wire bytes into a
    /// [`VerifiedPacketBatch`] on the sigverify thread. Kept alongside
    /// [`try_send_shared`] for the copy-vs-share benchmark.
    ///
    /// [`try_send_shared`]: CircularExportSender::try_send_shared
    #[inline]
    pub fn try_send(&self, batch: VerifiedPacketBatch) {
        let transaction_count = batch.packets.len() as u64;
        match self.sender.try_send(ExportItem::Owned(batch)) {
            Ok(()) => {
                self.metrics
                    .enqueued_transactions
                    .fetch_add(transaction_count, Ordering::Relaxed);
            }
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.metrics.dropped_batches.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .dropped_transactions
                    .fetch_add(transaction_count, Ordering::Relaxed);
            }
        }
    }

    /// Shared path: hand the exporter an `Arc<Vec<PacketBatch>>`. The hot path
    /// only pays an `Arc` clone plus this enqueue; the wire-byte copy and
    /// discard filtering happen later, on the exporter thread.
    ///
    /// P0.2 (skip work on saturation): when the queue is already full the batch
    /// is dropped immediately, so nothing is retained and no copy is ever
    /// scheduled for it.
    #[inline]
    pub fn try_send_shared(&self, batches: Arc<Vec<PacketBatch>>, is_tpu_vote: bool) {
        self.try_send_shared_with_vote_filter(batches, is_tpu_vote, false);
    }

    /// Like [`try_send_shared`], but when `filter_simple_votes` is true the
    /// exporter thread skips `SIMPLE_VOTE_TX` packets (BAM mixed batches).
    #[inline]
    pub fn try_send_shared_with_vote_filter(
        &self,
        batches: Arc<Vec<PacketBatch>>,
        is_tpu_vote: bool,
        filter_simple_votes: bool,
    ) {
        // Packet count only (no wire copy) so Grafana enqueued/dropped match
        // the owned-path semantics on the production Arc path.
        let transaction_count: u64 = batches.iter().map(|b| b.len() as u64).sum();

        if self.sender.is_full() {
            self.metrics.dropped_batches.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .dropped_transactions
                .fetch_add(transaction_count, Ordering::Relaxed);
            return;
        }

        let item = ExportItem::Shared(SharedVerifiedBatch {
            batches,
            is_tpu_vote,
            filter_simple_votes,
            received_at_unix_nanos: unix_nanos_now(),
        });
        match self.sender.try_send(item) {
            Ok(()) => {
                self.metrics
                    .enqueued_transactions
                    .fetch_add(transaction_count, Ordering::Relaxed);
            }
            Err(_) => {
                self.metrics.dropped_batches.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .dropped_transactions
                    .fetch_add(transaction_count, Ordering::Relaxed);
            }
        }
    }

    /// Record the time spent in the sigverify hook and the bytes copied out
    /// of the packet buffers.
    #[inline]
    pub fn record_hook(&self, hook_us: u64, copy_bytes: u64) {
        self.metrics.hook_us.fetch_add(hook_us, Ordering::Relaxed);
        self.metrics
            .copy_bytes
            .fetch_add(copy_bytes, Ordering::Relaxed);
    }

    /// BAM post-sigverify hook (Arc path): share the verified batch with the
    /// exporter. Hot path cost is an `Arc` clone; vote filtering and wire-byte
    /// copy happen on the exporter thread.
    #[inline]
    pub fn export_bam_shared(&self, batches: Arc<Vec<PacketBatch>>) {
        let start = Instant::now();
        self.try_send_shared_with_vote_filter(
            batches,
            /* is_tpu_vote */ false,
            /* filter_simple_votes */ !self.include_votes,
        );
        self.record_hook(start.elapsed().as_micros() as u64, 0);
    }
}

#[cfg(all(test, feature = "dev-context-only-utils"))]
mod tests {
    use {
        super::*,
        solana_perf::packet::to_packet_batches,
        std::sync::atomic::Ordering,
    };

    fn make_batches(num_tx: usize) -> Arc<Vec<PacketBatch>> {
        let payloads: Vec<Vec<u8>> = (0..num_tx).map(|i| vec![i as u8; 64]).collect();
        Arc::new(to_packet_batches(&payloads, num_tx.max(1)))
    }

    #[test]
    fn shared_path_increments_enqueued_transactions() {
        let (sender, _rx) = CircularExportSender::new_for_tests(8, false);
        let batches = make_batches(7);
        sender.try_send_shared(batches, false);
        assert_eq!(
            sender.metrics().enqueued_transactions.load(Ordering::Relaxed),
            7
        );
        assert_eq!(sender.metrics().dropped_batches.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn shared_path_full_queue_increments_dropped_transactions() {
        let (sender, _rx) = CircularExportSender::new_for_tests(1, false);
        let batches = make_batches(5);
        sender.try_send_shared(Arc::clone(&batches), false);
        sender.try_send_shared(batches, false);
        assert_eq!(
            sender.metrics().enqueued_transactions.load(Ordering::Relaxed),
            5
        );
        assert_eq!(sender.metrics().dropped_batches.load(Ordering::Relaxed), 1);
        assert_eq!(
            sender.metrics().dropped_transactions.load(Ordering::Relaxed),
            5
        );
    }
}
