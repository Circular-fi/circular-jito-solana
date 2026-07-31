use {
    crate::{
        event::{BatchOrigin, ExportItem, SharedVerifiedBatch, VerifiedPacketBatch, unix_nanos_now},
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
    /// Gate for [`export_verified`](Self::export_verified) (native TPU
    /// sigverify hook). When `false`, the hook is a no-op.
    forward_tpu: bool,
    /// Gate for [`export_bam_shared`](Self::export_bam_shared) (BAM
    /// post-sigverify hook). When `false`, the hook is a no-op.
    forward_preconf: bool,
    /// Gate for [`export_jito_bundle_shared`](Self::export_jito_bundle_shared)
    /// (classic, non-BAM Jito block-engine bundle hook). When `false`, the
    /// hook is a no-op.
    forward_jito_bundle: bool,
    /// Test/bench only: which strategy `export_verified` uses. Production is
    /// hardwired to the shared path (no field, no branch).
    #[cfg(feature = "dev-context-only-utils")]
    mode: HookMode,
}

impl CircularExportSender {
    pub(crate) fn new(
        sender: crossbeam_channel::Sender<ExportItem>,
        metrics: Arc<CircularExportMetrics>,
        forward_tpu: bool,
        forward_preconf: bool,
        forward_jito_bundle: bool,
    ) -> Self {
        Self {
            sender,
            metrics,
            forward_tpu,
            forward_preconf,
            forward_jito_bundle,
            #[cfg(feature = "dev-context-only-utils")]
            mode: HookMode::Arc,
        }
    }

    /// Build a sender backed by a plain bounded channel, without spawning an
    /// exporter thread. Lets tests and benchmarks inspect exactly what the
    /// sigverify hook hands to the exporter. All forwarding gates default to
    /// enabled.
    #[cfg(feature = "dev-context-only-utils")]
    pub fn new_for_tests(queue_capacity: usize) -> (Self, crossbeam_channel::Receiver<ExportItem>) {
        let (sender, receiver) = crossbeam_channel::bounded(queue_capacity);
        (Self::new(sender, Arc::default(), true, true, true), receiver)
    }

    /// Like [`new_for_tests`] but pins the [`HookMode`] driven by
    /// [`export_verified`]. Used by the 3-way sigverify benchmark.
    ///
    /// [`new_for_tests`]: CircularExportSender::new_for_tests
    /// [`export_verified`]: CircularExportSender::export_verified
    #[cfg(feature = "dev-context-only-utils")]
    pub fn new_for_tests_with_mode(
        queue_capacity: usize,
        mode: HookMode,
    ) -> (Self, crossbeam_channel::Receiver<ExportItem>) {
        let (sender, receiver) = crossbeam_channel::bounded(queue_capacity);
        let mut sender = Self::new(sender, Arc::default(), true, true, true);
        sender.mode = mode;
        (sender, receiver)
    }

    /// Test/bench only: access the shared metrics counters directly, without
    /// waiting for the periodic `datapoint_info!` report.
    #[cfg(feature = "dev-context-only-utils")]
    pub fn metrics(&self) -> Arc<CircularExportMetrics> {
        self.metrics.clone()
    }

    /// Entry point called by the native TPU sigverify hook
    /// (`SigVerifyWorkerPool::run_transaction_task`). Production always takes
    /// the shared `Arc` path (an atomic refcount bump; the wire-byte copy and
    /// discard filtering happen later, on the exporter thread). In test/bench
    /// builds the strategy is selected by [`HookMode`]. This is a no-op when
    /// `is_tpu_vote` is set (vote transactions are never exported) or when
    /// `forward_tpu` is disabled.
    #[inline]
    pub fn export_verified(&self, batches: &Arc<Vec<PacketBatch>>, is_tpu_vote: bool) {
        if is_tpu_vote || !self.forward_tpu {
            return;
        }
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
                HookMode::Arc => self.try_send_shared(batches.clone(), BatchOrigin::Tpu),
            }
        }
        #[cfg(not(feature = "dev-context-only-utils"))]
        {
            self.try_send_shared(batches.clone(), BatchOrigin::Tpu);
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
    /// only pays an `Arc` clone plus this enqueue; the wire-byte copy and the
    /// (unconditional) vote filtering happen later, on the exporter thread.
    /// `origin` selects how the exporter resolves the [`TransactionSource`] of
    /// every packet in this batch (native TPU vs. BAM, see [`BatchOrigin`]).
    ///
    /// [`TransactionSource`]: crate::event::TransactionSource
    ///
    /// P0.2 (skip work on saturation): when the queue is already full the batch
    /// is dropped immediately, so nothing is retained and no copy is ever
    /// scheduled for it.
    #[inline]
    pub fn try_send_shared(&self, batches: Arc<Vec<PacketBatch>>, origin: BatchOrigin) {
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
            origin,
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

    /// BAM post-sigverify hook (Arc path): share the verified `AtomicTxnBatch`
    /// with the exporter. Hot path cost is an `Arc` clone; vote filtering and
    /// wire-byte copy happen on the exporter thread. `is_bundle` should be
    /// the batch's `revert_on_error` flag: `true` for an atomic
    /// multi-transaction batch (tagged `BAM_BUNDLE`), `false` for a single
    /// transaction (tagged `BAM_TPU`). This is a no-op when `forward_preconf`
    /// is disabled.
    #[inline]
    pub fn export_bam_shared(&self, batches: Arc<Vec<PacketBatch>>, is_bundle: bool) {
        if !self.forward_preconf {
            return;
        }
        let start = Instant::now(); //for testing ONLY
        self.try_send_shared(batches, BatchOrigin::Bam { is_bundle });
        self.record_hook(start.elapsed().as_micros() as u64, 0);
    }

    /// Classic (non-BAM) Jito block-engine bundle hook: share a verified
    /// bundle's `PacketBatch` with the exporter. Unlike the native TPU/BAM
    /// hooks, the caller must clone the bundle's `PacketBatch` once before
    /// calling this (`BundleSigverifyStage` owns it directly, not behind an
    /// `Arc`) — see `bundle_sigverify_stage.rs` for the call site. This is a
    /// no-op when `forward_jito_bundle` is disabled.
    #[inline]
    pub fn export_jito_bundle_shared(&self, batches: Arc<Vec<PacketBatch>>) {
        if !self.forward_jito_bundle {
            return;
        }
        self.try_send_shared(batches, BatchOrigin::JitoBundle);
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
        let (sender, _rx) = CircularExportSender::new_for_tests(8);
        let batches = make_batches(7);
        sender.try_send_shared(batches, BatchOrigin::Tpu);
        assert_eq!(
            sender.metrics().enqueued_transactions.load(Ordering::Relaxed),
            7
        );
        assert_eq!(sender.metrics().dropped_batches.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn shared_path_full_queue_increments_dropped_transactions() {
        let (sender, _rx) = CircularExportSender::new_for_tests(1);
        let batches = make_batches(5);
        sender.try_send_shared(Arc::clone(&batches), BatchOrigin::Tpu);
        sender.try_send_shared(batches, BatchOrigin::Tpu);
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

    #[test]
    fn export_verified_noop_when_forward_tpu_disabled() {
        let (raw_sender, rx) = crossbeam_channel::bounded(8);
        let sender = CircularExportSender::new(raw_sender, Arc::default(), false, true, true);
        sender.export_verified(&make_batches(3), false);
        assert!(rx.try_recv().is_err());
        assert_eq!(
            sender.metrics().enqueued_transactions.load(Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn export_bam_shared_noop_when_forward_preconf_disabled() {
        let (raw_sender, rx) = crossbeam_channel::bounded(8);
        let sender = CircularExportSender::new(raw_sender, Arc::default(), true, false, true);
        sender.export_bam_shared(make_batches(3), false);
        assert!(rx.try_recv().is_err());
        assert_eq!(
            sender.metrics().enqueued_transactions.load(Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn export_jito_bundle_shared_noop_when_forward_jito_bundle_disabled() {
        let (raw_sender, rx) = crossbeam_channel::bounded(8);
        let sender = CircularExportSender::new(raw_sender, Arc::default(), true, true, false);
        sender.export_jito_bundle_shared(make_batches(3));
        assert!(rx.try_recv().is_err());
        assert_eq!(
            sender.metrics().enqueued_transactions.load(Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn export_jito_bundle_shared_increments_enqueued_transactions() {
        let (sender, _rx) = CircularExportSender::new_for_tests(8);
        sender.export_jito_bundle_shared(make_batches(4));
        assert_eq!(
            sender.metrics().enqueued_transactions.load(Ordering::Relaxed),
            4
        );
    }
}
