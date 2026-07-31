use {
    solana_perf::packet::PacketBatch,
    std::{
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    },
};

/// Ingress path of a verified transaction. Kept as internal metadata for
/// logging and metrics; also drives [`memo`](Self::memo), which *is* sent on
/// the wire as the request's `memo` field — there is no separate CLI-provided
/// memo override, the source is the only thing that determines it.
///
/// Vote transactions never reach this type: they are filtered out before a
/// [`VerifiedPacket`] is ever built (see [`build_owned_batch`] and the
/// `SIMPLE_VOTE_TX` filtering in the exporter service), so there is no
/// `TpuVote` / `BamVote` variant.
///
/// `Bam` / `BamBundle` are set from the Jito BAM post-sigverify path
/// (individual transaction vs. an atomic multi-transaction batch,
/// i.e. `revert_on_error == true`). `HarmonicScheduler` is reserved for
/// Harmonic-only builds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionSource {
    Tpu,
    Forwarded,
    Bam,
    /// BAM atomic multi-transaction batch (`revert_on_error == true`), the
    /// BAM equivalent of a Jito bundle.
    BamBundle,
    HarmonicScheduler,
}

impl TransactionSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tpu => "tpu",
            Self::Forwarded => "forwarded",
            Self::Bam => "bam",
            Self::BamBundle => "bam_bundle",
            Self::HarmonicScheduler => "harmonic_scheduler",
        }
    }

    /// Memo sent to Fast as the request's `memo` field, identifying which
    /// ingress channel a transaction came from. This is the only source of
    /// the memo value — there is no operator-provided override.
    pub const fn memo(self) -> &'static str {
        match self {
            Self::Tpu | Self::Forwarded | Self::HarmonicScheduler => "TPU",
            Self::Bam => "BAM_TPU",
            Self::BamBundle => "BAM_BUNDLE",
        }
    }
}

/// A single verified (non-discarded) transaction, copied out of the sigverify
/// packet buffer. `transaction` is the Solana wire payload, which is exactly
/// the bincode serialization of a `VersionedTransaction` — the byte string
/// Fast expects in its `transaction` field, forwarded unchanged.
pub struct VerifiedPacket {
    pub transaction: Vec<u8>,
    pub source: TransactionSource,
}

/// Batch of verified transactions handed from a sigverify worker to the
/// exporter. One timestamp per batch: all packets of a batch leave sigverify
/// at the same instant.
pub struct VerifiedPacketBatch {
    pub packets: Vec<VerifiedPacket>,
    pub received_at_unix_nanos: u64,
}

/// A batch shared with the exporter by reference. The sigverify hook only
/// clones the `Arc` (an atomic refcount bump, no data copy); the exporter
/// thread does the discard / vote filtering and the wire-byte copy off the
/// hot path. This is the low-overhead alternative to [`VerifiedPacketBatch`].
///
/// Currently only fed by the BAM path ([`export_bam_shared`]); every packet
/// is always vote-filtered (`SIMPLE_VOTE_TX`) unconditionally, there is no
/// opt-out.
///
/// [`export_bam_shared`]: crate::sender::CircularExportSender::export_bam_shared
pub struct SharedVerifiedBatch {
    /// The exact `Arc<Vec<PacketBatch>>` also handed to banking stage. Held
    /// alive until the exporter thread has extracted the wire bytes.
    pub batches: Arc<Vec<PacketBatch>>,
    /// Whether this `AtomicTxnBatch` is an atomic multi-transaction batch
    /// (`revert_on_error == true`, the BAM equivalent of a bundle) rather
    /// than a single transaction. Decides between [`TransactionSource::Bam`]
    /// and [`TransactionSource::BamBundle`].
    pub is_bundle: bool,
    pub received_at_unix_nanos: u64,
}

/// Queue element handed from the sigverify hook to the exporter thread. Two
/// variants coexist during the copy-vs-share migration:
/// - [`ExportItem::Owned`]: legacy path, bytes copied on the sigverify thread.
/// - [`ExportItem::Shared`]: new path, only an `Arc` clone on the hot path.
pub enum ExportItem {
    Owned(VerifiedPacketBatch),
    Shared(SharedVerifiedBatch),
}

/// Copy the wire bytes of every valid, non-vote packet out of `batches`. This
/// is the work the legacy owned path performs on the sigverify thread and the
/// shared path defers to the exporter thread. Vote transactions (batch-level
/// `is_tpu_vote` or per-packet `SIMPLE_VOTE_TX`) are never forwarded — there
/// is no opt-in to export them. Returns `None` when no packet survived
/// verification.
pub fn build_owned_batch(
    batches: &[PacketBatch],
    is_tpu_vote: bool,
) -> Option<(VerifiedPacketBatch, u64)> {
    if is_tpu_vote {
        return None;
    }

    let mut packets = Vec::new();
    let mut copy_bytes = 0u64;

    for packet_batch in batches {
        for packet in packet_batch.iter() {
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

            let source = if packet.meta().forwarded() {
                TransactionSource::Forwarded
            } else {
                TransactionSource::Tpu
            };

            copy_bytes += data.len() as u64;
            packets.push(VerifiedPacket {
                transaction: data.to_vec(),
                source,
            });
        }
    }

    (!packets.is_empty()).then(|| {
        (
            VerifiedPacketBatch {
                packets,
                received_at_unix_nanos: unix_nanos_now(),
            },
            copy_bytes,
        )
    })
}

pub fn unix_nanos_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos() as u64)
        .unwrap_or_default()
}
