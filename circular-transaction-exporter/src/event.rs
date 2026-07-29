use {
    solana_perf::packet::PacketBatch,
    std::{
        sync::Arc,
        time::{SystemTime, UNIX_EPOCH},
    },
};

/// Ingress path of a verified transaction. Kept as internal metadata for
/// logging and metrics; the Fast contract carries no source field, so it is
/// not sent on the wire.
///
/// `Bam` / `BamVote` are set from the Jito BAM post-sigverify path.
/// `HarmonicScheduler` is reserved for Harmonic-only builds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransactionSource {
    Tpu,
    TpuVote,
    Forwarded,
    Bam,
    BamVote,
    HarmonicScheduler,
}

impl TransactionSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tpu => "tpu",
            Self::TpuVote => "tpu_vote",
            Self::Forwarded => "forwarded",
            Self::Bam => "bam",
            Self::BamVote => "bam_vote",
            Self::HarmonicScheduler => "harmonic_scheduler",
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
pub struct SharedVerifiedBatch {
    /// The exact `Arc<Vec<PacketBatch>>` also handed to banking stage. Held
    /// alive until the exporter thread has extracted the wire bytes.
    pub batches: Arc<Vec<PacketBatch>>,
    /// Whether these are TPU vote packets (for metrics/source only when
    /// [`Self::filter_simple_votes`] is false; the TPU hook decides
    /// include/exclude before enqueue).
    pub is_tpu_vote: bool,
    /// When true, the exporter skips packets flagged `SIMPLE_VOTE_TX`. Used
    /// by the BAM path where a batch can mix votes and non-votes.
    pub filter_simple_votes: bool,
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

/// Copy the wire bytes of every valid (non-discarded) packet out of `batches`.
/// This is the work the legacy owned path performs on the sigverify thread and
/// the shared path defers to the exporter thread. Returns `None` when no packet
/// survived verification.
pub fn build_owned_batch(
    batches: &[PacketBatch],
    is_tpu_vote: bool,
) -> Option<(VerifiedPacketBatch, u64)> {
    let mut packets = Vec::new();
    let mut copy_bytes = 0u64;

    for packet_batch in batches {
        for packet in packet_batch.iter() {
            if packet.meta().discard() {
                continue;
            }
            let Some(data) = packet.data(..) else {
                continue;
            };

            let source = if is_tpu_vote {
                TransactionSource::TpuVote
            } else if packet.meta().forwarded() {
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
