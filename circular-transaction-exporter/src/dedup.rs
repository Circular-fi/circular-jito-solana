//! Exact, TTL-based deduplication of transaction bytes sent to Circular Fast.
//!
//! Runs single-threaded on the `circExporter` thread (no locks, no atomics
//! needed). Duplicate detection is byte-exact — no bloom filter, no hash
//! truncation, zero false positives/negatives.
//!
//! Design notes (see plan for the full rationale):
//! - Bytes are stored once per distinct transaction, behind an `Rc<[u8]>`
//!   shared between the lookup map and the eviction queue. The eviction
//!   queue only clones the `Rc` (a cheap refcount bump), never the bytes.
//! - The duplicate-hit path (the hot path under load) never allocates:
//!   `Rc<[u8]>: Borrow<[u8]>` lets us call `contains_key` with a plain
//!   `&[u8]`.
//! - Eviction is FIFO from the front of the queue: since a hit never
//!   refreshes an entry's timestamp, insertion order is exactly expiration
//!   order, so eviction is O(1) amortized with no per-entry bookkeeping
//!   beyond the queue itself.

use std::{
    collections::{HashMap, VecDeque},
    rc::Rc,
    time::{Duration, Instant},
};

/// Exact deduplicator for transaction wire bytes, with a fixed TTL sliding
/// window and no entry cap: memory usage tracks the number of distinct
/// transactions seen within the TTL window.
pub struct ExportDedup {
    ttl: Duration,
    seen: HashMap<Rc<[u8]>, Instant, ahash::RandomState>,
    order: VecDeque<(Instant, Rc<[u8]>)>,
}

impl ExportDedup {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            seen: HashMap::with_hasher(ahash::RandomState::new()),
            order: VecDeque::new(),
        }
    }

    /// Returns `true` if `data` was already seen within the TTL window (the
    /// caller should drop it). Otherwise records it as seen and returns
    /// `false`. A `ttl` of zero disables deduplication entirely: every call
    /// evicts immediately and always returns `false`.
    pub fn check_and_insert(&mut self, data: &[u8]) -> bool {
        if self.ttl.is_zero() {
            return false;
        }

        let now = Instant::now();
        self.evict_expired(now);

        if self.seen.contains_key(data) {
            return true;
        }

        let key: Rc<[u8]> = Rc::from(data);
        self.order.push_back((now, key.clone()));
        self.seen.insert(key, now);
        false
    }

    /// Current number of distinct transactions tracked (for the
    /// `dedup_table_size` gauge).
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }

    fn evict_expired(&mut self, now: Instant) {
        while let Some((inserted_at, _)) = self.order.front() {
            if now.duration_since(*inserted_at) < self.ttl {
                break;
            }
            let (_, key) = self.order.pop_front().unwrap();
            self.seen.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_within_ttl_is_detected() {
        let mut dedup = ExportDedup::new(Duration::from_secs(10));
        assert!(!dedup.check_and_insert(b"tx-a"));
        assert!(dedup.check_and_insert(b"tx-a"));
        assert_eq!(dedup.len(), 1);
    }

    #[test]
    fn distinct_payloads_are_not_deduped() {
        let mut dedup = ExportDedup::new(Duration::from_secs(10));
        assert!(!dedup.check_and_insert(b"tx-a"));
        assert!(!dedup.check_and_insert(b"tx-b"));
        assert_eq!(dedup.len(), 2);
    }

    #[test]
    fn entry_expires_after_ttl() {
        let mut dedup = ExportDedup::new(Duration::from_millis(20));
        assert!(!dedup.check_and_insert(b"tx-a"));
        std::thread::sleep(Duration::from_millis(40));
        assert!(!dedup.check_and_insert(b"tx-a"));
        assert_eq!(dedup.len(), 1);
    }

    #[test]
    fn zero_ttl_disables_dedup() {
        let mut dedup = ExportDedup::new(Duration::ZERO);
        assert!(!dedup.check_and_insert(b"tx-a"));
        assert!(!dedup.check_and_insert(b"tx-a"));
        assert_eq!(dedup.len(), 0);
    }

    #[test]
    fn eviction_is_fifo_and_only_drops_expired_prefix() {
        let mut dedup = ExportDedup::new(Duration::from_millis(30));
        assert!(!dedup.check_and_insert(b"tx-a"));
        std::thread::sleep(Duration::from_millis(20));
        assert!(!dedup.check_and_insert(b"tx-b"));
        // tx-a is now ~20ms old, tx-b is fresh. Sleep past tx-a's expiry but
        // not tx-b's.
        std::thread::sleep(Duration::from_millis(15));
        // Triggers eviction of tx-a only.
        assert!(!dedup.check_and_insert(b"tx-c"));
        assert_eq!(dedup.len(), 2);
        assert!(!dedup.check_and_insert(b"tx-a"));
        assert!(dedup.check_and_insert(b"tx-b"));
        assert!(dedup.check_and_insert(b"tx-c"));
    }
}
