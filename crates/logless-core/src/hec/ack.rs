//! HEC indexer acknowledgement, keyed to WAL durability.
//!
//! A HEC client with `useACK` posts a batch, gets an `ackId`, and polls
//! `/services/collector/ack` until that id reports `true`. The whole point of
//! the mechanism is that `true` means "you may now forget your copy". Answering
//! `true` on receipt — which is the tempting shortcut, since it needs no state
//! at all — turns the client's durability guarantee into a lie: a crash between
//! the ack and the `fdatasync` loses data the sender had already discarded.
//!
//! So an ack here flips only once the records it covers have been `fdatasync`ed
//! into the WAL:
//!
//! 1. The receiver [`AckTable::issue`]s an id when it hands a batch to the
//!    pipeline, recording the batch's sequence number.
//! 2. The record path [`AckTable::bind`]s that sequence to the WAL record count
//!    it must reach — it knows this only after the records are submitted, and
//!    submission order is the order the writer will append in.
//! 3. The WAL writer publishes its synced record count after each `fdatasync`.
//! 4. [`AckTable::query`] compares the two.
//!
//! Steps 1 and 2 are separate because the HTTP response has to carry the id
//! immediately, while the watermark is only known once the single-threaded
//! record path has drained that batch.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Outstanding acks kept per channel. Splunk's own default is a million; this
/// is far lower because each entry is only two integers but an unbounded map is
/// still a memory leak driven by a remote client. Entries are only ever evicted
/// once durable, which is what makes eviction safe (see [`AckTable::query`]).
pub const MAX_OUTSTANDING_PER_CHANNEL: usize = 8192;

#[derive(Debug, Default)]
struct Channel {
    /// Next id to hand out. Splunk's ids start at 0 and increase per channel.
    next_id: u64,
    /// Ids at or below this were durable when evicted, so they answer `true`
    /// without needing an entry. Without this floor, eviction would turn a
    /// satisfied ack into a permanent `false` and the client would poll for it
    /// forever.
    durable_floor: u64,
    /// ack id → batch sequence.
    pending: HashMap<u64, u64>,
}

impl Channel {
    /// Retires the leading run of durable ids and moves the floor over them.
    ///
    /// Strictly from the bottom, because the floor is one number: it can say
    /// "everything below N is durable" and nothing else. Retiring a durable id
    /// that sits *above* an undurable one would leave a hole the floor cannot
    /// describe — the hole would then answer `false` forever (the client polls
    /// for an ack that will never come) or, if the floor jumped the hole,
    /// `true` for data still in flight. One stuck batch therefore stalls
    /// eviction for its channel, which is what [`AckTable::has_capacity`] is
    /// for: the answer to a client whose data is not reaching disk is
    /// backpressure, not a quietly growing map.
    fn retire_durable_prefix(&mut self, watermarks: &mut HashMap<u64, u64>, synced: u64) {
        while self.durable_floor < self.next_id {
            match self.pending.get(&self.durable_floor) {
                Some(seq) if watermarks.get(seq).is_some_and(|w| synced >= *w) => {
                    let seq = *seq;
                    self.pending.remove(&self.durable_floor);
                    // Drop the watermark with the entry that referenced it.
                    // Sweeping for unreferenced watermarks instead would make
                    // every batch walk the whole outstanding set — O(n) per
                    // request against a map sized by the client's backlog.
                    watermarks.remove(&seq);
                }
                Some(_) => break,
                None => {}
            }
            self.durable_floor += 1;
        }
    }
}

#[derive(Debug, Default)]
struct Inner {
    channels: HashMap<String, Channel>,
    /// batch sequence → WAL record count that must be synced for it to count as
    /// durable. Absent until the record path binds it.
    watermarks: HashMap<u64, u64>,
}

/// Shared between the receiver threads, the record path and the ack endpoint.
#[derive(Debug)]
pub struct AckTable {
    inner: Mutex<Inner>,
    next_batch: AtomicU64,
    /// Published by the WAL writer after each successful `fdatasync`.
    synced_records: Arc<AtomicU64>,
}

impl AckTable {
    pub fn new(synced_records: Arc<AtomicU64>) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner::default()),
            next_batch: AtomicU64::new(1),
            synced_records,
        })
    }

    /// Sequence number for a batch about to be handed to the pipeline.
    pub fn next_batch_seq(&self) -> u64 {
        self.next_batch.fetch_add(1, Ordering::Relaxed)
    }

    /// Issues an ack id on `channel` for a batch that has been accepted.
    pub fn issue(&self, channel: &str, batch_seq: u64) -> u64 {
        let synced = self.synced_records.load(Ordering::Acquire);
        let mut inner = self.lock();
        // Split the borrow: eviction needs the watermark map while holding the
        // channel entry.
        let Inner { channels, watermarks } = &mut *inner;
        let entry = channels.entry(channel.to_string()).or_default();
        let id = entry.next_id;
        entry.next_id += 1;
        entry.pending.insert(id, batch_seq);
        entry.retire_durable_prefix(watermarks, synced);
        id
    }

    /// Whether `channel` can take another outstanding ack.
    ///
    /// Checked *before* a batch is admitted. A channel at the cap means the
    /// client has thousands of batches we have not managed to sync — the
    /// pipeline is behind, and `503 Server is busy` is a truthful answer that
    /// the client's own retry handles. Accepting anyway would either grow the
    /// map without bound or force an unsafe eviction.
    pub fn has_capacity(&self, channel: &str) -> bool {
        self.lock()
            .channels
            .get(channel)
            .is_none_or(|c| c.pending.len() < MAX_OUTSTANDING_PER_CHANNEL)
    }

    /// Binds a batch to the WAL record count that makes it durable. Called by
    /// the record path once the batch has been submitted, in submission order.
    pub fn bind(&self, batch_seq: u64, records_submitted_total: u64) {
        let mut inner = self.lock();
        inner.watermarks.insert(batch_seq, records_submitted_total);
    }

    /// Answers an ack poll. Unknown ids report `false`: an id we never issued
    /// means the client is asking about someone else's channel, and claiming
    /// durability for data we never saw is the one answer that loses data.
    ///
    /// A poll never mutates. Dropping an id because the client has now seen it
    /// `true` — which is what Splunk does — would make a re-poll of the same id
    /// answer `false`, and clients do re-poll: they ask about a window of ids,
    /// not one at a time. Retirement is left to [`AckTable::issue`], from the
    /// bottom, where it is safe.
    pub fn query(&self, channel: &str, ids: &[u64]) -> Vec<(u64, bool)> {
        let synced = self.synced_records.load(Ordering::Acquire);
        let inner = self.lock();
        let Some(entry) = inner.channels.get(channel) else {
            return ids.iter().map(|id| (*id, false)).collect();
        };
        ids.iter()
            .map(|id| {
                // Below the floor means retired, and only durable ids retire.
                if *id < entry.durable_floor {
                    return (*id, true);
                }
                let durable = entry
                    .pending
                    .get(id)
                    .and_then(|seq| inner.watermarks.get(seq))
                    .is_some_and(|watermark| synced >= *watermark);
                (*id, durable)
            })
            .collect()
    }

    pub fn outstanding(&self) -> usize {
        self.lock().channels.values().map(|c| c.pending.len()).sum()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A poisoned ack table is still better than refusing every ack: the
        // alternative is a client that never learns its data is safe.
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> (Arc<AckTable>, Arc<AtomicU64>) {
        let synced = Arc::new(AtomicU64::new(0));
        (AckTable::new(Arc::clone(&synced)), synced)
    }

    #[test]
    fn ack_is_false_until_the_wal_has_synced_it() {
        let (t, synced) = table();
        let seq = t.next_batch_seq();
        let id = t.issue("chan", seq);
        assert_eq!(t.query("chan", &[id]), vec![(id, false)], "not even bound yet");

        t.bind(seq, 100);
        assert_eq!(t.query("chan", &[id]), vec![(id, false)], "bound but not synced");

        synced.store(99, Ordering::Release);
        assert_eq!(t.query("chan", &[id]), vec![(id, false)], "one record short");

        synced.store(100, Ordering::Release);
        assert_eq!(t.query("chan", &[id]), vec![(id, true)]);
    }

    #[test]
    fn a_satisfied_ack_stays_true_when_polled_again() {
        // Clients ask about a window of ids repeatedly, so an answer that
        // decays to `false` makes them resend data already on disk.
        // Clients poll in batches and re-ask; a `true` that flips back to
        // `false` would make a client resend data it had already been told was
        // safe.
        let (t, synced) = table();
        let seq = t.next_batch_seq();
        let id = t.issue("chan", seq);
        t.bind(seq, 10);
        synced.store(10, Ordering::Release);
        assert_eq!(t.query("chan", &[id]), vec![(id, true)]);
        assert_eq!(t.query("chan", &[id]), vec![(id, true)]);
    }

    #[test]
    fn ids_are_per_channel_and_start_at_zero() {
        let (t, _) = table();
        assert_eq!(t.issue("a", t.next_batch_seq()), 0);
        assert_eq!(t.issue("a", t.next_batch_seq()), 1);
        assert_eq!(t.issue("b", t.next_batch_seq()), 0, "channels number independently");
    }

    #[test]
    fn unknown_channel_and_unknown_id_report_false() {
        let (t, _) = table();
        assert_eq!(t.query("nobody", &[0, 7]), vec![(0, false), (7, false)]);
        t.issue("chan", t.next_batch_seq());
        assert_eq!(t.query("chan", &[99]), vec![(99, false)]);
    }

    #[test]
    fn out_of_order_durability_is_not_assumed() {
        // Two batches, the later one bound to a lower watermark than the
        // earlier: each must be judged on its own watermark, not on its id.
        let (t, synced) = table();
        let (s1, s2) = (t.next_batch_seq(), t.next_batch_seq());
        let (id1, id2) = (t.issue("c", s1), t.issue("c", s2));
        t.bind(s1, 500);
        t.bind(s2, 200);
        synced.store(200, Ordering::Release);
        assert_eq!(t.query("c", &[id1, id2]), vec![(id1, false), (id2, true)]);
    }

    #[test]
    fn a_stuck_batch_stalls_retirement_and_closes_the_channel() {
        let (t, synced) = table();
        // One undurable batch first, then enough durable ones to force eviction.
        let stuck_seq = t.next_batch_seq();
        let stuck = t.issue("c", stuck_seq);
        t.bind(stuck_seq, u64::MAX);

        synced.store(1, Ordering::Release);
        let mut durable_ids = Vec::new();
        for _ in 0..MAX_OUTSTANDING_PER_CHANNEL + 10 {
            let seq = t.next_batch_seq();
            let id = t.issue("c", seq);
            t.bind(seq, 1);
            durable_ids.push(id);
        }

        assert_eq!(
            t.query("c", &[stuck]),
            vec![(stuck, false)],
            "an ack whose data is not on disk must never report durable"
        );
        // Every durable id still answers truthfully, retired or not.
        assert!(t.query("c", &durable_ids).iter().all(|(_, ok)| *ok));
        // Retirement cannot pass the stuck id, so the channel fills and the
        // receiver must answer `Server is busy` rather than grow this map.
        assert!(!t.has_capacity("c"), "a stalled channel must stop accepting");
        assert!(t.has_capacity("other"), "one bad channel must not close the rest");
    }

    #[test]
    fn durable_batches_retire_and_the_channel_stays_open() {
        let (t, synced) = table();
        for _ in 0..MAX_OUTSTANDING_PER_CHANNEL + 100 {
            let seq = t.next_batch_seq();
            let id = t.issue("c", seq);
            t.bind(seq, seq);
            synced.store(seq, Ordering::Release);
            assert_eq!(t.query("c", &[id]), vec![(id, true)]);
        }
        assert!(t.has_capacity("c"));
        assert!(
            t.outstanding() <= 2,
            "retirement should keep a healthy channel near empty, got {}",
            t.outstanding()
        );
    }

    #[test]
    fn binding_after_the_query_still_works() {
        // The record path binds asynchronously; a client that polls early must
        // simply be told "not yet" rather than getting a wrong answer.
        let (t, synced) = table();
        let seq = t.next_batch_seq();
        let id = t.issue("c", seq);
        synced.store(u64::MAX, Ordering::Release);
        assert_eq!(t.query("c", &[id]), vec![(id, false)], "unbound is never durable");
        t.bind(seq, 5);
        assert_eq!(t.query("c", &[id]), vec![(id, true)]);
    }
}
