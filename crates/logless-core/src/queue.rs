//! Bounded ingest queue with severity-ordered load shedding.
//!
//! The hard rule (`docs/architecture.md` §1): **never block the producing
//! application**. When the queue is full we sacrifice data, in this order —
//! debug first, then info, and only as a last resort warn-and-above, which is
//! allowed a short bounded wait before it too is dropped.
//!
//! This is a load-shedding system by design. The honesty requirement is the
//! accounting invariant, asserted in tests and by the chaos harness:
//!
//! ```text
//! received == enqueued + dropped_debug + dropped_info + dropped_critical
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};

use crate::model::{LevelClass, LogRecord};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Enqueued,
    /// Shed because the queue was full. Carries what we gave up, so the
    /// operator sees which fidelity was lost rather than a single opaque
    /// counter.
    Shed(LevelClass),
    /// The writer side is gone — shutdown, or a writer thread that died.
    Closed,
}

#[derive(Debug, Default)]
pub struct IngestStats {
    pub received: AtomicU64,
    pub enqueued: AtomicU64,
    pub dropped_debug: AtomicU64,
    pub dropped_info: AtomicU64,
    /// WARN and above. Any non-zero value here is an incident, not a metric.
    pub dropped_critical: AtomicU64,
    pub closed: AtomicU64,
}

impl IngestStats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            received: self.received.load(Ordering::Relaxed),
            enqueued: self.enqueued.load(Ordering::Relaxed),
            dropped_debug: self.dropped_debug.load(Ordering::Relaxed),
            dropped_info: self.dropped_info.load(Ordering::Relaxed),
            dropped_critical: self.dropped_critical.load(Ordering::Relaxed),
            closed: self.closed.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub received: u64,
    pub enqueued: u64,
    pub dropped_debug: u64,
    pub dropped_info: u64,
    pub dropped_critical: u64,
    pub closed: u64,
}

impl StatsSnapshot {
    pub fn dropped(&self) -> u64 {
        self.dropped_debug + self.dropped_info + self.dropped_critical + self.closed
    }

    /// The invariant. Every record is either stored or explicitly counted as
    /// lost; nothing vanishes silently.
    pub fn accounts_for_everything(&self) -> bool {
        self.received == self.enqueued + self.dropped()
    }
}

/// Producer handle. Cheap to clone — one per receiver/source.
#[derive(Clone)]
pub struct IngestQueue {
    tx: Sender<LogRecord>,
    stats: Arc<IngestStats>,
    critical_timeout: Duration,
}

pub struct IngestConsumer {
    rx: Receiver<LogRecord>,
}

pub fn channel(capacity: usize, critical_timeout: Duration) -> (IngestQueue, IngestConsumer) {
    let (tx, rx) = bounded(capacity);
    (
        IngestQueue {
            tx,
            stats: Arc::new(IngestStats::default()),
            critical_timeout,
        },
        IngestConsumer { rx },
    )
}

impl IngestQueue {
    pub fn stats(&self) -> Arc<IngestStats> {
        Arc::clone(&self.stats)
    }

    pub fn len(&self) -> usize {
        self.tx.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tx.is_empty()
    }

    /// Offer a record. Never blocks for longer than `critical_enqueue_timeout`,
    /// and only for WARN+.
    pub fn submit(&self, record: LogRecord) -> Admission {
        self.stats.received.fetch_add(1, Ordering::Relaxed);
        let class = record.level_class();

        match self.tx.try_send(record) {
            Ok(()) => {
                self.stats.enqueued.fetch_add(1, Ordering::Relaxed);
                Admission::Enqueued
            }
            Err(TrySendError::Full(record)) => {
                if class != LevelClass::WarnPlus {
                    self.shed(class);
                    return Admission::Shed(class);
                }
                // WARN+ earns a bounded wait — but only bounded. A stalled disk
                // must not become a stalled application.
                match self.tx.send_timeout(record, self.critical_timeout) {
                    Ok(()) => {
                        self.stats.enqueued.fetch_add(1, Ordering::Relaxed);
                        Admission::Enqueued
                    }
                    Err(crossbeam_channel::SendTimeoutError::Timeout(_)) => {
                        self.shed(class);
                        Admission::Shed(class)
                    }
                    Err(crossbeam_channel::SendTimeoutError::Disconnected(_)) => {
                        self.stats.closed.fetch_add(1, Ordering::Relaxed);
                        Admission::Closed
                    }
                }
            }
            Err(TrySendError::Disconnected(_)) => {
                self.stats.closed.fetch_add(1, Ordering::Relaxed);
                Admission::Closed
            }
        }
    }

    fn shed(&self, class: LevelClass) {
        let counter = match class {
            LevelClass::Debug => &self.stats.dropped_debug,
            LevelClass::Info => &self.stats.dropped_info,
            LevelClass::WarnPlus => &self.stats.dropped_critical,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

impl IngestConsumer {
    /// Take up to `max` records, waiting at most `timeout` for the first one.
    ///
    /// Batching is what makes group commit worthwhile: one frame and one
    /// `fdatasync` per batch rather than per record.
    pub fn next_batch(&self, max: usize, timeout: Duration) -> Vec<LogRecord> {
        let mut batch = Vec::new();
        match self.rx.recv_timeout(timeout) {
            Ok(first) => batch.push(first),
            Err(_) => return batch,
        }
        while batch.len() < max {
            match self.rx.try_recv() {
                Ok(r) => batch.push(r),
                Err(_) => break,
            }
        }
        batch
    }

    /// Like [`IngestConsumer::next_batch`], but keeps waiting up to `linger`
    /// after the first record arrives.
    ///
    /// Without it the WAL writes a frame per wake-up: the stdin path measured
    /// 1.26 records per frame at 62 bytes each, so the 8-byte length and CRC
    /// header cost more than the data in it. A few milliseconds of linger turns
    /// that into whole batches, and bounds the added latency at `linger` —
    /// which is well inside the fsync interval the crash window is already set
    /// by, so it costs nothing that was not already being waited for.
    pub fn next_batch_lingering(
        &self,
        max: usize,
        timeout: Duration,
        linger: Duration,
    ) -> Vec<LogRecord> {
        let mut batch = Vec::new();
        match self.rx.recv_timeout(timeout) {
            Ok(first) => batch.push(first),
            Err(_) => return batch,
        }
        let deadline = std::time::Instant::now() + linger;
        while batch.len() < max {
            // Drain what is already there before waiting again.
            match self.rx.try_recv() {
                Ok(r) => {
                    batch.push(r);
                    continue;
                }
                Err(crossbeam_channel::TryRecvError::Disconnected) => break,
                Err(crossbeam_channel::TryRecvError::Empty) => {}
            }
            let Some(left) = deadline.checked_duration_since(std::time::Instant::now()) else {
                break;
            };
            match self.rx.recv_timeout(left) {
                Ok(r) => batch.push(r),
                Err(_) => break,
            }
        }
        batch
    }

    /// Drain everything still queued. Used on shutdown so a clean stop does not
    /// lose what was already accepted.
    pub fn drain(&self) -> Vec<LogRecord> {
        self.rx.try_iter().collect()
    }

    pub fn len(&self) -> usize {
        self.rx.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Severity;

    fn rec(sev: Severity) -> LogRecord {
        LogRecord::new(0, sev, "x")
    }

    #[test]
    fn sheds_debug_before_info_and_never_blocks() {
        let (q, _consumer) = channel(2, Duration::from_millis(1));
        assert_eq!(q.submit(rec(Severity::INFO)), Admission::Enqueued);
        assert_eq!(q.submit(rec(Severity::INFO)), Admission::Enqueued);

        // Full now.
        assert_eq!(
            q.submit(rec(Severity::DEBUG)),
            Admission::Shed(LevelClass::Debug)
        );
        assert_eq!(
            q.submit(rec(Severity::INFO)),
            Admission::Shed(LevelClass::Info)
        );

        let s = q.stats().snapshot();
        assert_eq!(s.dropped_debug, 1);
        assert_eq!(s.dropped_info, 1);
        assert_eq!(s.dropped_critical, 0);
        assert!(s.accounts_for_everything());
    }

    #[test]
    fn warn_plus_waits_but_only_briefly() {
        let (q, _consumer) = channel(1, Duration::from_millis(20));
        q.submit(rec(Severity::INFO));

        let start = std::time::Instant::now();
        let outcome = q.submit(rec(Severity::ERROR));
        let waited = start.elapsed();

        assert_eq!(outcome, Admission::Shed(LevelClass::WarnPlus));
        assert!(waited >= Duration::from_millis(20), "should have waited");
        assert!(
            waited < Duration::from_millis(500),
            "must stay bounded, waited {waited:?}"
        );
        assert_eq!(q.stats().snapshot().dropped_critical, 1);
    }

    #[test]
    fn warn_plus_gets_through_once_space_appears() {
        let (q, consumer) = channel(1, Duration::from_millis(500));
        q.submit(rec(Severity::INFO));

        let q2 = q.clone();
        let handle = std::thread::spawn(move || q2.submit(rec(Severity::ERROR)));
        // Free a slot while the ERROR is waiting.
        std::thread::sleep(Duration::from_millis(20));
        let _ = consumer.next_batch(1, Duration::from_millis(100));

        assert_eq!(handle.join().unwrap(), Admission::Enqueued);
        assert_eq!(q.stats().snapshot().dropped_critical, 0);
    }

    #[test]
    fn accounting_holds_under_concurrent_producers() {
        let (q, consumer) = channel(16, Duration::from_millis(1));
        let mut handles = Vec::new();
        for t in 0..8 {
            let q = q.clone();
            handles.push(std::thread::spawn(move || {
                for i in 0..500 {
                    let sev = match (t + i) % 3 {
                        0 => Severity::DEBUG,
                        1 => Severity::INFO,
                        _ => Severity::ERROR,
                    };
                    q.submit(rec(sev));
                }
            }));
        }
        // A slow consumer, so shedding actually happens.
        let drainer = std::thread::spawn(move || {
            let mut seen = 0;
            while seen < 4000 {
                let b = consumer.next_batch(64, Duration::from_millis(5));
                if b.is_empty() && seen > 0 {
                    break;
                }
                seen += b.len();
            }
            seen
        });

        for h in handles {
            h.join().unwrap();
        }
        let drained = drainer.join().unwrap();

        let s = q.stats().snapshot();
        assert_eq!(s.received, 4000);
        assert!(s.accounts_for_everything(), "invariant violated: {s:?}");
        assert!(drained as u64 <= s.enqueued);
    }

    #[test]
    fn reports_closed_when_the_writer_is_gone() {
        let (q, consumer) = channel(4, Duration::from_millis(1));
        drop(consumer);
        assert_eq!(q.submit(rec(Severity::ERROR)), Admission::Closed);
        let s = q.stats().snapshot();
        assert_eq!(s.closed, 1);
        assert!(s.accounts_for_everything());
    }

    #[test]
    fn batches_are_capped_and_ordered() {
        let (q, consumer) = channel(100, Duration::from_millis(1));
        for i in 0..10 {
            q.submit(LogRecord::new(i, Severity::INFO, format!("m{i}")));
        }
        let batch = consumer.next_batch(4, Duration::from_millis(50));
        assert_eq!(batch.len(), 4);
        assert_eq!(batch[0].body, "m0");
        assert_eq!(batch[3].body, "m3");
        assert_eq!(consumer.drain().len(), 6);
    }
}
