//! How far parallel chunk claims may run ahead of ordered delivery.
//!
//! With [`OrderingMode::Stable`](crate::OrderingMode::Stable), a batch from chunk `k` reaches
//! the caller only after every chunk before `k` has been delivered. Batches from chunks that
//! finish early wait in the collector's reorder buffer, so without a limit a slow chunk lets
//! the other workers fill memory with everything after it. A worker or reader may therefore
//! claim chunk `k` only while `k < delivered + window`, which caps the reorder buffer at about
//! `window` chunks whatever the order the work finishes in. A chunk is one batch on the
//! memory-mapped pipeline, and one read extent (4 MB of pages, or one batch if that is more) on
//! the buffered ones, so there the bound in batches is the window times the batches per extent.
//!
//! Two properties keep this free of deadlock. The wait happens at the claim, before the
//! claimer holds anything the collector needs. And chunks are claimed in increasing order, so
//! whenever a claimer waits, the next chunk to deliver has already been claimed by a thread
//! that is not waiting on the gate. The wait is timed and rechecks the stop flags, so a scan
//! that ends early never leaves a claimer parked.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex, PoisonError};
use std::time::Duration;

/// How often a waiting claimer rechecks the stop flags. A stop also wakes waiters at once
/// through [`ChunkGate::close`]; this only bounds the delay if a stop is signalled through a
/// flag alone.
const RECHECK: Duration = Duration::from_millis(10);

#[derive(Debug)]
struct State {
    next_claim: usize,
    delivered: usize,
    closed: bool,
}

/// Hands out chunk indices in increasing order, at most `window` past the next chunk the
/// collector has yet to deliver. With no window, claims never wait.
#[derive(Debug)]
pub(super) struct ChunkGate {
    state: Mutex<State>,
    wake: Condvar,
    chunk_count: usize,
    window: Option<usize>,
}

impl ChunkGate {
    /// A gate over `chunk_count` chunks. `window` of `None` means unordered delivery: there is
    /// no reorder buffer to bound, so claims proceed freely. A window is at least 1.
    pub(super) fn new(chunk_count: usize, window: Option<usize>) -> Self {
        Self {
            state: Mutex::new(State {
                next_claim: 0,
                delivered: 0,
                closed: false,
            }),
            wake: Condvar::new(),
            chunk_count,
            window: window.map(|w| w.max(1)),
        }
    }

    /// The next chunk to work on, or `None` when every chunk is claimed, the gate is closed, or
    /// any of `stops` is set. Waits while the next chunk is beyond the window.
    pub(super) fn claim(&self, stops: &[&AtomicBool]) -> Option<usize> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if state.closed || stops.iter().any(|s| s.load(Ordering::Relaxed)) {
                return None;
            }
            if state.next_claim >= self.chunk_count {
                return None;
            }
            let allowed = self
                .window
                .is_none_or(|w| state.next_claim < state.delivered.saturating_add(w));
            if allowed {
                let idx = state.next_claim;
                state.next_claim += 1;
                return Some(idx);
            }
            state = self
                .wake
                .wait_timeout(state, RECHECK)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// Record that every chunk below `delivered` has been handed to the caller.
    pub(super) fn advance(&self, delivered: usize) {
        if self.window.is_none() {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if delivered > state.delivered {
            state.delivered = delivered;
            drop(state);
            self.wake.notify_all();
        }
    }

    /// End all claiming; waiters return `None` at once.
    pub(super) fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.closed = true;
        drop(state);
        self.wake.notify_all();
    }
}

/// Closes the gate, and sets `stop`, if the thread holding it unwinds.
///
/// A panic skips the normal exits that stop a scan. In the collector or in any worker, reader
/// or parse stage, it would leave other claimers waiting for a delivery that never comes, and
/// `std::thread::scope` waits for them before the panic can reach the caller. Each of those
/// threads holds one of these for its whole run.
pub(super) struct CloseOnUnwind<'a> {
    gate: &'a ChunkGate,
    stop: Option<&'a AtomicBool>,
}

impl<'a> CloseOnUnwind<'a> {
    pub(super) const fn new(gate: &'a ChunkGate, stop: Option<&'a AtomicBool>) -> Self {
        Self { gate, stop }
    }
}

impl Drop for CloseOnUnwind<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Some(stop) = self.stop {
                stop.store(true, Ordering::Relaxed);
            }
            self.gate.close();
        }
    }
}

/// The claim window for an ordered scan with `workers` decode threads: room for every worker
/// to hold a chunk, plus as many again finished and waiting, so a worker rarely idles while one
/// chunk lags behind.
pub(super) const fn ordered_window(workers: usize) -> usize {
    let w = workers.saturating_mul(2);
    if w < 2 { 2 } else { w }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn claims_are_handed_out_in_order_and_end_with_the_chunks() {
        let gate = ChunkGate::new(3, None);
        let no_stop = AtomicBool::new(false);
        assert_eq!(gate.claim(&[&no_stop]), Some(0));
        assert_eq!(gate.claim(&[&no_stop]), Some(1));
        assert_eq!(gate.claim(&[&no_stop]), Some(2));
        assert_eq!(gate.claim(&[&no_stop]), None);
    }

    #[test]
    fn a_claim_beyond_the_window_waits_for_delivery() {
        let gate = Arc::new(ChunkGate::new(10, Some(2)));
        let no_stop = AtomicBool::new(false);
        assert_eq!(gate.claim(&[&no_stop]), Some(0));
        assert_eq!(gate.claim(&[&no_stop]), Some(1));
        let claimed = Arc::new(AtomicUsize::new(usize::MAX));
        let waiter = {
            let gate = Arc::clone(&gate);
            let claimed = Arc::clone(&claimed);
            std::thread::spawn(move || {
                let no_stop = AtomicBool::new(false);
                claimed.store(gate.claim(&[&no_stop]).unwrap(), Ordering::SeqCst);
            })
        };
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(
            claimed.load(Ordering::SeqCst),
            usize::MAX,
            "claimed past the window"
        );
        gate.advance(1);
        waiter.join().unwrap();
        assert_eq!(claimed.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn a_stop_flag_or_close_releases_a_waiting_claimer() {
        for by_close in [false, true] {
            let gate = Arc::new(ChunkGate::new(10, Some(1)));
            let stop = Arc::new(AtomicBool::new(false));
            assert_eq!(gate.claim(&[&stop]), Some(0));
            let waiter = {
                let gate = Arc::clone(&gate);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || gate.claim(&[&stop]))
            };
            std::thread::sleep(Duration::from_millis(30));
            if by_close {
                gate.close();
            } else {
                stop.store(true, Ordering::SeqCst);
            }
            assert_eq!(waiter.join().unwrap(), None);
        }
    }

    #[test]
    fn many_claimers_take_each_chunk_exactly_once() {
        let gate = Arc::new(ChunkGate::new(1000, Some(4)));
        let taken: Arc<Mutex<Vec<usize>>> = Arc::default();
        let delivered = Arc::new(AtomicUsize::new(0));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let gate = Arc::clone(&gate);
                let taken = Arc::clone(&taken);
                std::thread::spawn(move || {
                    let no_stop = AtomicBool::new(false);
                    while let Some(i) = gate.claim(&[&no_stop]) {
                        taken.lock().unwrap().push(i);
                    }
                })
            })
            .collect();
        // Deliver in order as chunks appear, as the collector does.
        while delivered.load(Ordering::SeqCst) < 1000 {
            let mut t = taken.lock().unwrap().clone();
            t.sort_unstable();
            let next = t.iter().enumerate().take_while(|(i, c)| i == *c).count();
            delivered.store(next, Ordering::SeqCst);
            gate.advance(next);
            std::thread::yield_now();
        }
        for h in handles {
            h.join().unwrap();
        }
        let mut t = taken.lock().unwrap().clone();
        t.sort_unstable();
        assert_eq!(t, (0..1000).collect::<Vec<_>>());
    }
}
