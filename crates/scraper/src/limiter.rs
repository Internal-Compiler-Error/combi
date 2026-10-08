//! How hard to push MGP. Two limits apply at once:
//!
//! - a hard cap on requests per second, spaced evenly, which the crawler never exceeds;
//! - an adaptive cap on requests in flight. It grows while MGP answers about as fast as it does
//!   when idle, eases off when answers slow down (requests are queueing on its side), and halves,
//!   with a pause, on errors and timeouts. So the crawler settles at whatever MGP serves without
//!   strain, and backs away when anyone else is loading it too.

use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::{Instant, sleep_until};

/// how many recent response times are kept
const WINDOW: usize = 200;
/// a low percentile of them is what MGP takes when nothing is queued ahead, without one lucky
/// fast answer setting the bar
const FAST_PERCENTILE: f64 = 0.1;
/// The idle baseline follows that percentile down at once but up only this fraction of the way
/// per answer (a few thousand answers to catch up), so a server slowing under load keeps reading
/// as congested instead of becoming the new normal; a lasting change in MGP still gets adopted.
const BASELINE_RISE: f64 = 0.0005;
/// a response slower than this multiple of the baseline means MGP is queueing requests
const SLOW: f64 = 2.0;
/// plus this much, so a fast server's noise doesn't read as congestion
const SLACK: Duration = Duration::from_millis(150);

pub struct Limiter {
    state: Mutex<State>,
    freed: Notify,
    min_gap: Duration,
    min: f64,
    max: f64,
}

struct State {
    limit: f64,
    in_flight: usize,
    next_slot: Instant,
    paused_until: Instant,
    recent: VecDeque<Duration>,
    baseline: Option<Duration>,
    pauses: usize,
}

pub enum Outcome {
    /// answered normally, after this long
    Ok(Duration),
    /// an error, a timeout or a 5xx/429: MGP is struggling, so back off for this long
    Overloaded(Duration),
}

/// A turn to make one request; hand it back with `done` so the limiter can learn from it.
pub struct Permit<'a> {
    limiter: &'a Limiter,
    returned: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct Snapshot {
    pub limit: f64,
    pub in_flight: usize,
    pub baseline: Option<Duration>,
    /// times MGP struggled (an error, timeout, 5xx or 429) and every request paused
    pub pauses: usize,
    pub median: Option<Duration>,
}

impl Limiter {
    pub fn new(max_per_second: f64, min_in_flight: usize, max_in_flight: usize) -> Self {
        let now = Instant::now();
        Self {
            state: Mutex::new(State {
                limit: min_in_flight as f64,
                in_flight: 0,
                next_slot: now,
                paused_until: now,
                recent: VecDeque::new(),
                baseline: None,
                pauses: 0,
            }),
            freed: Notify::new(),
            min_gap: Duration::from_secs_f64(1.0 / max_per_second),
            min: min_in_flight as f64,
            max: max_in_flight as f64,
        }
    }

    pub async fn acquire(&self) -> Permit<'_> {
        loop {
            let freed = self.freed.notified();
            let mut s = self.state.lock().unwrap();
            let now = Instant::now();
            if s.paused_until > now {
                let until = s.paused_until;
                drop(s);
                sleep_until(until).await;
                continue;
            }
            if (s.in_flight as f64) < s.limit.floor().max(1.0) {
                s.in_flight += 1;
                // even spacing: each request gets the next free slot on the per-second cap
                let slot = s.next_slot.max(now);
                s.next_slot = slot + self.min_gap;
                drop(s);
                sleep_until(slot).await;
                return Permit { limiter: self, returned: false };
            }
            drop(s);
            freed.await;
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let s = self.state.lock().unwrap();
        let mut sorted: Vec<_> = s.recent.iter().copied().collect();
        sorted.sort();
        Snapshot { limit: s.limit, in_flight: s.in_flight, baseline: s.baseline, median: percentile(&sorted, 0.5), pauses: s.pauses }
    }

    fn release(&self, outcome: Option<Outcome>) {
        let mut s = self.state.lock().unwrap();
        s.in_flight -= 1;
        match outcome {
            Some(Outcome::Ok(took)) => {
                s.recent.push_back(took);
                if s.recent.len() > WINDOW {
                    s.recent.pop_front();
                }
                let mut sorted: Vec<_> = s.recent.iter().copied().collect();
                sorted.sort();
                let fast = percentile(&sorted, FAST_PERCENTILE).unwrap_or(took);
                let baseline = match s.baseline {
                    Some(b) if fast > b => b + (fast - b).mul_f64(BASELINE_RISE),
                    _ => fast,
                };
                s.baseline = Some(baseline);
                if took <= baseline.mul_f64(SLOW) + SLACK {
                    // about one more request in flight per round of answers
                    s.limit = (s.limit + 1.0 / s.limit).min(self.max);
                } else {
                    s.limit = (s.limit * 0.9).max(self.min);
                }
            }
            Some(Outcome::Overloaded(pause)) => {
                s.pauses += 1;
                s.limit = (s.limit * 0.5).max(self.min);
                s.paused_until = s.paused_until.max(Instant::now() + pause);
            }
            None => {}
        }
        drop(s);
        self.freed.notify_waiters();
    }
}

fn percentile(sorted: &[Duration], p: f64) -> Option<Duration> {
    sorted.get(((sorted.len() as f64 - 1.0) * p).round() as usize).copied()
}

impl Permit<'_> {
    pub fn done(mut self, outcome: Outcome) {
        self.returned = true;
        self.limiter.release(Some(outcome));
    }
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        // dropped without an outcome (the request was abandoned): just free the slot
        if !self.returned {
            self.limiter.release(None);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn grows_while_fast_and_halves_on_trouble() {
        let l = Limiter::new(1000.0, 1, 8);
        for _ in 0..40 {
            l.acquire().await.done(Outcome::Ok(Duration::from_millis(300)));
        }
        let grown = l.snapshot().limit;
        assert!(grown > 5.0, "limit grew to {grown}");
        l.acquire().await.done(Outcome::Overloaded(Duration::from_secs(5)));
        assert!((l.snapshot().limit - grown / 2.0).abs() < 0.01);
    }

    #[tokio::test(start_paused = true)]
    async fn eases_off_when_answers_slow_down() {
        let l = Limiter::new(1000.0, 1, 8);
        for _ in 0..40 {
            l.acquire().await.done(Outcome::Ok(Duration::from_millis(300)));
        }
        let before = l.snapshot().limit;
        for _ in 0..5 {
            l.acquire().await.done(Outcome::Ok(Duration::from_secs(3)));
        }
        assert!(l.snapshot().limit < before * 0.7);
    }

    #[tokio::test(start_paused = true)]
    async fn a_server_slowing_under_load_stays_congested() {
        let l = Limiter::new(1000.0, 1, 8);
        for _ in 0..200 {
            l.acquire().await.done(Outcome::Ok(Duration::from_millis(150)));
        }
        // a whole window of slower answers: the baseline barely moves, so the limit stays down
        for _ in 0..200 {
            l.acquire().await.done(Outcome::Ok(Duration::from_millis(800)));
        }
        let s = l.snapshot();
        assert!(s.baseline.unwrap() < Duration::from_millis(250), "baseline rose to {:?}", s.baseline);
        assert_eq!(s.limit, 1.0);
    }

    #[tokio::test(start_paused = true)]
    async fn never_exceeds_the_rate_cap() {
        let l = Limiter::new(4.0, 8, 8);
        let start = Instant::now();
        for _ in 0..9 {
            l.acquire().await.done(Outcome::Ok(Duration::ZERO));
        }
        // nine evenly spaced requests at 4 a second take at least two seconds
        assert!(start.elapsed() >= Duration::from_secs(2));
    }
}
