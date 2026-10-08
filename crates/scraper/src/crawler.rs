//! One shared, deduplicated queue of pages for the whole run, worked by a pool of tasks. Fetching
//! goes through the limiter; storing doesn't hold a fetch turn, so the database never slows MGP down.

use std::collections::{HashSet, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::mgp::Mgp;
use crate::parser::parse_page;
use crate::store::{Direction, Settled, Store};

/// a deadlock between two pages' writes is retried this many times
const SAVE_TRIES: u32 = 3;

pub struct Crawler {
    mgp: Mgp,
    store: Store,
    settled: Settled,
    force: bool,
    year: i32,
    queue: Mutex<Queue>,
    changed: Notify,
    pub stats: Stats,
}

struct Queue {
    todo: VecDeque<(i32, Direction)>,
    seen: HashSet<i32>,
    working: usize,
}

#[derive(Default)]
pub struct Stats {
    pub fetched: AtomicUsize,
    pub changed: AtomicUsize,
    /// not due, so followed through the database without fetching
    pub skipped: AtomicUsize,
    /// not on MGP
    pub missing: AtomicUsize,
    pub errors: AtomicUsize,
}

impl Crawler {
    pub fn new(mgp: Mgp, store: Store, settled: Settled, force: bool, start: Vec<(i32, Direction)>) -> Self {
        let seen = start.iter().map(|(id, _)| *id).collect();
        Self {
            mgp,
            store,
            settled,
            force,
            year: chrono::Utc::now().format("%Y").to_string().parse().unwrap(),
            queue: Mutex::new(Queue { todo: start.into(), seen, working: 0 }),
            changed: Notify::new(),
            stats: Stats::default(),
        }
    }

    /// Work the queue with `workers` tasks until it's empty and nothing is in progress.
    pub async fn run(&self, workers: usize) {
        let report = async {
            let mut last = (Instant::now(), 0);
            loop {
                tokio::time::sleep(Duration::from_secs(15)).await;
                last = self.report(last);
            }
        };
        let started = Instant::now();
        tokio::select! {
            _ = futures::future::join_all((0..workers).map(|_| self.work())) => {}
            _ = report => {}
        }
        // the last line covers the whole run
        self.report((started, 0));
    }

    fn report(&self, (at, fetched_then): (Instant, usize)) -> (Instant, usize) {
        let s = &self.stats;
        let fetched = s.fetched.load(Relaxed);
        let per_minute = (fetched - fetched_then) as f64 / at.elapsed().as_secs_f64().max(0.001) * 60.0;
        let l = self.mgp.limiter().snapshot();
        let ms = |d: Option<Duration>| d.map_or("-".into(), |d| format!("{}ms", d.as_millis()));
        let (todo, seen) = {
            let q = self.queue.lock().unwrap();
            (q.todo.len(), q.seen.len())
        };
        info!(
            "{fetched} fetched ({} changed), {} not due, {} not on MGP, {} errors | {per_minute:.0}/min, {} in flight of {:.1}, median {} vs idle {}, {} retries, {} pauses | {todo} queued, {seen} seen",
            s.changed.load(Relaxed),
            s.skipped.load(Relaxed),
            s.missing.load(Relaxed),
            s.errors.load(Relaxed),
            l.in_flight,
            l.limit,
            ms(l.median),
            ms(l.baseline),
            self.mgp.retries.load(Relaxed),
            l.pauses,
        );
        (Instant::now(), fetched)
    }

    async fn work(&self) {
        loop {
            let notified = self.changed.notified();
            let job = {
                let mut q = self.queue.lock().unwrap();
                match q.todo.pop_front() {
                    Some(job) => {
                        q.working += 1;
                        Some(job)
                    }
                    None if q.working == 0 => return,
                    None => None,
                }
            };
            let Some((id, direction)) = job else {
                notified.await;
                continue;
            };

            let next = self.visit(id, direction).await.unwrap_or_else(|e| {
                self.stats.errors.fetch_add(1, Relaxed);
                warn!("{id}: {e:#}");
                vec![]
            });

            let mut q = self.queue.lock().unwrap();
            for (page, direction) in next {
                if q.seen.insert(page) {
                    q.todo.push_back((page, direction));
                }
            }
            q.working -= 1;
            drop(q);
            self.changed.notify_waiters();
        }
    }

    /// Crawl one page if it's due, and say where to go from it.
    async fn visit(&self, id: i32, direction: Direction) -> color_eyre::Result<Vec<(i32, Direction)>> {
        if !self.force && self.settled.missing.contains(&id) {
            self.stats.missing.fetch_add(1, Relaxed);
            return Ok(vec![]);
        }
        if !self.force && self.settled.not_due.contains(&id) {
            self.stats.skipped.fetch_add(1, Relaxed);
            return self.store.next_from(id, direction).await;
        }

        let html = self.mgp.page(id).await?;
        let Some(page) = parse_page(id, &html)? else {
            self.store.log_missing(id).await?;
            self.stats.missing.fetch_add(1, Relaxed);
            return Ok(vec![]);
        };
        let mut tries = 0;
        let changed = loop {
            match self.store.save(&page, self.year).await {
                Ok(changed) => break changed,
                Err(e) if tries + 1 < SAVE_TRIES && is_deadlock(&e) => tries += 1,
                Err(e) => return Err(e),
            }
        };
        self.stats.fetched.fetch_add(1, Relaxed);
        if changed == Some(true) {
            self.stats.changed.fetch_add(1, Relaxed);
        }
        self.store.next_from(id, direction).await
    }
}

fn is_deadlock(e: &color_eyre::Report) -> bool {
    e.downcast_ref::<sqlx::Error>()
        .and_then(|e| e.as_database_error())
        .and_then(|e| e.code())
        .is_some_and(|code| code == "40P01")
}
