//! Fetching pages from MGP, every request going through the limiter.

use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::time::Duration;

use color_eyre::eyre::eyre;
use reqwest::{Client, StatusCode};
use tokio::time::Instant;

use crate::limiter::{Limiter, Outcome};

/// tries per page before giving up on it for this run
const TRIES: u32 = 4;
/// MGP's pages are small; anything slower than this is a stuck request
const TIMEOUT: Duration = Duration::from_secs(30);
/// how long every request leaves MGP alone after any request hits an error
const BACKOFF: Duration = Duration::from_secs(10);

pub struct Mgp {
    client: Client,
    limiter: Limiter,
    /// requests tried again after a failure
    pub retries: AtomicUsize,
}

impl Mgp {
    pub fn new(limiter: Limiter) -> color_eyre::Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("combi-crawler/", env!("CARGO_PKG_VERSION"), " (+https://combi.me-9df.workers.dev)"))
            .timeout(TIMEOUT)
            .pool_idle_timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self { client, limiter, retries: AtomicUsize::new(0) })
    }

    pub fn limiter(&self) -> &Limiter {
        &self.limiter
    }

    /// The page's HTML. Errors only once every try failed; the limiter has backed off each time.
    pub async fn page(&self, id: i32) -> color_eyre::Result<String> {
        let url = format!("https://www.mathgenealogy.org/id.php?id={id}");
        let mut last = None;
        for attempt in 0..TRIES {
            if attempt > 0 {
                self.retries.fetch_add(1, Relaxed);
            }
            let permit = self.limiter.acquire().await;
            let started = Instant::now();
            let result = self.client.get(&url).send().await;
            let pause = BACKOFF;
            match result {
                Ok(res) if res.status().is_success() => match res.text().await {
                    Ok(body) => {
                        permit.done(Outcome::Ok(started.elapsed()));
                        return Ok(body);
                    }
                    Err(e) => {
                        permit.done(Outcome::Overloaded(pause));
                        last = Some(eyre!("reading page {id}: {e}"));
                    }
                },
                Ok(res) => {
                    let status = res.status();
                    // a 429 or 503 may say how long to wait
                    let asked = res.headers().get("retry-after").and_then(|v| v.to_str().ok()?.parse::<u64>().ok()).map(Duration::from_secs);
                    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                        permit.done(Outcome::Overloaded(asked.unwrap_or(pause).max(pause)));
                    } else {
                        // a client error won't get better by asking again
                        permit.done(Outcome::Ok(started.elapsed()));
                        return Err(eyre!("page {id}: MGP answered {status}"));
                    }
                    last = Some(eyre!("page {id}: MGP answered {status}"));
                }
                Err(e) => {
                    permit.done(Outcome::Overloaded(pause));
                    last = Some(eyre!("fetching page {id}: {e}"));
                }
            }
            tracing::debug!("page {id} attempt {} failed: {:?}", attempt + 1, last);
            // on top of the shared pause, this page waits longer before each retry
            tokio::time::sleep(Duration::from_secs(2u64.pow(attempt))).await;
        }
        Err(last.unwrap())
    }
}
