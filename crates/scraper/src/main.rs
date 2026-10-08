//! Crawls the Mathematics Genealogy Project into Postgres in bulk, as fast as MGP comfortably
//! serves. The website crawls on demand too (web/worker); this is for filling the database.

mod crawler;
mod limiter;
mod mgp;
mod parser;
mod store;

use clap::{Args, Parser, Subcommand};
use rand::seq::SliceRandom;
use sqlx::postgres::PgPoolOptions;

use crate::crawler::Crawler;
use crate::limiter::Limiter;
use crate::mgp::Mgp;
use crate::store::{Direction, Store};

#[derive(Parser)]
#[command(about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
    #[command(flatten)]
    load: Load,
}

#[derive(Subcommand)]
enum Command {
    /// Crawl people and everyone connected to them: students down, advisors up, without end
    Walk {
        /// MGP IDs to start from
        ids: Vec<i32>,
        /// also start from every ID in this inclusive range, e.g. 0..2000
        #[arg(long, value_parser = parse_range)]
        range: Option<(i32, i32)>,
        /// only follow students
        #[arg(long, conflicts_with = "up")]
        down: bool,
        /// only follow advisors
        #[arg(long)]
        up: bool,
    },
    /// Crawl every ID in a range, without following anyone; MGP's IDs run to about 350,000.
    /// IDs go in random order, so a sweep that stops early has covered the whole range evenly.
    Sweep {
        #[arg(long, default_value_t = 1)]
        from: i32,
        #[arg(long, default_value_t = 360_000)]
        to: i32,
        /// go through the IDs in order instead
        #[arg(long)]
        in_order: bool,
    },
}

/// How hard to push MGP. The crawler finds the most it can do below these on its own.
#[derive(Args)]
struct Load {
    /// never more than this many requests a second
    #[arg(long, global = true, default_value_t = 10.0)]
    max_rate: f64,
    /// never more than this many requests at once
    #[arg(long, global = true, default_value_t = 16)]
    max_in_flight: usize,
    /// crawl pages even when they aren't due
    #[arg(long, global = true)]
    force: bool,
}

fn parse_range(s: &str) -> Result<(i32, i32), String> {
    let (a, b) = s.split_once("..").ok_or("expected FROM..TO")?;
    let b = b.trim_start_matches('=');
    let (a, b): (i32, i32) = (a.parse().map_err(|e| format!("{e}"))?, b.parse().map_err(|e| format!("{e}"))?);
    if a > b { Err(format!("{a} is after {b}")) } else { Ok((a, b)) }
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let cli = Cli::parse();

    let start: Vec<(i32, Direction)> = match cli.command {
        Command::Walk { ids, range, down, up } => {
            let direction = if down {
                Direction::Down
            } else if up {
                Direction::Up
            } else {
                Direction::Both
            };
            let ranged = range.into_iter().flat_map(|(a, b)| a..=b);
            ids.into_iter().chain(ranged).map(|id| (id, direction)).collect()
        }
        Command::Sweep { from, to, in_order } => {
            let mut ids: Vec<_> = (from..=to).map(|id| (id, Direction::Neither)).collect();
            if !in_order {
                ids.shuffle(&mut rand::thread_rng());
            }
            ids
        }
    };
    if start.is_empty() {
        color_eyre::eyre::bail!("nothing to crawl: give IDs or --range");
    }

    let url = std::env::var("DATABASE_URL").or_else(|_| std::env::var("POSTGRES_URL"))?;
    // more workers than requests in flight, so storing pages never holds up fetching
    let workers = cli.load.max_in_flight * 2;
    let pool = PgPoolOptions::new().max_connections(workers as u32 + 2).connect(&url).await?;
    let store = Store::new(pool);
    let settled = store.settled().await?;
    tracing::info!(
        "{} pages to start from; {} aren't due and {} aren't on MGP, so those are only followed",
        start.len(),
        settled.not_due.len(),
        settled.missing.len()
    );

    let mgp = Mgp::new(Limiter::new(cli.load.max_rate, 1, cli.load.max_in_flight))?;
    Crawler::new(mgp, store, settled, cli.load.force, start).run(workers).await;
    Ok(())
}
