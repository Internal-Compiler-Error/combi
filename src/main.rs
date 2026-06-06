#![allow(dead_code)]

mod mathematician;
mod parser;

use std::collections::{BTreeMap, VecDeque};
use clap::Parser;
use color_eyre::eyre::eyre;
use lazy_static::lazy_static;
use mathematician::Country;
use mathematician::Mathematician;
use mathematician::School;
use mathematician::SchoolLocation;
use rand_distr::Distribution;
use rand_distr::Uniform;
use reqwest::Client;
use scraper::Html;
use scraper::Selector;
use sqlx::{FromRow, PgPool};

use std::fmt::Debug;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use sqlx::types::chrono;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::warn;

use crate::mathematician::AdvisorRelation;
use crate::mathematician::AdvisorRelationRepo;
use crate::mathematician::CountryRepo;
use crate::mathematician::MathematicianRepo;
use crate::mathematician::SchoolLocationRepo;
use crate::mathematician::SchoolRepo;
use crate::parser::{Id, MGPage};

#[derive(Debug, Eq, PartialEq, Parser)]
struct Args {
    /// the number of concurrent requests to make
    #[clap(short, long, default_value = "256")]
    concurrency: usize,

    // #[clap(long, default_value = "48h")]
    // refresh_tolerance: Duration,
}

#[derive(Debug, FromRow, Eq, PartialEq, Clone)]
struct LastScraped{
    id: i64,
    date: chrono::DateTime<chrono::Local>,
    page_scraped: Id,
    result: String,
}

#[instrument(skip(repo))]
async fn insert_record(
    repo: &mut sqlx::PgConnection,
    record: MGPage,
) -> color_eyre::Result<()>
// The repo traits take `self` by value, so naming `repo` directly would move it on the
// first call. Instead we reborrow (`&mut *repo`) at every call site: each reborrow is a
// fresh `&'short mut PgConnection` whose borrow ends when its awaited call returns, which
// is exactly what sqlx's `impl<'c> Executor<'c> for &'c mut PgConnection` wants. A generic
// `&mut E` can't work here — it would demand `for<'a> &'a mut E: Executor<'a>`, but sqlx
// ties the executor lifetime to the borrow, so that HRTB is never satisfiable.
{
    let advisor = record;

    let mathematician = Mathematician {
        id: advisor.id,
        name: Some(advisor.name),
        dissertation: advisor.dissertation,
        graduating_year: advisor.year,
        school: advisor.school.clone(),
    };


    let country = advisor.country.map(|name| Country { name });
    let school = advisor.school.map(|name| School { name });

    let school_location = if country.is_some() && school.is_some() {
        Some(SchoolLocation {
            school: school.clone().unwrap().name,
            country: country.clone().unwrap().name,
        })
    } else {
        None
    };

    if let Some(country) = &country {
        (&mut *repo).update_country(&country).await?;
        debug!("country inserted");
    }

    if let Some(school) = &school {
        (&mut *repo).update_school(&school).await?;
        debug!("school inserted");
    }

    if let Some(school_location) = &school_location {
        (&mut *repo).update_location(&school_location).await?;
        debug!("school location inserted");
    }


    (&mut *repo).upsert_mathematician(&mathematician).await?;
    debug!("mathematician inserted");


    Ok(())
}

#[instrument(skip(repo))]
async fn insert_relations(
    repo: &mut sqlx::PgConnection,
    advisor: &MGPage,
    descendants: &Vec<MGPage>) -> color_eyre::Result<()>
{
    for descendant in descendants {
        let relation = AdvisorRelation {
            advisor: advisor.id,
            advisee: descendant.id,
        };

        (&mut *repo).update_advisor_relation(&relation).await?;
        debug!("{}->{}: {}->{} inserted", advisor.id, descendant.id, advisor.name, descendant.name);
    }

    Ok(())
}

#[instrument(skip(pool))]
async fn insert_layer(
    pool: &PgPool,
    advisor: MGPage,
    descendants: Vec<MGPage>) -> color_eyre::Result<()> {
    let mut tx = pool.begin().await?;

    insert_record(&mut tx, advisor.clone()).await?;
    for descendant in &descendants {
        insert_record(&mut tx, descendant.clone()).await?;
    }

    insert_relations(&mut tx, &advisor, &descendants).await?;

    sqlx::query!(
        "INSERT INTO scrape_logs(date, page_scraped, result) VALUES(NOW(), $1, 'success')",
        advisor.id.0
    )
        .execute(&mut *tx)
        .await
        .inspect_err(|e| error!("Failed to insert {} to scrape log: {})", advisor.id.0, e))?;

    tx.commit().await?;

    Ok(())
}
#[derive(Debug)]
struct Scraper {
    db_pool: Arc<sqlx::Pool<sqlx::Postgres>>,
    client: Client,

    /// used to limit the number of in-flight HTTP requests
    semaphore: Semaphore,
    // TODO: implement an error tolerance and stop after a certain threshold
}

lazy_static! {
    static ref PARAGRAPH_SELECTOR: Selector = Selector::parse("p").unwrap();
}

impl Scraper {
    /// The stupid page returns a 200 even if the ID is not found
    fn id_not_found_err(page: &Html) -> bool {
        page.select(&PARAGRAPH_SELECTOR)
            .any(|e| e.text().any(|t| t.trim() == "You have specified an ID that does not exist in the database. Please back up and try again."))
    }


    #[instrument(skip(self))]
    async fn download_page(&self, url: &str) -> color_eyre::Result<String> {
        async fn download_page(client: &Client, url: &str) -> color_eyre::Result<String> {
            Ok(client.get(url).send().await?.text().await?)
        }

        let _permit = self.semaphore.acquire().await?;
        let mut retry = 3;
        loop {
            if retry == 0 {
                error!("Failed to get {url} after 3 tries");
                return Err(eyre!("Failed to get {url}"));
            }

            match download_page(&self.client, &url).await {
                Ok(page) => break Ok(page),
                Err(e) => {
                    debug!("Failed to get page: {e}");

                    let factor = {
                        let dist = Uniform::new(10.0, 30.0);
                        let mut rng = rand::thread_rng();
                        dist.sample(&mut rng)
                    };

                    let wait_duration = Duration::from_millis((1000. * factor) as u64);

                    warn!("{url} Connection failed, waiting for {wait_duration:?}");
                    sleep(wait_duration).await;
                    retry -= 1;
                }
            }
        }
    }

    async fn download_and_parse(&self, id: Id) -> color_eyre::Result<MGPage> {
        async fn log_failure(id: Id, pool: Arc<sqlx::Pool<sqlx::Postgres>>) {
            let _ = sqlx::query!("INSERT INTO scrape_logs(date, page_scraped, result) VALUES(NOW(), $1, 'failed')", id.0)
                .execute(&*pool)
                .await
                .inspect_err(|e| error!("We can't even insert into our log for {id}: {e}"));
        }

        let url = format!("https://www.mathgenealogy.org/id.php?id={}", id.0);
        let page = self.download_page(&url).await?;
        let page = Html::parse_document(&page);

        if Self::id_not_found_err(&page) {
            let pool = Arc::clone(&self.db_pool);

            // Html is not send so we have to do this stupid nonsense
            tokio::spawn(async move {
                log_failure(id, pool).await;
            });

            return Err(eyre!("{id} is not a valid ID in their database"));
        }
        let record = parser::scrape(id, &page)?;
        Ok(record)
    }

    async fn scrape_single(&self, id: Id) -> color_eyre::Result<Option<MGPage>> {
        let now = chrono::Local::now();
        let last_successful_scrape = sqlx::query_file_as!(LastScraped, "queries/latest_page_scrape_record.sql", id.0 as i64, "success")
            .fetch_optional(&*self.db_pool)
            .await?;

        if let Some(last_scraped) = last_successful_scrape
            && now.signed_duration_since(last_scraped.date).abs().to_std()? < Duration::from_hours(24)
        {
            return Ok(None)
        }

        info!("Working on {id}");

        let advisor = self.download_and_parse(id).await?;
        info!("Page {id} scrapped");
        Ok(Some(advisor))
    }

    /// Return:
    /// Error(e) -> something went wrong
    /// Ok(None) -> the page doesn't exist (at all) or was recently scraped
    /// Ok(page) -> page obtained from cache or freshly downloaded
    async fn scrape_single_cached(&self, id: Id, cache: &RwLock<BTreeMap<Id, MGPage>>) -> color_eyre::Result<Option<MGPage>> {
        // first, check if we have a cached version, then release the refcell
        let has = {
            cache.read().expect("not poisoned").contains_key(&id)
        };
        if has {
            return Ok(cache.read().expect("not poisoned").get(&id).cloned())
        }

        // now we know it's not cached at the point of checking, download it
        let page = self.scrape_single(id).await?;
        match page {
            None => Ok(None),
            Some(page) => {
                cache.write().expect("not poisoned").insert(id, page.clone());
                Ok(Some(page))
            }
        }
    }

    async fn scrape_multiple_cached<I>(&self, ids: I, cache: &RwLock<BTreeMap<Id, MGPage>>) -> color_eyre::Result<Vec<MGPage>>
    where
        I: Iterator<Item=Id>,
    {

        let mut descendant_pages: FuturesUnordered<_> = ids
            .map(|id| {
                self.scrape_single_cached(id, cache)
            })
            .collect();

        let mut descendants = vec![];
        while let Some(Ok(Some(page))) = descendant_pages.next().await {
            descendants.push(page);
        }
        Ok(descendants)
    }

    #[instrument(skip(self))]
    async fn bfs_walk(&self, root: Id) -> color_eyre::Result<()> {
        let mut q = VecDeque::new();
        q.push_back(root);
        let mut id2page = RwLock::new(BTreeMap::new());

        while let Some(id) = q.pop_front() {
            match self.visit(id, &mut id2page).await {
                Ok((root, children)) => {
                    q.extend(children);
                    id2page.write().expect("not poisoned").remove(&root);
                }
                Err(e) => {
                    warn!("Failed to visit {id}: {e}");
                }
            }
        }

        Ok(())
    }

    async fn visit(&self, root: Id, id2page: &RwLock<BTreeMap<Id, MGPage>>) -> color_eyre::Result<(Id, Vec<Id>)>     {
        let root_page = self.scrape_single_cached(root, id2page).await?;

        if root_page.is_none() {
            info!("{root} doesn't have a page or was scraped in the last 24 hours");
            return Err(eyre!("{root} doesn't have a page"));
        }
        let root_page = root_page.unwrap();

        let descendants = self.scrape_multiple_cached(root_page.students.iter().map(|s| s.id), id2page).await?;

        insert_layer(&self.db_pool, root_page, descendants.clone()).await?;

        Ok((root, descendants.iter().map(|d| d.id).collect()))
    }
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    color_eyre::install()?;

    let postgres_url = std::env::var(&"POSTGRES_URL").expect("POSTGRES_URL is not set");

    let db_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(12)
        .connect(&postgres_url)
        .await?;
    let pool = Arc::new(db_pool);

    let http_cli = reqwest::Client::new();

    let scraper = Scraper {
        db_pool: Arc::clone(&pool),
        client: http_cli,
        semaphore: Semaphore::new(args.concurrency),
    };
    let scraper = Arc::new(scraper);

    let mut tasks = vec![];

    for id in 6448..=6448 {
        let id = parser::Id(id);
        let scraper = Arc::clone(&scraper);

        let task = tokio::spawn(async move {
            let _ = scraper.bfs_walk(id).await.inspect_err(|e| eprintln!("{e}"));
        });

        tasks.push(task);
    }

    for task in tasks {
        let _ = task.await;
    }

    Ok(())
}
