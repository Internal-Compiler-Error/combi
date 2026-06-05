#![allow(dead_code)]

mod mathematician;
mod parser;

use clap::Parser;
use color_eyre::eyre::eyre;
use lazy_static::lazy_static;
use mathematician::Country;
use mathematician::Dissertation;
use mathematician::GraduationRecord;
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
use std::ops::Sub;
use std::sync::Arc;
use std::time::Duration;
use sqlx::types::chrono;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::log::log;
use tracing::warn;

use crate::mathematician::AdvisorRelation;
use crate::mathematician::AdvisorRelationRepo;
use crate::mathematician::CountryRepo;
use crate::mathematician::DissertationRepo;
use crate::mathematician::GraduationRecordRepo;
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

#[instrument(skip(pool))]
async fn insert_record(
    pool: &PgPool,
    record: MGPage,
) -> color_eyre::Result<()> {
    let advisor = record;

    let mathematician = Mathematician {
        id: advisor.id,
        name: Some(advisor.name),
    };

    pool.upsert_mathematician(&mathematician).await?;
    debug!("mathematician inserted");

    if let Some(dissertation) = advisor.dissertation {
        let dissertation = Dissertation {
            title: dissertation,
            author: advisor.id,
        };

        pool.upsert_dissertation(&dissertation).await?;
        debug!("dissertation inserted");
    }

    let country = advisor.country.map(|name| Country { name });
    let school = advisor.school.map(|name| School { name });
    let year = advisor.year;

    let grad_record = if country.is_some() && school.is_some() && year.is_some() {
        Some(GraduationRecord {
            mathematician: advisor.id,
            school: school.clone().unwrap().name,
            year: year.unwrap() as i32,
        })
    } else {
        None
    };

    let school_location = if country.is_some() && school.is_some() {
        Some(SchoolLocation {
            school: school.clone().unwrap().name,
            country: country.clone().unwrap().name,
        })
    } else {
        None
    };

    if let Some(country) = &country {
        pool.update_country(&country).await?;
        debug!("country inserted");
    }

    if let Some(school) = &school {
        pool.update_school(&school).await?;
        debug!("school inserted");
    }

    if let Some(school_location) = &school_location {
        pool.update_location(&school_location).await?;
        debug!("school location inserted");
    }

    if let Some(grad_record) = &grad_record {
        pool.upsert_graduation_record(&grad_record).await?;
        debug!("graduation record inserted");
    }


    let _ = sqlx::query!(
        "INSERT INTO scrape_logs(date, page_scraped, result) VALUES(NOW(), $1, 'success')",
        advisor.id.0
    )
    .execute(pool)
    .await
    .inspect_err(|e| error!("Failed to insert {} to scrape log: {}))", advisor.id.0, e));

    Ok(())
}

#[instrument(skip(pool))]
async fn insert_relations(
    pool: &PgPool,
    advisor: MGPage,
    descendants: Vec<MGPage>) -> color_eyre::Result<()> {
    for descendant in &descendants {
        let relation = AdvisorRelation {
            advisor: advisor.id,
            advisee: descendant.id,
        };

        pool.update_advisor_relation(&relation).await?;
        debug!("{}->{}: {}->{} inserted", advisor.id, descendant.id, advisor.name, descendant.name);
    }

    Ok(())
}

#[instrument(skip(pool))]
async fn insert_layer(
    pool: &PgPool,
    advisor: MGPage,
    descendants: Vec<MGPage>) -> color_eyre::Result<()> {
    // TODO: wrap the following into a transaction

    insert_record(pool, advisor.clone()).await?;
    for descendant in &descendants {
        insert_record(pool, descendant.clone()).await?;
    }

    insert_relations(pool, advisor, descendants).await?;
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

    async fn get_record(&self, id: Id) -> color_eyre::Result<MGPage> {
        async fn insert_to_bogus(id: Id, pool: Arc<sqlx::Pool<sqlx::Postgres>>) {
            let _ = sqlx::query!("INSERT INTO scrape_logs(date, page_scraped, result) VALUES(NOW(), $1, 'failed')", id.0)
                .execute(&*pool)
                .await
                .inspect_err(|e| error!("We can't even insert into our log for {id}"));
        }

        let url = format!("https://www.mathgenealogy.org/id.php?id={}", id.0);
        let page = self.download_page(&url).await?;
        let page = Html::parse_document(&page);

        if Self::id_not_found_err(&page) {
            let pool = Arc::clone(&self.db_pool);

            // Html is not send so we have to do this stupid nonsense
            tokio::spawn(async move {
                insert_to_bogus(id, pool).await;
            });

            return Err(eyre!("{id} is not a valid ID in their database"));
        }
        let record = parser::scrape(id, &page)?;
        Ok(record)
    }
    /// A layer is defined as one node and all the nodes connected to it, i.e. one layer of a tree
    async fn get_single_layer(&self, root: Id) -> color_eyre::Result<(MGPage, Vec<MGPage>)> {
        let root_page = self.scrape_single(root).await?;

        if root_page.is_none() {
            info!("{root} doesn't have a page or was scraped in the last 24 hours");
            return Err(eyre!("{root} doesn't have a page"));
        }
        let root_page = root_page.unwrap();

        let mut descendants = vec![];
        for descendant in &root_page.students {
            if let Some(descendant_id) = descendant.id {
                let descendant_page = self.scrape_single(descendant_id).await;
                match descendant_page {
                    Err(err) => {
                        info!("{descendant_id}: {err}");
                        continue
                    },
                    Ok(None) => {
                        info!("{descendant_id}: doesn't have a page");
                    },
                    Ok(Some(descendant)) => {
                        descendants.push(descendant);
                    }
                }
            }
        }
        Ok((root_page, descendants))
    }

    #[instrument(skip(self))]
    async fn scrape_single_layer(&self, root: Id) -> color_eyre::Result<()> {
        let (advisor, descendants) = self.get_single_layer(root).await?;
        insert_layer(&self.db_pool, advisor, descendants).await?;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn scrape_single(&self, id: Id) -> color_eyre::Result<Option<MGPage>> {
        // TODO: implement a logic where if the record has been updated recently, then skip it
        // unless we are in a forced mode
        // TODO: implement a force mode to skip this check
        let now = chrono::Local::now();
        let last_scraped = sqlx::query_file_as!(LastScraped, "queries/latest_page_scrape_record.sql", id.0 as i64)
            .fetch_optional(&*self.db_pool)
            .await?;


        if let Some(last_scraped) = last_scraped
            && now.signed_duration_since(last_scraped.date).abs().to_std()? < Duration::from_hours(24)
        {
            return Ok(None);
        }

        info!("Working on {id}");

        let advisor = self.get_record(id).await?;
        info!("Page {id} scrapped");
        Ok(Some(advisor))
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
        // semaphore: Semaphore::new(args.concurrency),
        semaphore: Semaphore::new(1),
    };
    let scraper = Arc::new(scraper);

    let mut tasks = vec![];

    for id in 300..=600 {
        let id = parser::Id(id);
        let scraper = Arc::clone(&scraper);

        let task = tokio::spawn(async move {
            scraper.scrape_single_layer(id).await.inspect_err(|e| println!("{e}"));
        });

        tasks.push(task);
    }

    for task in tasks {
        let _ = task.await;
    }

    Ok(())
}
