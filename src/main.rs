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
use sqlx::PgPool;

use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
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
use crate::mathematician::DissertationRepo;
use crate::mathematician::GraduationRecordRepo;
use crate::mathematician::MathematicianRepo;
use crate::mathematician::SchoolLocationRepo;
use crate::mathematician::SchoolRepo;

#[derive(Debug, Eq, PartialEq, Parser)]
struct Args {
    /// the number of concurrent requests to make
    #[clap(short, long, default_value = "256")]
    concurrency: usize,
}

#[instrument(skip(pool))]
async fn insert_record(
    pool: &PgPool,
    record: (parser::Id, parser::ScrapeRecord),
) -> color_eyre::Result<()> {
    let advisor_id = record.0;
    let advisor = record.1;

    let mathematician = Mathematician {
        id: advisor_id,
        name: advisor.name,
    };

    pool.update_mathematician(&mathematician).await.unwrap();
    debug!("mathematician inserted");

    if let Some(dissertation) = advisor.dissertation {
        let dissertation = Dissertation {
            title: dissertation,
            author: advisor_id,
        };

        pool.update_dissertation(&dissertation).await.unwrap();
        debug!("disseration inserted");
    }

    let country = advisor.country.map(|name| Country { name });
    let school = advisor.school.map(|name| School { name });
    let year = advisor.year;

    let grad_record = if country.is_some() && school.is_some() && year.is_some() {
        Some(GraduationRecord {
            mathematician: advisor_id,
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
        pool.update_country(&country).await.unwrap();
        debug!("country inserted");
    }

    if let Some(school) = &school {
        pool.update_school(&school).await.unwrap();
        debug!("school inserted");
    }

    if let Some(school_location) = &school_location {
        pool.update_location(&school_location).await.unwrap();
        debug!("school location inserted");
    }

    if let Some(grad_record) = &grad_record {
        pool.update_graduation_record(&grad_record).await.unwrap();
        debug!("graduation record inserted");
    }

    for student in &advisor.students {
        if let Some(student_id) = student.id {
            let mathematician = Mathematician {
                id: student_id,
                name: student.name.clone(),
            };

            let relation = AdvisorRelation {
                advisor: advisor_id,
                advisee: student_id,
            };

            pool.update_mathematician(&mathematician).await.unwrap();
            pool.update_advisor_relation(&relation).await.unwrap();
            debug!("adivsor avisee record inserted");
        }
    }

    Ok(())
}

#[derive(Debug)]
struct Scraper {
    db_pool: Arc<sqlx::Pool<sqlx::Postgres>>,
    client: Client,

    /// used to limit the number of in-flight HTTP requests
    semaphore: Semaphore,
    // TODO: implement an error tolerance and stop after a certrain threshold
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
    async fn get_page(&self, url: &str) -> color_eyre::Result<String> {
        async fn get_page(client: &Client, url: &str) -> color_eyre::Result<String> {
            Ok(client.get(url).send().await?.text().await?)
        }

        let mut retry = 3;
        loop {
            if retry == 0 {
                error!("Failed to get {url} after 3 tries");
                return Err(eyre!("Failed to get {url}"));
            }

            match get_page(&self.client, &url).await {
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

    async fn get_record(&self, id: parser::Id) -> color_eyre::Result<parser::ScrapeRecord> {
        let url = format!("https://www.mathgenealogy.org/id.php?id={}", id.0);
        let page = self.get_page(&url).await?;
        let page = Html::parse_document(&page);
        if Self::id_not_found_err(&page) {
            return Err(eyre!("{id} is not a valid ID in their database"));
        }
        let record = parser::scrape(&page)?;
        Ok(record)
    }

    #[instrument(skip(self))]
    async fn scrape(&self, id: parser::Id) -> color_eyre::Result<()> {
        // TODO: implement a logic where if the record has been updated recently, then skip it
        // unless we are in a forced mode

        let _permit = self.semaphore.acquire().await?;
        info!("Working on {id}");

        let advisor = self.get_record(id).await?;

        info!("Main mathematician scraped {id}");

        insert_record(&*self.db_pool, (id, advisor)).await
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

    let client = reqwest::Client::new();

    let scraper = Scraper {
        db_pool: Arc::clone(&pool),
        client,
        semaphore: Semaphore::new(args.concurrency),
    };
    let scraper = Arc::new(scraper);

    let mut tasks = vec![];

    for id in 58200..=350000 {
        let id = parser::Id(id);
        let scraper = Arc::clone(&scraper);

        let task = tokio::spawn(async move { scraper.scrape(id).await });

        tasks.push(task);
    }

    for task in tasks {
        let _ = task.await;
    }

    Ok(())
}
