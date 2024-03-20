#![allow(dead_code)]

mod mathematician;
mod parser;

use color_eyre::eyre::eyre;
use mathematician::Mathematician;
use rand_distr::Distribution;
use rand_distr::Normal;
use reqwest::Client;
use scraper::Html;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::sleep;
use tracing::debug;
use tracing::info;
use tracing::instrument;
use tracing::warn;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Id<T>
where
    T: Debug + PartialEq + Eq + Clone,
{
    pub id: i32,
    pub inner: T,
}

async fn insert_mathematician(
    pool: &sqlx::Pool<sqlx::Postgres>,
    mathematician: &Id<&Mathematician>,
) -> color_eyre::Result<()> {
    let mut transaction = pool.begin().await?;

    if let Some(ref inner) = mathematician.inner.school {
        // insert the school first
        let school_id = sqlx::query!(
            "INSERT INTO school(name, country) VALUES ($1, $2) RETURNING id",
            inner.name,
            inner.country
        )
        .fetch_one(&mut *transaction)
        .await?;

        sqlx::query!("INSERT INTO mathematicians (id, name, school, dissertation, year) VALUES ($1, $2, $3, $4, $5);",
                mathematician.id,
                mathematician.inner.name,
                school_id.id,
                mathematician.inner.dissertation,
                mathematician.inner.year.map(|x| x as i16)).execute(&mut *transaction).await?;
    } else {
        sqlx::query!(
            "INSERT INTO mathematicians (id, name, dissertation, year) VALUES ($1, $2, $3, $4)",
            mathematician.id,
            mathematician.inner.name,
            mathematician.inner.dissertation,
            mathematician.inner.year.map(|x| x as i16)
        )
        .execute(&mut *transaction)
        .await?;
    }

    transaction.commit().await?;
    Ok(())
}

async fn insert_relation(
    pool: &sqlx::Pool<sqlx::Postgres>,
    advisor: &Id<&Mathematician>,
    advisee: &Id<&Mathematician>,
) -> color_eyre::Result<()> {
    // TODO: worry about conflicts later
    let query = sqlx::query!(
        r"INSERT INTO advisor_relations(advisor, advisee) VALUES ($1, $2);",
        advisor.id,
        advisee.id
    );
    let _result = query.execute(pool).await?;

    Ok(())
}

async fn has_mathematician(pool: &sqlx::Pool<sqlx::Postgres>, id: i32) -> color_eyre::Result<bool> {
    let query = sqlx::query!(
        r"SELECT COUNT(*) FROM mathematicians WHERE id = $1 LIMIT 1;",
        id
    );
    let result = query.fetch_one(pool).await?;

    Ok(result.count == Some(1))
}

#[derive(Debug)]
struct Scraper {
    db_pool: Arc<sqlx::Pool<sqlx::Postgres>>,
    client: Client,
}

impl Scraper {
    #[instrument]
    async fn scrape(&self, id: i32) -> color_eyre::Result<()> {
        // first see if the mathematician already exists
        if has_mathematician(&self.db_pool, id).await? {
            return Ok(());
        }
        debug!("{id} requires scrapping");

        // scrape the page
        let url = format!("https://www.mathgenealogy.org/id.php?id={id}");

        async fn get(client: &Client, url: &str) -> color_eyre::Result<String> {
            Ok(client.get(url).send().await?.text().await?)
        }

        let mut retry = 3;
        let page = loop {
            if retry == 0 {
                return Err(eyre!("Failed to get page"));
            }

            match get(&self.client, &url).await {
                Ok(page) => break page,
                Err(e) => {
                    warn!("Failed to get page: {e}");

                    let factor = {
                        let dist = Normal::new(1.0, 2.0).unwrap();
                        let mut rng = rand::thread_rng();
                        dist.sample(&mut rng)
                    };

                    let wait_duration =
                        Duration::from_millis((1000. * (3f64 - retry as f64) * factor) as u64);

                    info!("Connection failed, waiting for {wait_duration:?}");
                    sleep(wait_duration).await;
                    retry -= 1;
                }
            }
        };

        info!("scraping {}", url);

        let (advisor, advisees) = {
            let page = Html::parse_document(&page);
            let page = Mutex::new(page);
            let page = page.lock().unwrap();

            let advisor = parser::scrape_mathematician(&*page)?;

            let advisees = parser::scrape_students(&*page)?;

            (advisor, advisees)
        };

        // TODO: we will figure out how to bulk insert more efficiently later

        // insert the advisor
        let advisor = Id {
            id,
            inner: &advisor,
        };

        insert_mathematician(&self.db_pool, &advisor).await?;

        // insert the advisees
        for (id, advisee) in &*advisees {
            let advisee = Id {
                id: *id,
                inner: advisee,
            };
            insert_mathematician(&self.db_pool, &advisee).await?;
            insert_relation(&self.db_pool, &advisor, &advisee).await?;
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    // init tracing with fmt substribers
    tracing_subscriber::fmt::init();

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
    };
    let scraper = Arc::new(scraper);

    let mut tasks = vec![];
    for i in 1..256 {
        let scraper = Arc::clone(&scraper);
        let task = tokio::spawn(async move { scraper.scrape(i).await });
        tasks.push(task);
    }

    for task in tasks {
        let _ = task.await;
    }

    Ok(())
}
