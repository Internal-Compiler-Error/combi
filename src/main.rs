#![allow(dead_code)]

mod mathematician;
mod parser;

use color_eyre::eyre::eyre;
use mathematician::Country;
use mathematician::Dissertation;
use mathematician::GraduationRecord;
use mathematician::Mathematician;
use mathematician::School;
use rand_distr::Distribution;
use rand_distr::Normal;
use reqwest::Client;
use scraper::Html;
use std::fmt::Debug;
use std::sync::Arc;
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

async fn insert_school(
    pool: &sqlx::Pool<sqlx::Postgres>,
    school: &School,
) -> color_eyre::Result<()> {
    let _ = sqlx::query!(
        "INSERT INTO schools(name) VALUES ($1) ON CONFLICT DO NOTHING;",
        school.name,
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn insert_country(
    pool: &sqlx::Pool<sqlx::Postgres>,
    country: &Country,
) -> color_eyre::Result<()> {
    let _ = sqlx::query!(
        "INSERT INTO countries(name) VALUES ($1) ON CONFLICT DO NOTHING;",
        country.name,
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_dissertation(
    pool: &sqlx::Pool<sqlx::Postgres>,
    dissertation: &Dissertation,
) -> color_eyre::Result<()> {
    let _ = sqlx::query!(
        "INSERT INTO dissertations(title, author) VALUES ($1, $2) ON CONFLICT DO NOTHING;",
        dissertation.title,
        dissertation.author.id
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn insert_adivsor_relation(
    pool: &sqlx::Pool<sqlx::Postgres>,
    advisor: &Mathematician,
    advisee: &Mathematician,
) -> color_eyre::Result<()> {
    let _ = sqlx::query!(
        "INSERT INTO advisor_relations(advisor, advisee) VALUES ($1, $2) ON CONFLICT DO NOTHING;",
        advisor.id,
        advisee.id
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_mathematician(
    pool: &sqlx::Pool<sqlx::Postgres>,
    mathematician: &Mathematician,
) -> color_eyre::Result<()> {
    let _ = sqlx::query!(
        "INSERT INTO mathematicians(name) VALUES ($1) ON CONFLICT DO NOTHING;",
        mathematician.name,
    )
    .execute(pool)
    .await?;

    Ok(())
}

async fn insert_relation(
    pool: &sqlx::Pool<sqlx::Postgres>,
    advisor: &Mathematician,
    advisee: &Mathematician,
) -> color_eyre::Result<()> {
    let query = sqlx::query!(
        r"INSERT INTO advisor_relations(advisor, advisee) VALUES ($1, $2);",
        advisor.id,
        advisee.id
    );
    let _result = query.execute(pool).await?;

    Ok(())
}

async fn insert_grad_record(
    pool: &sqlx::Pool<sqlx::Postgres>,
    grad_record: &GraduationRecord,
) -> color_eyre::Result<()> {
    insert_school(pool, &grad_record.school).await?;

    let _ = sqlx::query!(
        "INSERT INTO graduation_records(mathematician, school, year) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;",
        &grad_record.mathematician.id,
        grad_record.school.name,
        grad_record.year as i16,
    )
    .execute(pool)
    .await?;
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

async fn insert_record(
    pool: &sqlx::Pool<sqlx::Postgres>,
    advisor: &parser::ScrapeRecord,
    advisees: &[parser::ScrapeRecord],
) -> color_eyre::Result<()> {
    insert_mathematician(pool, &advisor.mathematician).await?;

    if let Some(dissertation) = &advisor.dissertation {
        insert_dissertation(pool, &dissertation).await?;
    }

    if let Some(record) = &advisor.graduation_record {
        insert_grad_record(pool, &record).await?;
    }

    for advisee in &advisees {
        insert_mathematician(pool, &advisee.mathematician).await?;
        insert_adivsor_relation(pool, &advisor.mathematician, &advisee.mathematician).await?;
    }

    Ok(())
}

#[derive(Debug)]
struct Scraper {
    db_pool: Arc<sqlx::Pool<sqlx::Postgres>>,
    client: Client,
}

impl Scraper {
    #[instrument]
    async fn get_page(&self, url: &str) -> color_eyre::Result<Html> {
        async fn get_page(client: &Client, url: &str) -> color_eyre::Result<String> {
            Ok(client.get(url).send().await?.text().await?)
        }

        let mut retry = 3;
        let page = loop {
            if retry == 0 {
                warn!("Failed to get {url} after 3 tries");
                return Err(eyre!("Failed to get page"));
            }

            match get_page(&self.client, &url).await {
                Ok(page) => break page,
                Err(e) => {
                    debug!("Failed to get page: {e}");

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

        Ok(Html::parse_document(&page))
    }

    #[instrument]
    async fn scrape(&self, id: i32) -> color_eyre::Result<()> {
        // first see if the mathematician already exists
        if has_mathematician(&self.db_pool, id).await? {
            return Ok(());
        }
        debug!("{id} requires scrapping");

        // scrape the page
        let url = format!("https://www.mathgenealogy.org/id.php?id={id}");

        info!("scraping {}", url);
        let advisor = {
            let page = self.get_page(&url).await?;
            parser::scrape(&page)?
        };

        let mut advisees = vec![];
        // visit all the students
        for student_id in &advisor.students_ids {
            let url = format!("https://www.mathgenealogy.org/id.php?id={student_id}");
            let student_page = self.get_page(&url).await?;
            let student = parser::scrape(&student_page)?;

            // we only explore one layer deep
            advisees.push(student);
        }

        insert_record(&self.db_pool, &advisor, advisees.as_slice()).await?;
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
