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
use rand_distr::Uniform;
use reqwest::Client;
use scraper::Html;
use sqlx::PgConnection;
use sqlx::Postgres;
use sqlx::Transaction;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::warn;

#[instrument(skip(executor))]
async fn insert_school<'a, E>(executor: E, school: &School) -> color_eyre::Result<()>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    let _ = sqlx::query!(
        "INSERT INTO schools(name) VALUES ($1) ON CONFLICT DO NOTHING;",
        school.name,
    )
    .execute(executor)
    .await
    .inspect_err(|e| {
        error!("Failed to insert advisor relation: {e}");
    })?;

    Ok(())
}

#[instrument(skip(executor))]
async fn insert_school_location<'a, E>(
    executor: E,
    school: &School,
    country: &Country,
) -> color_eyre::Result<()>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    let _ = sqlx::query!(
        "INSERT INTO school_locations(school, country) VALUES ($1, $2) ON CONFLICT DO NOTHING;",
        school.name,
        country.name,
    )
    .execute(executor)
    .await
    .inspect_err(|e| {
        error!("Failed to insert advisor relation: {e}");
    })?;

    debug!("school location inserted {} {}", school.name, country.name);
    Ok(())
}

#[instrument(skip(executor))]
async fn insert_country<'a, E>(executor: E, country: &Country) -> color_eyre::Result<()>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    let _ = sqlx::query!(
        "INSERT INTO countries(name) VALUES ($1) ON CONFLICT DO NOTHING;",
        country.name,
    )
    .execute(executor)
    .await
    .inspect_err(|e| {
        error!("Failed to insert advisor relation: {e}");
    })?;

    Ok(())
}

#[instrument(skip(executor))]
async fn insert_dissertation<'a, E>(
    executor: E,
    dissertation: &Dissertation,
) -> color_eyre::Result<()>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    let _ = sqlx::query!(
        "INSERT INTO dissertations(title, author) VALUES ($1, $2) ON CONFLICT DO NOTHING;",
        dissertation.title,
        dissertation.author.id
    )
    .execute(executor)
    .await
    .inspect_err(|e| {
        error!("Failed to insert advisor relation: {e}");
    })?;

    Ok(())
}

#[instrument(skip(executor))]
async fn insert_adivsor_relation<'a, E>(
    executor: E,
    advisor: &Mathematician,
    advisee: &Mathematician,
) -> color_eyre::Result<()>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    let _ = sqlx::query!(
        "INSERT INTO advisor_relations(advisor, advisee) VALUES ($1, $2) ON CONFLICT DO NOTHING;",
        advisor.id,
        advisee.id
    )
    .execute(executor)
    .await
    .inspect_err(|e| {
        error!("Failed to insert advisor relation: {e}");
    })?;
    Ok(())
}

#[instrument(skip(executor))]
async fn insert_mathematician<'a, E>(
    executor: E,
    mathematician: &Mathematician,
) -> color_eyre::Result<()>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    let _ = sqlx::query!(
        "INSERT INTO mathematicians(id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING;",
        mathematician.id,
        mathematician.name,
    )
    .execute(executor)
    .await
    .inspect_err(|e| {
        error!("Failed to insert mathematician: {e}");
    })?;

    Ok(())
}

#[instrument(skip(executor))]
async fn insert_relation<'a, E>(
    executor: E,
    advisor: &Mathematician,
    advisee: &Mathematician,
) -> color_eyre::Result<()>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    let query = sqlx::query!(
        r"INSERT INTO advisor_relations(advisor, advisee) VALUES ($1, $2);",
        advisor.id,
        advisee.id
    );
    let _result = query.execute(executor).await.inspect_err(|e| {
        error!("Failed to insert advisor relation: {e}");
    })?;

    Ok(())
}

#[instrument(skip(executor))]
async fn insert_grad_record<E>(
    executor: &mut PgConnection,
    grad_record: &GraduationRecord,
) -> color_eyre::Result<()> {
    insert_school(&mut *executor, &grad_record.school).await?;

    if let Some(ref country) = &grad_record.country {
        insert_country(&mut *executor, &country).await?;
        insert_school_location(&mut *executor, &grad_record.school, &country).await?;
    }

    let _ = sqlx::query!(
        "INSERT INTO graduation_records(mathematician, school, year) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;",
        &grad_record.mathematician.id,
        grad_record.school.name,
        grad_record.year as i16)
        .execute(&mut *executor)
        .await
        .inspect_err(|e| {
            error!("Failed to insert advisor relation: {e}");
        })?;

    Ok(())
}

async fn has_mathematician<'a, E>(executor: E, id: i32) -> color_eyre::Result<bool>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    let query = sqlx::query!(
        r"SELECT COUNT(*) FROM mathematicians WHERE id = $1 LIMIT 1;",
        id
    );
    let result = query.fetch_one(executor).await?;

    Ok(result.count == Some(1))
}

#[instrument(level = "debug", skip(executor))]
async fn has_advisor_advisee<'a, E>(
    executor: E,
    advisor: i32,
    advisee: i32,
) -> color_eyre::Result<bool>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    let query = sqlx::query!(
        r"SELECT COUNT(*) FROM advisor_relations WHERE advisor = $1 AND advisee = $2 LIMIT 1;",
        advisor,
        advisee
    );
    let result = query.fetch_one(executor).await.inspect_err(|e| {
        error!("Failed to find out about advisor relation: {e}");
    })?;

    Ok(result.count == Some(1))
}

#[instrument(skip(transaction))]
async fn insert_record<'a>(
    mut transaction: Transaction<'a, Postgres>,
    advisor: &parser::ScrapeRecord,
    advisees: &[parser::ScrapeRecord],
) -> color_eyre::Result<()> {
    insert_mathematician(&mut *transaction, &advisor.mathematician).await?;
    debug!("mathematician inserted");

    if let Some(dissertation) = &advisor.dissertation {
        insert_dissertation(&mut *transaction, &dissertation).await?;
        debug!("disseration inserted");
    }

    if let Some(record) = &advisor.graduation_record {
        insert_grad_record::<PgConnection>(&mut *transaction, &record).await?;
        debug!("grad record inserted");
    }

    for advisee in advisees {
        insert_mathematician(&mut *transaction, &advisee.mathematician).await?;
        insert_adivsor_relation(
            &mut *transaction,
            &advisor.mathematician,
            &advisee.mathematician,
        )
        .await?;

        debug!("adivsor avisee record inserted");
    }

    transaction.commit().await?;
    Ok(())
}

#[derive(Debug)]
struct Scraper {
    db_pool: Arc<sqlx::Pool<sqlx::Postgres>>,
    client: Client,
}

impl Scraper {
    #[instrument(skip(self))]
    async fn get_page(&self, url: &str) -> color_eyre::Result<Html> {
        async fn get_page(client: &Client, url: &str) -> color_eyre::Result<String> {
            Ok(client.get(url).send().await?.text().await?)
        }

        let mut retry = 3;
        let page = loop {
            if retry == 0 {
                error!("Failed to get {url} after 3 tries");
                return Err(eyre!("Failed to get {url}"));
            }

            match get_page(&self.client, &url).await {
                Ok(page) => break page,
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
        };

        Ok(Html::parse_document(&page))
    }

    #[instrument(skip(self))]
    async fn scrape(&self, id: i32) -> color_eyre::Result<()> {
        //
        // first see if the mathematician already exists
        if has_mathematician(&*self.db_pool, id).await? {
            return Ok(());
        }
        info!("Started scraping");

        // scrape the page
        let url = format!("https://www.mathgenealogy.org/id.php?id={id}");

        let advisor = {
            let page = self.get_page(&url).await.inspect_err(|e| {
                error!("Failed to get page: {e}");
            })?;
            let mut advisor = parser::scrape(&page, id).inspect_err(|e| {
                error!("Failed to scrape page: {e}");
            })?;

            advisor
        };
        info!("Main mathematician scraped");
        insert_mathematician(&*self.db_pool, &advisor.mathematician).await?;

        let mut advisees = vec![];
        // visit all the students
        for student_id in &advisor.students_ids {
            if has_mathematician(&*self.db_pool, *student_id).await?
                && has_advisor_advisee(&*self.db_pool, id, *student_id).await?
            {
                continue;
            }
            let url = format!("https://www.mathgenealogy.org/id.php?id={student_id}");
            let student_page = self.get_page(&url).await?;
            let mut student = parser::scrape(&student_page, *student_id)?;
            info!("Student scraped {student_id}");
            // student.mathematician.id = *student_id;

            //
            //
            // we only explore one layer deep
            //
            advisees.push(student);
        }

        info!("Started transaction");
        let transaction = self.db_pool.begin().await?;
        insert_record(transaction, &advisor, advisees.as_slice()).await?;
        info!("Transaction committed");

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
    // let mut rng = thread_rng();
    // let dist = Uniform::new(0, 307384);

    for id in 1..=307433 {
        // let id = dist.sample(&mut rng);
        let scraper = Arc::clone(&scraper);

        if !has_mathematician(&*scraper.db_pool, id).await? {
            let task = tokio::spawn(async move { scraper.scrape(id).await });

            // sleep for 1 second
            let sleep_duration = Duration::from_millis(700);
            sleep(sleep_duration).await;
            tasks.push(task);
        }
    }

    for task in tasks {
        let _ = task.await;
    }

    Ok(())
}
