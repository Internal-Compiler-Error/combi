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
use rand_distr::Distribution;
use rand_distr::Uniform;
use reqwest::Client;
use scraper::Html;
use scraper::Selector;
use sqlx::PgConnection;
use sqlx::Postgres;
use sqlx::Transaction;
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
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Debug, Eq, PartialEq, Parser)]
struct Args {
    /// the number of concurrent requests to make
    #[clap(short, long, default_value = "256")]
    concurrency: usize,
}

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
        error!("Failed to insert school name {e}");
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
        error!("Failed to insert school location {e}");
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
        error!("Failed to insert country {e}");
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
        dissertation.author.id.0
    )
    .execute(executor)
    .await
    .inspect_err(|e| {
        error!("Failed to insert dissertation {e}");
    })?;

    Ok(())
}

#[instrument(skip(executor))]
async fn insert_adivsor_relation<'a, E>(
    executor: E,
    advisor: parser::Id,
    advisee: parser::Id,
) -> color_eyre::Result<()>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    let _ = sqlx::query!(
        "INSERT INTO advisor_relations(advisor, advisee) VALUES ($1, $2) ON CONFLICT DO NOTHING;",
        advisor.0,
        advisee.0,
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
    id: parser::Id,
    name: impl AsRef<str> + Debug,
) -> color_eyre::Result<()>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    let _ = sqlx::query!(
        "INSERT INTO mathematicians(id, name) VALUES ($1, $2) ON CONFLICT DO NOTHING;",
        id.0,
        name.as_ref(),
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
    advisor: parser::Id,
    advisee: parser::Id,
) -> color_eyre::Result<()>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    let query = sqlx::query!(
        r"INSERT INTO advisor_relations(advisor, advisee) VALUES ($1, $2);",
        advisor.0,
        advisee.0,
    );
    let _result = query.execute(executor).await.inspect_err(|e| {
        error!("Failed to insert advisor relation: {e}");
    })?;

    Ok(())
}

#[instrument(skip(executor))]
// TODO: figure out what the fuck am I suppose to do make executor a generic
async fn insert_grad_record<E>(
    executor: &mut E,
    grad_record: &GraduationRecord,
) -> color_eyre::Result<()>
where
    for<'a> &'a mut E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    insert_school::<&mut E>(executor, &grad_record.school).await?;
    let _ = sqlx::query!(
        "INSERT INTO graduation_records(mathematician, school, year) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;",
        &grad_record.mathematician.0,
        grad_record.school.name,
        grad_record.year as i32)
        .execute(executor)
        .await
        .inspect_err(|e| {
            error!("Failed to insert graduation record {e}");
        })?;

    Ok(())
}

#[instrument(level = "debug", skip(executor))]
async fn has_advisor_advisee<'a, E>(
    executor: E,
    advisor: parser::Id,
    advisee: parser::Id,
) -> color_eyre::Result<bool>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    let query = sqlx::query!(
        r"SELECT COUNT(*) FROM advisor_relations WHERE advisor = $1 AND advisee = $2 LIMIT 1;",
        advisor.0,
        advisee.0,
    );
    let result = query.fetch_one(executor).await.inspect_err(|e| {
        error!("Failed to find out about advisor relation {e}");
    })?;

    Ok(result.count == Some(1))
}

#[instrument(level = "debug", skip(executor))]
async fn is_likely_bogus<'a, E>(executor: E, id: parser::Id) -> color_eyre::Result<bool>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    let count = sqlx::query!(
        r"SELECT COUNT(1) FROM likely_bogus WHERE id = $1 LIMIT 1;",
        id.0
    )
    .fetch_one(executor)
    .await?;

    Ok(count.count == Some(1))
}

#[instrument(level = "debug", skip(executor))]
async fn insert_likely_bogus<'a, E>(executor: E, id: parser::Id) -> color_eyre::Result<()>
where
    E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    sqlx::query!(r"INSERT INTO likely_bogus(id) VALUES($1);", id.0)
        .execute(executor)
        .await?;

    Ok(())
}

#[instrument(skip(transaction))]
async fn insert_record<E>(
    transaction: &mut E,
    record: (parser::Id, &parser::ScrapeRecord),
) -> color_eyre::Result<()>
where
    for<'a> &'a mut E: sqlx::Executor<'a, Database = sqlx::Postgres>,
{
    let advisor_id = record.0;
    let advisor = record.1;

    insert_mathematician::<&mut E>(transaction, advisor_id, &advisor.name).await?;
    debug!("mathematician inserted");

    if let Some(country) = &advisor.country {
        let country = Country {
            name: country.clone(),
        };
        insert_country::<&mut E>(transaction, &country).await?;
        debug!("country inserted");
    }

    if let Some(dissertation) = &advisor.dissertation {
        let dissertation = Dissertation {
            title: dissertation.clone(),
            author: Mathematician {
                id: advisor_id,
                name: advisor.name.clone(),
            },
        };

        insert_dissertation::<&mut E>(transaction, &dissertation).await?;
        debug!("disseration inserted");
    }

    if let Some(school) = &advisor.school {
        let school = School {
            name: school.clone(),
        };
        insert_school::<&mut E>(transaction, &school).await?;
        debug!("school inserted");

        // TODO: yikes, duplicate inserts
        if let Some(country) = &advisor.country {
            let country = Country {
                name: country.clone(),
            };
            insert_school_location::<&mut E>(transaction, &school, &country).await?;
            debug!("school location inserted");
        }
    }

    if let Some(school) = &advisor.school {
        if let Some(year) = advisor.year {
            let graduation_record = GraduationRecord {
                mathematician: advisor_id,
                school: School {
                    name: school.clone(),
                },
                year: year as i32,
            };
            insert_grad_record(transaction, &graduation_record).await?;
            debug!("grad record inserted");
        }
    }

    for student in &advisor.students {
        if let Some(student_id) = student.id {
            insert_mathematician::<&mut E>(transaction, student_id, &student.name).await?;
            insert_adivsor_relation::<&mut E>(transaction, advisor_id, student_id).await?;
            debug!("adivsor avisee record inserted");
        }
    }

    // transaction.commit().await?;
    Ok(())
}

#[derive(Debug)]
struct Scraper {
    db_pool: Arc<sqlx::Pool<sqlx::Postgres>>,
    client: Client,

    /// used to limit the number of in-flight HTTP requests
    semaphore: Semaphore,
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
        // first see if the mathematician already exists
        // if has_mathematician(&*self.db_pool, id).await? {
        //     return Ok(());
        // }

        let _permit = self.semaphore.acquire().await?;
        info!("Working on {id}");

        let advisor = self.get_record(id).await?;
        info!("Main mathematician scraped {id}");
        insert_mathematician(&*self.db_pool, id, &advisor.name).await?;

        let mut advisees = vec![];
        // visit all the students
        for student in &advisor.students {
            let Some(student_id) = student.id else {
                continue;
            };

            // if has_mathematician(&*self.db_pool, student_id).await?
            //     && has_advisor_advisee(&*self.db_pool, id, student_id).await?
            // {
            //     // if they're already in the database, skip
            //     continue;
            // }

            let student = self.get_record(student_id).await?;
            info!("Student scraped {student:?}");

            // we only explore one layer deep
            advisees.push(student);
        }

        info!("Started transaction");
        let mut transaction = self.db_pool.begin().await?;
        let executor = &mut *transaction;
        insert_record(executor, (id, &advisor)).await?;
        info!("Transaction committed");

        Ok(())
    }
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    let args = Args::parse();

    // let console_layer = console_subscriber::spawn();
    //
    // // `tracing_subscriber::Registry`:
    // tracing_subscriber::registry()
    //     .with(console_layer)
    //     .with(tracing_subscriber::fmt::layer())
    //     .init();

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
    // let mut rng = thread_rng();
    // let dist = Uniform::new(0, 307384);

    for id in 58200..=350000 {
        // let id = dist.sample(&mut rng);
        let id = parser::Id(id);
        let scraper = Arc::clone(&scraper);

        // if !has_mathematician(&*scraper.db_pool, id).await? {
        let task = tokio::spawn(async move { scraper.scrape(id).await });

        // sleep for 1 second
        // let sleep_duration = Duration::from_millis(700);
        // sleep(sleep_duration).await;
        tasks.push(task);
        // }
    }

    for task in tasks {
        let _ = task.await;
    }

    Ok(())
}
