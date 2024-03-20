#![allow(dead_code)]

mod mathematician;

use color_eyre::eyre::eyre;
use mathematician::Mathematician;
use regex::Regex;
use reqwest::Client;
use scraper::Html;
use scraper::Selector;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Id<T>
where
    T: Debug + PartialEq + Eq + Clone,
{
    pub id: i32,
    pub inner: T,
}

fn scrape_students(page: &Html) -> color_eyre::Result<Box<[(i32, Mathematician)]>> {
    let id_re = Regex::new(r"id\.php\?id=(\d+)").unwrap();

    let student_selector = Selector::parse("table").expect("student selector is invalid");

    let mut students = page.select(&student_selector);

    // we're not idiots, this is a valid selector
    let rows_selector = Selector::parse("tr").unwrap();
    let cell_selector = Selector::parse("td").unwrap();
    let anchor_selector = Selector::parse("a").unwrap();

    let students = students.next();

    let student_row;
    match students {
        None => {
            eprintln!("no students");
            return Ok(Box::new([]));
        }
        Some(stuff) => {
            student_row = stuff;
        }
    }
    let mut students = student_row.select(&rows_selector);

    // first row is the header
    let _ = students.next();

    // TODO: filter_map is not exactly the best way to do this
    let students: Vec<_> = students
        .filter_map(|row| {
            let mut cells = row.select(&cell_selector);

            let name_tag = cells.next()?;

            let href = name_tag
                .select(&anchor_selector)
                .next()
                .unwrap()
                .attr("href")?;

            let id: i32 = id_re.captures(href)?.get(1)?.as_str().parse().unwrap();

            let name = name_tag.text().next()?.to_string();

            let mut names = name.split(",");

            // names are in the format of 'surname, first name'
            let last = names.next().unwrap();
            let first = names.last().unwrap();

            let university = cells.next()?.text().next()?.to_string();

            let mathematician = Mathematician::new(
                first.trim().to_string(),
                last.trim().to_string(),
                university,
            );
            Some((id, mathematician))
        })
        .collect();

    Ok(students.into_boxed_slice())
}
fn scrape_mathematician(page: &Html) -> color_eyre::Result<Mathematician> {
    let name_selector =
        Selector::parse("#paddingWrapper > h2:nth-child(4)").expect("name selector is invalid");

    let uni_selector = Selector::parse(
        "#paddingWrapper > div:nth-child(7) > span:nth-child(1) > span:nth-child(1)",
    )
    .expect("uni selector is invalid");

    let name: String = page
        .select(&name_selector)
        .next()
        .ok_or(eyre!("Name not found"))?
        .text()
        .next()
        .ok_or(eyre!("Name not found"))?
        .to_string();

    // let university: String = page
    //     .select(&uni_selector)
    //     .next()
    //     .ok_or(eyre!("University not found"))?
    //     .text()
    //     .next()
    //     .ok_or(eyre!("University not found"))?
    //     .to_string();

    let university = "".to_string();

    let mut name_iter = name.split_whitespace();
    let first: Option<&str> = name_iter.next();
    let last: Option<&str> = name_iter.last();

    Ok(Mathematician::new(
        first.map_or("", |s| s).to_string(),
        last.map_or("", |s| s).to_string(),
        university,
    ))
}

async fn insert_mathematician(
    pool: &sqlx::Pool<sqlx::Postgres>,
    mathematician: &Id<&Mathematician>,
) -> color_eyre::Result<u64> {
    let result = sqlx::query!(
        "
INSERT INTO mathematicians(id, first_name, last_name, dissertation)
VALUES($1, $2, $3, $4)
ON CONFLICT(id)
    DO UPDATE SET
        first_name   = EXCLUDED.first_name,
        last_name    = EXCLUDED.last_name,
        dissertation = EXCLUDED.dissertation;",
        mathematician.id,
        mathematician.inner.first_name,
        mathematician.inner.last_name,
        mathematician.inner.dissertation
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
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

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::read;

    #[test]
    fn scrape_rajesh() {
        let page = read("rajesh.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let page = Html::parse_document(&page);
        let rajesh = scrape_mathematician(&page).unwrap();

        assert_eq!(rajesh.first_name.unwrap(), "Rajesh");
        assert_eq!(rajesh.last_name.unwrap(), "Pereira");
        assert_eq!(rajesh.university.unwrap(), "University of Toronto");
    }

    #[test]
    fn scrape_rajesh_students() {
        let page = read("rajesh.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let page = Html::parse_document(&page);
        let students = scrape_students(&page).unwrap().into_vec();
        let expected = vec![
            Mathematician::new(
                "George".to_string(),
                "Hutchinson".to_string(),
                "University of Guelph".to_string(),
            ),
            Mathematician::new(
                "Jeremy".to_string(),
                "Levick".to_string(),
                "University of Guelph".to_string(),
            ),
            Mathematician::new(
                "Preeti".to_string(),
                "Mohindru".to_string(),
                "University of Guelph".to_string(),
            ),
            Mathematician::new(
                "Jeffrey".to_string(),
                "Tsang".to_string(),
                "University of Guelph".to_string(),
            ),
        ];
        println!("{students:?}");
        // assert_eq!(students, expected);
    }

    #[test]
    fn scrape_knuth() {
        let page = read("knuth.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let page = Html::parse_document(&page);

        let knuth = scrape_mathematician(&page).unwrap();

        assert_eq!(knuth.first_name.unwrap(), "Donald");
        assert_eq!(knuth.last_name.unwrap(), "Knuth");
        assert_eq!(
            knuth.university.unwrap(),
            "California Institute of Technology"
        );
    }
}

struct Scraper {
    db_pool: Arc<sqlx::Pool<sqlx::Postgres>>,
    client: Client,
}

impl Scraper {
    async fn scrape(&self, id: i32) -> color_eyre::Result<()> {
        // first see if the mathematician already exists
        if has_mathematician(&self.db_pool, id).await? {
            return Ok(());
        }
        println!("{id} requires scrapping");

        // scrape the page
        let url = format!("https://www.mathgenealogy.org/id.php?id={id}");
        let page = self.client.get(&url).send().await?.text().await?;

        println!("scraping {}", url);

        let (advisor, advisees) = {
            let page = Html::parse_document(&page);
            let page = Mutex::new(page);
            let page = page.lock().unwrap();

            let advisor = scrape_mathematician(&*page)?;

            let advisees = scrape_students(&*page)?;

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
    for i in 1..3000 {
        let scraper = Arc::clone(&scraper);
        let task = tokio::spawn(async move { scraper.scrape(i).await });
        tasks.push(task);
    }

    for task in tasks {
        let _ = task.await;
    }

    Ok(())
}
