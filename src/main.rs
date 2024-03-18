#![allow(dead_code())]
use scraper::Selector;
use std::fmt::{Debug, Display, Formatter};
use std::fs::read;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
// I mean it's not a very good error type, but it's better than nothing
struct Error {
    message: String,
}

impl Error {
    pub fn new(message: String) -> Self {
        Self { message }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for Error {}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct Mathematician {
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub university: Option<String>,
    pub dissertation: Option<String>,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct Id<T>
where
    T: Debug + PartialEq + Eq + Clone,
{
    pub id: i32,
    pub inner: T,
}

struct MathematicianBuilder {
    first_name: Option<String>,
    last_name: Option<String>,
    university: Option<String>,
    dissertation: Option<String>,
}

impl MathematicianBuilder {
    pub fn new() -> Self {
        Self {
            first_name: None,
            last_name: None,
            university: None,
            dissertation: None,
        }
    }

    pub fn first_name(mut self, first_name: String) -> Self {
        self.first_name = Some(first_name);
        self
    }

    pub fn last_name(mut self, last_name: String) -> Self {
        self.last_name = Some(last_name);
        self
    }

    pub fn university(mut self, university: String) -> Self {
        self.university = Some(university);
        self
    }

    pub fn dissertation(mut self, dissertation: String) -> Self {
        self.dissertation = Some(dissertation);
        self
    }

    pub fn build(self) -> Mathematician {
        Mathematician {
            first_name: self.first_name,
            last_name: self.last_name,
            university: self.university,
            dissertation: self.dissertation,
        }
    }
}

impl Mathematician {
    pub fn new(first_name: String, last_name: String, university: String) -> Self {
        Self {
            first_name: Some(first_name),
            last_name: Some(last_name),
            university: Some(university),
            dissertation: None,
        }
    }
}

fn scrape_students(page: &str) -> color_eyre::Result<Box<[Mathematician]>> {
    let document = scraper::Html::parse_document(page);

    let student_selector = Selector::parse("tbody").expect("student selector is invalid");

    let mut students = document.select(&student_selector);

    // we're not idiots, this is a valid selector
    let rows_selector = Selector::parse("tr").unwrap();
    let cell_selector = Selector::parse("td").unwrap();

    let mut students = students
        .next()
        .ok_or(Error::new("No students found".to_string()))?
        .select(&rows_selector);

    // first row is the header
    let _ = students.next();

    // TODO: filter_map is not exactly the best way to do this
    let students: Vec<_> = students
        .filter_map(|row| {
            let mut cells = row.select(&cell_selector);

            let name = cells.next()?.text().next()?.to_string();

            let mut names = name.split(",");

            // names are in the format of 'surname, first name'
            let last = names.next().unwrap();
            let first = names.last().unwrap();

            let university = cells.next()?.text().next()?.to_string();

            Some(Mathematician::new(
                first.trim().to_string(),
                last.trim().to_string(),
                university,
            ))
        })
        .collect();

    Ok(students.into_boxed_slice())
}
fn scrape_mathematician(page: &str) -> color_eyre::Result<Mathematician> {
    let document = scraper::Html::parse_document(page);

    let name_selector =
        Selector::parse("#paddingWrapper > h2:nth-child(4)").expect("name selector is invalid");

    let uni_selector = Selector::parse(
        "#paddingWrapper > div:nth-child(7) > span:nth-child(1) > span:nth-child(1)",
    )
    .expect("uni selector is invalid");

    let name: String = document
        .select(&name_selector)
        .next()
        .ok_or(Error::new("Name not found".to_string()))?
        .text()
        .next()
        .ok_or(Error::new("Name not found".to_string()))?
        .to_string();

    let university: String = document
        .select(&uni_selector)
        .next()
        .ok_or(Error::new("University not found".to_string()))?
        .text()
        .next()
        .ok_or(Error::new("University not found".to_string()))?
        .to_string();

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
    //     let result = sqlx::query!("
    // INSERT INTO mathematicians(id, first_name, last_name, dissertation, university)
    // VALUES(1, 2, 3, 4, 5)
    // ON CONFLICT(id)
    //     DO UPDATE SET
    //         first_name   = EXCLUDED.first_name,
    //         last_name    = EXCLUDED.last_name,
    //         dissertation = EXCLUDED.dissertation,
    //         university   = EXCLUDED.university;")
    //         .bind(&mathematician.id)
    //         .bind(&mathematician.inner.first_name)
    //         .bind(&mathematician.inner.last_name)
    //         .bind(&mathematician.inner.dissertation)
    //         .bind(&mathematician.inner.university)
    //         .execute(pool)
    //         .await?;

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

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::read;

    #[test]
    fn scrape_rajesh() {
        let page = read("rajesh.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let rajesh = scrape_mathematician(&page).unwrap();

        assert_eq!(rajesh.first_name.unwrap(), "Rajesh");
        assert_eq!(rajesh.last_name.unwrap(), "Pereira");
        assert_eq!(rajesh.university.unwrap(), "University of Toronto");
    }

    #[test]
    fn scrape_rajesh_students() {
        let page = read("rajesh.html").unwrap();
        let page = String::from_utf8(page).unwrap();

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
        assert_eq!(students, expected);
    }

    #[test]
    fn scrape_knuth() {
        let page = read("knuth.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let knuth = scrape_mathematician(&page).unwrap();

        assert_eq!(knuth.first_name.unwrap(), "Donald");
        assert_eq!(knuth.last_name.unwrap(), "Knuth");
        assert_eq!(
            knuth.university.unwrap(),
            "California Institute of Technology"
        );
    }
}

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;

    let postgres_url = std::env::var(&"POSTGRES_URL").expect("POSTGRES_URL is not set");

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&postgres_url)
        .await?;

    let page = read("rajesh.html")?;
    let page = String::from_utf8(page).unwrap();
    let rajesh = scrape_mathematician(&page)?;

    let id = Id {
        id: 92443,
        inner: &rajesh,
    };

    insert_mathematician(&pool, &id).await?;

    Ok(())
}
