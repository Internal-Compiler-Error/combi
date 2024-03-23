use color_eyre::eyre::eyre;
use lazy_static::lazy_static;
use regex::Regex;
use scraper::Html;
use scraper::Selector;
use sqlx::prelude::FromRow;
use tracing::debug;

lazy_static! {
    static ref ID_RE: Regex = Regex::new(r"id\.php\?id=(\d+)").unwrap();
    static ref NAME: Selector = Selector::parse("h2").unwrap();
    static ref DIV_SPAN: Selector = Selector::parse("div > span").unwrap();
    static ref SPAN: Selector = Selector::parse("span").unwrap();
    static ref ROWS_SELECTOR: Selector = Selector::parse("tr").unwrap();
    static ref CELL_SELECTOR: Selector = Selector::parse("td").unwrap();
    static ref ANCHOR_SELECTOR: Selector = Selector::parse("a").unwrap();
    static ref THESIS_SELECTOR: Selector = Selector::parse("#thesisTitle").unwrap();
    static ref COUNTRY_SELECTOR: Selector = Selector::parse("div > img").unwrap();
    static ref TABLE_SECTOR: Selector = Selector::parse("table").unwrap();
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow, Copy, sqlx::Type)]
pub struct Id(pub i32);

impl Into<i32> for Id {
    fn into(self) -> i32 {
        self.0
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
/// A record of a mathematician and their students
pub struct ScrapeRecord {
    /// The name of the main mathematician
    pub name: String,

    /// A list of studetns mentored under the main mathematician
    pub students: Vec<Student>,

    /// The title of dissertation of the main mathematician
    pub dissertation: Option<String>,

    /// The university or equivalent institution where the main mathematician graduated
    pub school: Option<String>,

    /// The country where the main mathematician graduated
    pub country: Option<String>,

    /// The year when the main mathematician graduated
    pub year: Option<i16>,

    /// The title of the degree, such as "Ph.D."
    pub degree: Option<String>,
}

/// A student of a mathematician
#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct Student {
    /// The name of the student
    pub name: String,

    /// The id of the student as stored in the mathgenealogy database
    pub id: Option<Id>,

    /// The school where the student graduated
    pub school: Option<String>,

    /// The year when the student graduated
    pub year: Option<i16>,
}

pub fn scrape(page: &Html) -> color_eyre::Result<ScrapeRecord> {
    let mathematician = scrape_mathematician(page)?;
    let dissertation = scrape_dissertation(page);
    let students = scrape_students(page)?;

    let university = parse_school(page);
    let year = parse_year(page);
    let country = parse_country(page);
    let degree = parse_title(page);

    Ok(ScrapeRecord {
        name: mathematician,
        students,
        dissertation: dissertation.map(|d| d.to_string()),
        school: university.map(|s| s.to_string()),
        country: country.map(|c| c.to_string()),
        year,
        degree: degree.map(|d| d.to_string()),
    })
}

pub fn scrape_dissertation(page: &Html) -> Option<&str> {
    let thesis = page.select(&THESIS_SELECTOR).next()?;
    let thesis = thesis.text().next()?;

    match thesis.trim() {
        "" => None,
        t => Some(t),
    }
}

pub fn scrape_students(page: &Html) -> color_eyre::Result<Vec<Student>> {
    let students = page.select(&TABLE_SECTOR).next();

    let entries = match students {
        None => {
            debug!("no students");
            return Ok(vec![]);
        }
        Some(x) => x,
    };
    let mut students = entries.select(&ROWS_SELECTOR).skip(1); // first row is the header

    let students: Vec<_> = students
        .filter_map(|row| {
            let mut cells = row.select(&CELL_SELECTOR);

            let name = cells.next()?;

            let href = name.select(&ANCHOR_SELECTOR).next()?.attr("href")?;
            let id: Option<Id> = ID_RE.captures(href)?.get(1)?.as_str().parse().ok().map(Id);

            let name = parse_name(name.text().next()?);

            fn school(cell: &scraper::ElementRef) -> Option<String> {
                Some(cell.text().next()?.trim().to_string())
            }
            let school = school(&cells.next()?);

            let year: Option<i16> = cells
                .next()
                .and_then(|cell| cell.text().next()?.trim().parse().ok());

            Some(Student {
                name,
                id,
                school,
                year,
            })
        })
        .collect();

    Ok(students)
}

fn parse_name(name: &str) -> String {
    let mut full = String::new();
    let mut parts = name.split(",");

    let surname = parts.next();
    if surname.is_none() {
        return name.to_string();
    }

    let surname = surname.unwrap();

    for part in parts {
        full.push_str(part.trim());
        full.push(' ');
    }
    full.push_str(surname.trim());

    full
}

pub fn scrape_mathematician(page: &Html) -> color_eyre::Result<String> {
    Ok(page
        .select(&NAME)
        .next()
        .ok_or(eyre!("Name not found"))?
        .text()
        .next()
        .ok_or(eyre!("Name not found"))?
        .trim()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" "))
}

fn parse_country(page: &Html) -> Option<&str> {
    let country = page.select(&COUNTRY_SELECTOR).next()?;
    let country = country.value().attr("alt")?;
    Some(country)
}

fn parse_title(page: &Html) -> Option<&str> {
    page.select(&DIV_SPAN).next()?.text().next()
}

fn parse_school(page: &Html) -> Option<&str> {
    Some(page.select(&DIV_SPAN).next()?.text().skip(1).next()?.trim())
}

fn parse_year(page: &Html) -> Option<i16> {
    let phd_section = page.select(&DIV_SPAN).next()?;
    let texts = phd_section.text();

    texts
        .map(|t| t.trim())
        .filter_map(|t| t.parse::<i16>().ok())
        .next()
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::read;

    #[test]
    fn parse_name_works_for_tai() {
        let page = read("Tai-Yih.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let page = Html::parse_document(&page);
        let name = scrape_mathematician(&page).unwrap();
        assert_eq!(name, "Tai-Yih Tso");
    }

    #[test]
    fn parse_year_works_for_knuth() {
        let page = read("knuth.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let page = Html::parse_document(&page);
        let year = parse_year(&page).unwrap();
        assert_eq!(year, 1963);
    }

    #[test]
    fn parse_year_works_for_rajesh() {
        let page = read("rajesh.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let page = Html::parse_document(&page);
        let year = parse_year(&page).unwrap();
        assert_eq!(year, 2003);
    }

    #[test]
    fn parse_country_works_for_knuth() {
        let page = read("knuth.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let page = Html::parse_document(&page);
        let country = parse_country(&page).unwrap();

        // it's stupid, I know...
        assert_eq!(country, "UnitedStates");
    }

    #[test]
    fn parse_country_works_for_rajesh() {
        let page = read("rajesh.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let page = Html::parse_document(&page);
        let country = parse_country(&page).unwrap();

        assert_eq!(country, "Canada");
    }

    #[test]
    fn scrape_rajesh() {
        let page = read("rajesh.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let page = Html::parse_document(&page);
        let rajesh = scrape(&page).unwrap();

        assert_eq!(rajesh.name, "Rajesh Pereira");
        assert_eq!(rajesh.school, Some("University of Toronto".to_string()));
        assert_eq!(
            rajesh.dissertation,
            Some("Trace Vectors in Matrix Analysis".to_string())
        );
        assert_eq!(rajesh.country, Some("Canada".to_string()));
    }

    #[test]
    fn scrape_abu() {
        let page = read("abu.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let page = Html::parse_document(&page);
        let abu = scrape(&page).unwrap();

        assert_eq!(abu.name, "Abu Sahl 'Isa ibn Yahya al-Masihi");
        assert_eq!(abu.dissertation, None);
    }

    #[test]
    fn scrape_rajesh_students() {
        let page = read("rajesh.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let page = Html::parse_document(&page);
        let students = scrape_students(&page).unwrap();

        let expected = vec![
            Student {
                name: "George Hutchinson".to_string(),
                id: Some(Id(235835)),
                school: Some("University of Guelph".to_string()),
                year: Some(2018),
            },
            Student {
                name: "Jeremy Levick".to_string(),
                id: Some(Id(197636)),
                school: Some("University of Guelph".to_string()),
                year: Some(2015),
            },
            Student {
                name: "Preeti Mohindru".to_string(),
                id: Some(Id(190371)),
                school: Some("University of Guelph".to_string()),
                year: Some(2014),
            },
            Student {
                name: "Jeffrey Tsang".to_string(),
                id: Some(Id(190372)),
                school: Some("University of Guelph".to_string()),
                year: Some(2014),
            },
        ];

        for (student, expected) in students.iter().zip(expected.iter()) {
            assert_eq!(student, expected)
        }
    }

    #[test]
    fn scrape_knuth() {
        let page = read("knuth.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let page = Html::parse_document(&page);

        let knuth = scrape(&page).unwrap();

        assert_eq!(knuth.name, "Donald Ervin Knuth");
        assert_eq!(
            knuth.school,
            Some("California Institute of Technology".to_string())
        );
        assert_eq!(
            knuth.dissertation,
            Some("Finite Semifields and Projective Planes".to_string())
        );
    }

    #[test]
    fn parse_uni_works_for_knuth() {
        let page = read("knuth.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let page = Html::parse_document(&page);

        let uni = parse_school(&page).unwrap();
        assert_eq!(uni, "California Institute of Technology".to_string(),);
    }

    #[test]
    fn parse_uni_works_for_rajesh() {
        let page = read("rajesh.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let page = Html::parse_document(&page);

        let uni = parse_school(&page).unwrap();
        assert_eq!(uni, "University of Toronto");
    }
}
