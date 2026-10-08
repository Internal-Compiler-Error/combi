//! Reads an MGP person page. This mirrors `parsePage` in web/worker/crawl.ts field for field, so
//! both crawlers store the same thing and hash it the same way (see the golden-file test below).

use std::sync::LazyLock;

use regex::Regex;
use scraper::{ElementRef, Html, Node, Selector};
use serde::Serialize;

static ID_IN_HREF: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"id\.php\?id=(\d+)").unwrap());
static NOT_FOUND: &str = "You have specified an ID that does not exist in the database. Please back up and try again.";

fn selector(s: &str) -> Selector {
    Selector::parse(s).unwrap()
}
static P: LazyLock<Selector> = LazyLock::new(|| selector("p"));
static H2: LazyLock<Selector> = LazyLock::new(|| selector("h2"));
static DEGREE: LazyLock<Selector> = LazyLock::new(|| selector("div > span"));
static SPAN: LazyLock<Selector> = LazyLock::new(|| selector("span"));
static FLAG: LazyLock<Selector> = LazyLock::new(|| selector("div > img"));
static THESIS: LazyLock<Selector> = LazyLock::new(|| selector("#thesisTitle"));
static TABLE: LazyLock<Selector> = LazyLock::new(|| selector("table"));
static TR: LazyLock<Selector> = LazyLock::new(|| selector("tr"));
static TD: LazyLock<Selector> = LazyLock::new(|| selector("td"));
static A: LazyLock<Selector> = LazyLock::new(|| selector("a"));

/// A person as named on someone else's page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Ref {
    pub id: i32,
    pub name: String,
}

/// A student as listed on their advisor's page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Student {
    pub id: i32,
    pub name: String,
    pub school: Option<String>,
    pub year: Option<i32>,
}

/// What one MGP page says about a person. Field order matters: it is the JSON that gets hashed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MgpPage {
    pub id: i32,
    pub name: String,
    pub dissertation: Option<String>,
    pub school: Option<String>,
    /// as MGP names its flag images: "UnitedStates"
    pub country: Option<String>,
    pub year: Option<i32>,
    pub advisors: Vec<Ref>,
    pub students: Vec<Student>,
}

fn squash(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn or_none(s: Option<String>) -> Option<String> {
    s.map(|s| squash(&s)).filter(|s| !s.is_empty())
}

fn text(e: ElementRef) -> String {
    e.text().collect()
}

/// MGP has typos like a degree "200" from a university founded in 1989; no degree on it predates 1000.
fn plausible_year(s: &str) -> Option<i32> {
    let max = chrono::Utc::now().format("%Y").to_string().parse::<i32>().unwrap() + 1;
    s.trim().parse::<i32>().ok().filter(|y| (1000..=max).contains(y))
}

fn href_id(a: ElementRef) -> Option<i32> {
    ID_IN_HREF.captures(a.value().attr("href")?)?.get(1)?.as_str().parse().ok()
}

/// Student tables list "Hall, Jr., Marshall"; the person's own page says "Marshall Hall, Jr.".
fn unsurname(listed: &str) -> String {
    let mut parts = listed.split(',').map(squash);
    let surname = parts.next().unwrap_or_default();
    let rest: Vec<String> = parts.collect();
    if rest.is_empty() { surname } else { [rest.join(" "), surname].join(" ") }
}

/// Student tables and search results share a row layout: linked name, school, year.
fn listed_person(row: ElementRef) -> Option<Student> {
    let mut cells = row.select(&TD);
    let name_cell = cells.next()?;
    let a = name_cell.select(&A).next()?;
    let id = href_id(a)?;
    let school = or_none(cells.next().map(text));
    let year = cells.next().and_then(|c| plausible_year(&text(c)));
    Some(Student { id, name: unsurname(&text(a)), school, year })
}

/// `None` when MGP has no one with this ID.
pub fn parse_page(id: i32, html: &str) -> color_eyre::Result<Option<MgpPage>> {
    let doc = Html::parse_document(html);
    if doc.select(&P).any(|p| text(p).trim() == NOT_FOUND) {
        return Ok(None);
    }
    let name = or_none(doc.select(&H2).next().map(text)).ok_or_else(|| color_eyre::eyre::eyre!("MGP page {id} has no name"))?;

    // <span>Ph.D. <span>School</span>1963</span>: the year is a direct text child
    let degree = doc.select(&DEGREE).next();
    let year = degree.and_then(|d| {
        d.children()
            .filter_map(|n| match n.value() {
                Node::Text(t) => Some(t.trim().to_string()),
                _ => None,
            })
            .find(|t| !t.is_empty() && t.chars().all(|c| c.is_ascii_digit()))
            .and_then(|t| plausible_year(&t))
    });

    let advisors = doc
        .select(&P)
        .filter(|p| text(*p).trim().starts_with("Advisor"))
        .flat_map(|p| p.select(&A).filter_map(|a| Some(Ref { id: href_id(a)?, name: squash(&text(a)) })).collect::<Vec<_>>())
        .collect();

    // the header row has no cells, so it drops out
    let students = doc.select(&TABLE).next().map(|t| t.select(&TR).filter_map(listed_person).collect()).unwrap_or_default();

    Ok(Some(MgpPage {
        id,
        name,
        dissertation: or_none(doc.select(&THESIS).next().map(text)),
        school: or_none(degree.and_then(|d| d.select(&SPAN).next()).map(text)),
        country: or_none(doc.select(&FLAG).next().and_then(|i| i.value().attr("alt")).map(str::to_string)),
        year,
        advisors,
        students,
    }))
}

#[cfg(test)]
mod test {
    use super::*;

    // the saved pages are shared with the Worker's tests
    fn fixture(name: &str) -> String {
        std::fs::read_to_string(format!("{}/../../web/worker/test/fixtures/mgp/{name}", env!("CARGO_MANIFEST_DIR"))).unwrap()
    }

    /// Byte-for-byte the JSON the Worker's parser produces, so both crawlers hash pages alike.
    #[test]
    fn matches_the_worker_parser() {
        for (id, name) in [(10416, "knuth"), (135101, "rajesh"), (1, "Tai-Yih"), (2, "abu")] {
            let page = parse_page(id, &fixture(&format!("{name}.html"))).unwrap().unwrap();
            let golden = fixture(&format!("{name}.parsed.json"));
            assert_eq!(serde_json::to_string(&page).unwrap(), golden.trim_end(), "{name}");
        }
    }

    #[test]
    fn knuth() {
        let knuth = parse_page(10416, &fixture("knuth.html")).unwrap().unwrap();
        assert_eq!(knuth.name, "Donald Ervin Knuth");
        assert_eq!(knuth.year, Some(1963));
        assert_eq!(knuth.advisors, vec![Ref { id: 6807, name: "Marshall Hall, Jr.".into() }]);
        assert_eq!(
            knuth.students[0],
            Student { id: 61940, name: "Bruce Baumgart".into(), school: Some("Stanford University".into()), year: Some(1974) }
        );
    }

    #[test]
    fn missing_id() {
        let html = format!("<html><body><p>{NOT_FOUND}</p></body></html>");
        assert_eq!(parse_page(1, &html).unwrap(), None);
    }

    #[test]
    fn implausible_year() {
        let html = fixture("knuth.html").replace(">1963</span>", ">200</span>");
        assert_eq!(parse_page(1, &html).unwrap().unwrap().year, None);
    }
}
