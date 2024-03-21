use crate::mathematician::Country;
use crate::mathematician::Dissertation;
use crate::mathematician::GraduationRecord;
use crate::mathematician::Mathematician;
use crate::mathematician::School;
use color_eyre::eyre::eyre;
use lazy_static::lazy_static;
use regex::Regex;
use scraper::Html;
use scraper::Selector;
use sqlx::prelude::FromRow;
use tracing::debug;
use tracing::instrument;
use tracing::warn;

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
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, FromRow)]
pub struct ScrapeRecord {
    pub mathematician: Mathematician,
    pub students_ids: Vec<i32>,
    pub dissertation: Option<Dissertation>,
    pub graduation_record: Option<GraduationRecord>,
}

pub fn scrape(page: &Html) -> color_eyre::Result<ScrapeRecord> {
    let mathematician = scrape_mathematician(page)?;
    let dissertation_title = scrape_dissertation(page);
    let student_ids = scrape_students(page)?;

    let university = parse_school(page);
    let year = parse_year(page);
    let graduation_record = match (university, year) {
        (Some((school, country)), Some(year)) => Some(GraduationRecord {
            mathematician: mathematician.clone(),
            country,
            school,
            year,
        }),
        _ => None,
    };

    Ok(ScrapeRecord {
        mathematician: mathematician.clone(),
        students_ids: student_ids,
        dissertation: dissertation_title.map(|t| Dissertation {
            title: t.to_string(),
            author: mathematician,
        }),
        graduation_record,
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

pub fn scrape_students(page: &Html) -> color_eyre::Result<Vec<i32>> {
    let id_re = Regex::new(r"id\.php\?id=(\d+)").unwrap();

    let student_selector = Selector::parse("table").expect("student selector is invalid");

    let mut students = page.select(&student_selector);

    let students = students.next();

    let student_row;
    match students {
        None => {
            debug!("no students");
            return Ok(vec![]);
        }
        Some(stuff) => {
            student_row = stuff;
        }
    }
    let mut students = student_row.select(&ROWS_SELECTOR);

    // first row is the header
    let _ = students.next();

    // TODO: filter_map is not exactly the best way to do this
    let students: Vec<_> = students
        .filter_map(|row| {
            let mut cells = row.select(&CELL_SELECTOR);

            let name_tag = cells.next()?;

            let href = name_tag
                .select(&ANCHOR_SELECTOR)
                .next()
                .unwrap()
                .attr("href")?;

            id_re.captures(href)?.get(1)?.as_str().parse().ok()

            // let name = name_tag.text().next()?;
            //
            // let name = parse_name(name);
            //
            // let university = cells.next()?.text().next()?.to_string();
            //
            // let mut builder = MathematicianBuilder::new();
            // builder.full_name(name).school(School {
            //     name: university,
            //     country: None,
            // });
            //
            // let mathematician = builder.build();

            // Some((id, mathematician))
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

#[instrument]
pub fn scrape_mathematician(page: &Html) -> color_eyre::Result<Mathematician> {
    let full_name = page
        .select(&NAME)
        .next()
        .ok_or(eyre!("Name not found"))?
        .text()
        .next()
        .ok_or(eyre!("Name not found"))?
        .trim()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    // builder.full_name(full_name);
    //
    // let school = parse_school(page);
    // if let Some(school) = school {
    //     builder.school(school);
    // }
    //
    // let thesis = scrape_dissertation(page);
    // if let Some(thesis) = thesis {
    //     builder.dissertation(thesis.to_string());
    // }

    // let year = parse_year(page);
    Ok(Mathematician {
        id: 0,
        name: full_name,
        // year,
    })
}

fn parse_country(page: &Html) -> Option<&str> {
    let country = page.select(&COUNTRY_SELECTOR).next()?;
    let country = country.value().attr("alt")?;
    Some(country)
}

fn parse_school(page: &Html) -> Option<(School, Option<Country>)> {
    // the university is next to the the span that contains 'Ph.D. ' (yes they have a stupid space
    // in there)
    let name = page
        .select(&DIV_SPAN)
        .next()?
        .text()
        .skip_while(|node| node.trim() != "Ph.D.")
        .skip(1)
        .next()?
        .trim();

    let country = parse_country(page).map(|c| Country {
        name: c.to_string(),
    });

    Some((
        School {
            name: name.to_string(),
        },
        country,
    ))
}

fn parse_year(page: &Html) -> Option<u16> {
    let phd_section = page.select(&DIV_SPAN).next()?;
    let texts = phd_section.text();

    texts
        .map(|t| t.trim())
        .filter_map(|t| t.parse::<u16>().ok())
        .next()
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs::read;

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
        let rajesh = scrape_mathematician(&page).unwrap();

        assert_eq!(rajesh.name, "Rajesh Pereira");
        assert_eq!(
            rajesh.school.unwrap(),
            School {
                name: "University of Toronto".to_string(),
                country: Some("Canada".to_string())
            }
        );
        assert_eq!(
            rajesh.dissertation,
            Some("Trace Vectors in Matrix Analysis".to_string())
        );
    }

    #[test]
    fn scrape_abu() {
        let page = read("abu.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let page = Html::parse_document(&page);
        let abu = scrape_mathematician(&page).unwrap();

        let expected = Mathematician {
            name: "Abu Sahl 'Isa ibn Yahya al-Masihi".to_string(),
            dissertation: None,
            school: None,
            year: None,
        };

        assert_eq!(abu, expected);
    }

    #[test]
    fn scrape_rajesh_students() {
        let page = read("rajesh.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let page = Html::parse_document(&page);
        let students = scrape_students(&page).unwrap().into_vec();

        let geroge = {
            let mut builder = MathematicianBuilder::new();
            builder
                .full_name("George Hutchinson".to_string())
                .school(School {
                    name: "University of Guelph".to_string(),
                    country: None,
                });
            builder.build()
        };

        let jeremy = {
            let mut builder = MathematicianBuilder::new();
            builder
                .full_name("Jeremy Levick".to_string())
                .school(School {
                    name: "University of Guelph".to_string(),
                    country: None,
                });
            builder.build()
        };

        let preeti = {
            let mut builder = MathematicianBuilder::new();
            builder
                .full_name("Preeti Mohindru".to_string())
                .school(School {
                    name: "University of Guelph".to_string(),
                    country: None,
                });
            builder.build()
        };

        let jeffrey = {
            let mut builder = MathematicianBuilder::new();
            builder
                .full_name("Jeffrey Tsang".to_string())
                .school(School {
                    name: "University of Guelph".to_string(),
                    country: None,
                });
            builder.build()
        };

        let expected = vec![geroge, jeremy, preeti, jeffrey];

        let students = students
            .iter()
            .map(|(_, student)| student)
            .collect::<Vec<_>>();

        for (student, expected) in students.iter().zip(expected.iter()) {
            assert_eq!(student.name, expected.name);
            assert_eq!(student.school, expected.school);
        }
    }

    #[test]
    fn scrape_knuth() {
        let page = read("knuth.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let page = Html::parse_document(&page);

        let knuth = scrape_mathematician(&page).unwrap();

        assert_eq!(knuth.name, "Donald Ervin Knuth");
        assert_eq!(
            knuth.school.unwrap(),
            School {
                name: "California Institute of Technology".to_string(),
                country: Some("UnitedStates".to_string()),
            }
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
        assert_eq!(
            uni,
            School {
                name: "California Institute of Technology".to_string(),
                country: Some("UnitedStates".to_string()),
            }
        );
    }

    #[test]
    fn parse_uni_works_for_rajesh() {
        let page = read("rajesh.html").unwrap();
        let page = String::from_utf8(page).unwrap();
        let page = Html::parse_document(&page);

        let uni = parse_school(&page).unwrap();
        assert_eq!(
            uni,
            School {
                name: "University of Toronto".to_string(),
                country: Some("Canada".to_string()),
            }
        );
    }
}
