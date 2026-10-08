//! Reading and writing crawl state in Postgres, with the same rules as web/worker/crawl.ts:
//! what's due (crawl_schedule, or two weeks after the log's last success for older pages), how a
//! page is stored, and how its recrawl interval adapts.

use std::collections::HashSet;

use sha2::{Digest, Sha256};
use sqlx::PgPool;

use crate::parser::MgpPage;

const MIN_RECRAWL_DAYS: f32 = 3.0;
const MAX_RECRAWL_DAYS: f32 = 365.0;
/// an ID MGP said it doesn't have is asked about again after this long
const NOT_FOUND_RECHECK_DAYS: i32 = 30;

pub struct Store {
    pool: PgPool,
}

/// Pages to leave alone at the start of a run.
pub struct Settled {
    /// crawled recently enough that they aren't due yet
    pub not_due: HashSet<i32>,
    /// MGP said recently that it has no such ID
    pub missing: HashSet<i32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// the starting page of a walk: follow students and advisors
    Both,
    Down,
    Up,
    /// a sweep: no following at all
    Neither,
}

/// The first recrawl interval, before there's any history of changes: someone early in their career
/// or with recent students may still gain students; someone whose last student graduated decades ago won't.
pub fn first_interval(page: &MgpPage, year: i32) -> f32 {
    let latest = page.students.iter().filter_map(|s| s.year).chain(page.year).max().unwrap_or(0);
    match page.year {
        None => 14.0,
        Some(y) if y >= year - 25 || latest >= year - 10 => 14.0,
        _ if latest >= year - 40 => 60.0,
        _ => 180.0,
    }
}

pub fn content_hash(page: &MgpPage) -> String {
    let json = serde_json::to_string(page).expect("pages serialize");
    Sha256::digest(json.as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
}

impl Store {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn settled(&self) -> color_eyre::Result<Settled> {
        let not_due: Vec<i32> = sqlx::query_scalar(
            "select page from crawl_schedule where next_due > now()
             union
             select page_scraped from scrape_logs l
             where result = 'success' and date > now() - interval '14 days'
               and not exists (select 1 from crawl_schedule s where s.page = l.page_scraped)",
        )
        .fetch_all(&self.pool)
        .await?;
        let missing: Vec<i32> = sqlx::query_scalar(
            "select page_scraped from scrape_logs l
             where result = 'failed' and date > now() - make_interval(days => $1)
               and not exists (select 1 from scrape_logs s where s.page_scraped = l.page_scraped and s.result = 'success' and s.date > l.date)",
        )
        .bind(NOT_FOUND_RECHECK_DAYS)
        .fetch_all(&self.pool)
        .await?;
        Ok(Settled { not_due: not_due.into_iter().collect(), missing: missing.into_iter().collect() })
    }

    pub async fn log_missing(&self, id: i32) -> color_eyre::Result<()> {
        sqlx::query("insert into scrape_logs (date, page_scraped, result) values (now(), $1, 'failed')").bind(id).execute(&self.pool).await?;
        Ok(())
    }

    /// Store a page, its advisors and students (as stubs where new), and its next due date, in one
    /// statement. Returns whether the page changed since its last crawl (`None` on a first crawl).
    pub async fn save(&self, page: &MgpPage, year: i32) -> color_eyre::Result<Option<bool>> {
        let id = page.id;
        let mut students: Vec<_> = page.students.iter().filter(|s| s.id != id).collect();
        students.sort_by_key(|s| s.id);
        students.dedup_by_key(|s| s.id);
        let mut advisors: Vec<_> = page.advisors.iter().filter(|a| a.id != id).collect();
        advisors.sort_by_key(|a| a.id);
        advisors.dedup_by_key(|a| a.id);

        // stubs for people known only from this page: students with what the list says, advisors by name
        let mut stubs: Vec<(i32, &str, Option<i32>, Option<&str>)> =
            students.iter().map(|s| (s.id, s.name.as_str(), s.year, s.school.as_deref())).collect();
        stubs.extend(advisors.iter().filter(|a| !students.iter().any(|s| s.id == a.id)).map(|a| (a.id, a.name.as_str(), None, None)));
        stubs.sort_by_key(|s| s.0);
        let mut relations: Vec<(i32, i32)> = advisors.iter().map(|a| (a.id, id)).chain(students.iter().map(|s| (id, s.id))).collect();
        relations.sort();
        let mut schools: Vec<&str> = page.school.as_deref().into_iter().chain(students.iter().filter_map(|s| s.school.as_deref())).collect();
        schools.sort();
        schools.dedup();

        // Every write is a CTE of one statement: one round trip per page. Foreign keys are checked
        // at the end of the statement, so stubs may refer to schools inserted alongside them.
        // Rows go in sorted, so crawls running side by side take their locks in the same order.
        let changed: Option<bool> = sqlx::query_scalar(
            "with
             prev as (select interval_days, content_hash from crawl_schedule where page = $1),
             next as (
               select case when p.content_hash is null then $8::real
                           when p.content_hash = $7 then least($9::real, p.interval_days * 2)
                           else greatest($10::real, p.interval_days / 2) end as days,
                      p.content_hash is not null and p.content_hash <> $7 as changed,
                      p.content_hash is null as first
               from (select 1) one left join prev p on true
             ),
             country as (insert into countries (name) select $6 where $6::text is not null on conflict do nothing),
             school as (insert into schools (name) select s from unnest($11::text[]) s order by 1 on conflict do nothing),
             located as (insert into school_locations (school, country) select $5, $6 where $5::text is not null and $6::text is not null on conflict do nothing),
             person as (
               insert into mathematicians (id, name, dissertation, graduating_year, school) values ($1, $2, $3, $4, $5)
               on conflict (id) do update set name = excluded.name, dissertation = excluded.dissertation,
                 graduating_year = excluded.graduating_year, school = excluded.school
             ),
             stubs as (
               insert into mathematicians (id, name, graduating_year, school)
               select * from unnest($12::int[], $13::text[], $14::int[], $15::text[]) order by 1
               on conflict (id) do nothing
             ),
             links as (insert into advisor_relations (advisor, advisee) select * from unnest($16::int[], $17::int[]) order by 1, 2 on conflict do nothing),
             logged as (insert into scrape_logs (date, page_scraped, result) values (now(), $1, 'success')),
             scheduled as (
               insert into crawl_schedule (page, last_crawled, next_due, interval_days, content_hash, changes)
               select $1, now(), now() + make_interval(secs => days::float8 * 86400), days, $7, changed::int from next
               on conflict (page) do update set last_crawled = excluded.last_crawled, next_due = excluded.next_due,
                 interval_days = excluded.interval_days, content_hash = excluded.content_hash,
                 crawls = crawl_schedule.crawls + 1, changes = crawl_schedule.changes + excluded.changes
             )
             select case when first then null else changed end from next",
        )
        .bind(id)
        .bind(&page.name)
        .bind(&page.dissertation)
        .bind(page.year)
        .bind(&page.school)
        .bind(&page.country)
        .bind(content_hash(page))
        .bind(first_interval(page, year))
        .bind(MAX_RECRAWL_DAYS)
        .bind(MIN_RECRAWL_DAYS)
        .bind(&schools)
        .bind(stubs.iter().map(|s| s.0).collect::<Vec<_>>())
        .bind(stubs.iter().map(|s| s.1).collect::<Vec<_>>())
        .bind(stubs.iter().map(|s| s.2).collect::<Vec<_>>())
        .bind(stubs.iter().map(|s| s.3).collect::<Vec<_>>())
        .bind(relations.iter().map(|r| r.0).collect::<Vec<_>>())
        .bind(relations.iter().map(|r| r.1).collect::<Vec<_>>())
        .fetch_one(&self.pool)
        .await?;
        Ok(changed)
    }

    /// Who to visit next from a page, as the database knows it: students going down, advisors going up.
    pub async fn next_from(&self, id: i32, direction: Direction) -> color_eyre::Result<Vec<(i32, Direction)>> {
        let (down, up) = match direction {
            Direction::Both => (true, true),
            Direction::Down => (true, false),
            Direction::Up => (false, true),
            Direction::Neither => return Ok(vec![]),
        };
        let rows: Vec<(i32, bool)> = sqlx::query_as(
            "select advisee, true from advisor_relations where advisor = $1 and $2
             union all
             select advisor, false from advisor_relations where advisee = $1 and $3",
        )
        .bind(id)
        .bind(down)
        .bind(up)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(page, down)| (page, if down { Direction::Down } else { Direction::Up })).collect())
    }
}
