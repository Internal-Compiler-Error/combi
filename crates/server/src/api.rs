//! JSON API. Every response type derives `TS`, so `cargo test -p combi-server` regenerates
//! the matching TypeScript in `web/src/api/types/` — the web app never hand-writes them.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use ts_rs::TS;

pub const MAX_GRAPH_DEPTH: i32 = 6;
pub const MAX_GRAPH_NODES: usize = 1500;

/// One mathematician as it appears in lists, search results and graphs.
#[derive(Debug, Serialize, TS)]
#[ts(export)]
pub struct Person {
    pub id: i32,
    pub name: String,
    pub year: Option<i32>,
    pub school: Option<String>,
    pub country: Option<String>,
    /// students recorded in the database, which may be fewer than the site lists
    pub student_count: i32,
}

#[derive(Debug, Serialize, TS)]
#[ts(export)]
pub struct PersonDetail {
    #[serde(flatten)]
    pub person: Person,
    pub dissertation: Option<String>,
    pub advisors: Vec<Person>,
    pub students: Vec<Person>,
    /// all descendants reachable in the database, counted once each
    pub descendant_count: i32,
}

#[derive(Debug, Serialize, TS)]
#[ts(export)]
pub struct GraphNode {
    #[serde(flatten)]
    pub person: Person,
    /// generations from the focus: negative for advisors, positive for students
    pub depth: i32,
}

#[derive(Debug, Serialize, TS)]
#[ts(export)]
pub struct GraphLink {
    pub advisor: i32,
    pub student: i32,
}

#[derive(Debug, Serialize, TS)]
#[ts(export)]
pub struct Graph {
    pub focus: i32,
    pub nodes: Vec<GraphNode>,
    pub links: Vec<GraphLink>,
    /// true when the neighbourhood had more than `MAX_GRAPH_NODES` people and the farthest were dropped
    pub truncated: bool,
}

#[derive(Debug, Serialize, TS)]
#[ts(export)]
pub struct Stats {
    pub mathematicians: i32,
    pub relations: i32,
    pub countries: i32,
    pub first_year: Option<i32>,
    pub last_year: Option<i32>,
    /// RFC 3339 time of the most recent successful page scrape
    pub last_scraped: Option<String>,
}

#[derive(Debug, Deserialize, TS)]
#[ts(export)]
pub struct SearchParams {
    pub q: String,
    #[ts(optional)]
    pub limit: Option<i32>,
}

#[derive(Debug, Deserialize, TS)]
#[ts(export)]
pub struct GraphParams {
    /// generations of advisors to include (0–6, default 2)
    #[ts(optional)]
    pub up: Option<i32>,
    /// generations of students to include (0–6, default 2)
    #[ts(optional)]
    pub down: Option<i32>,
}

pub enum ApiError {
    NotFound,
    Db(sqlx::Error),
}

impl From<sqlx::Error> for ApiError {
    fn from(e: sqlx::Error) -> Self {
        ApiError::Db(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        #[derive(Serialize)]
        struct Body {
            error: String,
        }
        let (status, error) = match self {
            ApiError::NotFound => (StatusCode::NOT_FOUND, "No mathematician with that ID is in the database".to_owned()),
            ApiError::Db(e) => {
                tracing::error!("database error: {e}");
                (StatusCode::INTERNAL_SERVER_ERROR, "Database error".to_owned())
            }
        };
        (status, Json(Body { error })).into_response()
    }
}

type ApiResult<T> = Result<Json<T>, ApiError>;

/// The Mathematics Genealogy Project names countries after its flag images ("UnitedStates");
/// put the spaces back for display.
pub fn pretty_country(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() + 4);
    let mut prev_lower = false;
    for c in raw.chars() {
        if c.is_uppercase() && prev_lower {
            out.push(' ');
        }
        prev_lower = c.is_lowercase();
        out.push(c);
    }
    out
}

/// Escape LIKE wildcards and collapse whitespace so user input is matched literally.
fn normalize_query(q: &str) -> String {
    q.split_whitespace()
        .map(|w| w.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_"))
        .collect::<Vec<_>>()
        .join(" ")
}

struct PersonRow {
    id: i32,
    name: Option<String>,
    year: Option<i32>,
    school: Option<String>,
    country: Option<String>,
    student_count: Option<i32>,
}

impl From<PersonRow> for Person {
    fn from(r: PersonRow) -> Self {
        Person {
            id: r.id,
            name: r.name.unwrap_or_else(|| format!("Unknown (ID {})", r.id)),
            year: r.year,
            school: r.school,
            country: r.country.as_deref().map(pretty_country),
            student_count: r.student_count.unwrap_or(0),
        }
    }
}

pub async fn search(State(pool): State<PgPool>, Query(p): Query<SearchParams>) -> ApiResult<Vec<Person>> {
    let q = normalize_query(&p.q);
    if q.is_empty() {
        return Ok(Json(vec![]));
    }
    let limit = p.limit.unwrap_or(20).clamp(1, 100);

    // Words must appear in order ("donald knuth" finds "Donald Ervin Knuth"); word similarity
    // catches typos ("knuht"). An all-digit query also matches the MGP ID exactly.
    // pg_trgm's default threshold of 0.6 misses one-letter typos in short names, so loosen it
    // for this transaction only.
    let mut tx = pool.begin().await?;
    sqlx::query!("set local pg_trgm.word_similarity_threshold = 0.45").execute(&mut *tx).await?;
    let rows = sqlx::query_as!(
        PersonRow,
        r#"
        with q as (select lower(immutable_unaccent($1)) as q)
        select m.id, m.name, m.graduating_year as year, m.school,
               (select string_agg(country, ', ' order by country) from school_locations l where l.school = m.school) as country,
               (select count(*)::int from advisor_relations r where r.advisor = m.id) as student_count
        from mathematicians m, q
        where m.search_name like '%' || replace(q.q, ' ', '%') || '%'
           or q.q <% m.search_name
           or m.id::text = $1
        order by m.id::text = $1 desc,
                 m.search_name like replace(q.q, ' ', '%') || '%' desc,
                 word_similarity(q.q, m.search_name) desc,
                 student_count desc,
                 m.name
        limit $2
        "#,
        q,
        limit as i64
    )
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;

    Ok(Json(rows.into_iter().map(Person::from).collect()))
}

pub async fn person(State(pool): State<PgPool>, Path(id): Path<i32>) -> ApiResult<PersonDetail> {
    let row = sqlx::query!(
        r#"
        select m.id, m.name, m.graduating_year as year, m.school, m.dissertation,
               (select string_agg(country, ', ' order by country) from school_locations l where l.school = m.school) as country,
               (select count(*)::int from advisor_relations r where r.advisor = m.id) as student_count
        from mathematicians m
        where m.id = $1
        "#,
        id
    )
    .fetch_optional(&pool)
    .await?
    .ok_or(ApiError::NotFound)?;

    let advisors = sqlx::query_as!(
        PersonRow,
        r#"
        select m.id, m.name, m.graduating_year as year, m.school,
               (select string_agg(country, ', ' order by country) from school_locations l where l.school = m.school) as country,
               (select count(*)::int from advisor_relations r2 where r2.advisor = m.id) as student_count
        from advisor_relations r join mathematicians m on m.id = r.advisor
        where r.advisee = $1
        order by m.graduating_year nulls last, m.name
        "#,
        id
    )
    .fetch_all(&pool)
    .await?;

    let students = sqlx::query_as!(
        PersonRow,
        r#"
        select m.id, m.name, m.graduating_year as year, m.school,
               (select string_agg(country, ', ' order by country) from school_locations l where l.school = m.school) as country,
               (select count(*)::int from advisor_relations r2 where r2.advisor = m.id) as student_count
        from advisor_relations r join mathematicians m on m.id = r.advisee
        where r.advisor = $1
        order by m.graduating_year nulls last, m.name
        "#,
        id
    )
    .fetch_all(&pool)
    .await?;

    let descendant_count = sqlx::query_scalar!(
        r#"
        with recursive d(id) as (
            select advisee from advisor_relations where advisor = $1
            union
            select r.advisee from advisor_relations r join d on r.advisor = d.id
        )
        select count(*)::int as "count!" from d
        "#,
        id
    )
    .fetch_one(&pool)
    .await?;

    Ok(Json(PersonDetail {
        person: PersonRow {
            id: row.id,
            name: row.name,
            year: row.year,
            school: row.school,
            country: row.country,
            student_count: row.student_count,
        }
        .into(),
        dissertation: row.dissertation.map(|d| d.trim().to_owned()).filter(|d| !d.is_empty()),
        advisors: advisors.into_iter().map(Person::from).collect(),
        students: students.into_iter().map(Person::from).collect(),
        descendant_count,
    }))
}

pub async fn graph(State(pool): State<PgPool>, Path(id): Path<i32>, Query(p): Query<GraphParams>) -> ApiResult<Graph> {
    let up = p.up.unwrap_or(2).clamp(0, MAX_GRAPH_DEPTH);
    let down = p.down.unwrap_or(2).clamp(0, MAX_GRAPH_DEPTH);

    let exists = sqlx::query_scalar!("select exists(select 1 from mathematicians where id = $1) as \"e!\"", id)
        .fetch_one(&pool)
        .await?;
    if !exists {
        return Err(ApiError::NotFound);
    }

    // Walk students downward and advisors upward, keeping each person at their nearest
    // generation. Fetch one extra row so we know whether the cap cut anything off.
    let rows = sqlx::query!(
        r#"
        with recursive
        down(id, depth) as (
            select $1::int, 0
            union
            select r.advisee, d.depth + 1 from advisor_relations r join down d on r.advisor = d.id where d.depth < $3
        ),
        up(id, depth) as (
            select $1::int, 0
            union
            select r.advisor, u.depth - 1 from advisor_relations r join up u on r.advisee = u.id where u.depth > -($2::int)
        ),
        nearest as (
            select id, (array_agg(depth order by abs(depth), depth))[1] as depth
            from (select * from down union all select * from up) both_ways
            group by id
        )
        select m.id, m.name, m.graduating_year as year, m.school,
               (select string_agg(country, ', ' order by country) from school_locations l where l.school = m.school) as country,
               (select count(*)::int from advisor_relations r where r.advisor = m.id) as student_count,
               n.depth as "depth!"
        from nearest n join mathematicians m on m.id = n.id
        order by abs(n.depth), n.depth, m.graduating_year nulls last, m.id
        limit $4
        "#,
        id,
        up,
        down,
        MAX_GRAPH_NODES as i64 + 1
    )
    .fetch_all(&pool)
    .await?;

    let truncated = rows.len() > MAX_GRAPH_NODES;
    let nodes: Vec<GraphNode> = rows
        .into_iter()
        .take(MAX_GRAPH_NODES)
        .map(|r| GraphNode {
            depth: r.depth,
            person: PersonRow {
                id: r.id,
                name: r.name,
                year: r.year,
                school: r.school,
                country: r.country,
                student_count: r.student_count,
            }
            .into(),
        })
        .collect();

    let ids: Vec<i32> = nodes.iter().map(|n| n.person.id).collect();
    let links = sqlx::query_as!(
        GraphLink,
        r#"
        select advisor, advisee as student
        from advisor_relations
        where advisor = any($1) and advisee = any($1)
        "#,
        &ids
    )
    .fetch_all(&pool)
    .await?;

    Ok(Json(Graph { focus: id, nodes, links, truncated }))
}

pub async fn stats(State(pool): State<PgPool>) -> ApiResult<Stats> {
    let s = sqlx::query!(
        r#"
        select (select count(*)::int from mathematicians) as "mathematicians!",
               (select count(*)::int from advisor_relations) as "relations!",
               (select count(*)::int from countries) as "countries!",
               (select min(graduating_year) from mathematicians) as first_year,
               (select max(graduating_year) from mathematicians) as last_year,
               (select max(date) from scrape_logs where result = 'success') as last_scraped
        "#
    )
    .fetch_one(&pool)
    .await?;

    Ok(Json(Stats {
        mathematicians: s.mathematicians,
        relations: s.relations,
        countries: s.countries,
        first_year: s.first_year,
        last_year: s.last_year,
        last_scraped: s.last_scraped.map(|d| d.to_rfc3339()),
    }))
}

/// The advisors with the most students on record, as starting points for browsing.
pub async fn notable(State(pool): State<PgPool>) -> ApiResult<Vec<Person>> {
    let rows = sqlx::query_as!(
        PersonRow,
        r#"
        select m.id, m.name, m.graduating_year as year, m.school,
               (select string_agg(country, ', ' order by country) from school_locations l where l.school = m.school) as country,
               c.n as student_count
        from (select advisor, count(*)::int as n from advisor_relations group by advisor order by n desc, advisor limit 12) c
        join mathematicians m on m.id = c.advisor
        order by c.n desc, m.name
        "#
    )
    .fetch_all(&pool)
    .await?;

    Ok(Json(rows.into_iter().map(Person::from).collect()))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn pretty_country_splits_words() {
        assert_eq!(pretty_country("UnitedStates"), "United States");
        assert_eq!(pretty_country("HongKong"), "Hong Kong");
        assert_eq!(pretty_country("Canada"), "Canada");
    }

    #[test]
    fn normalize_query_escapes_wildcards() {
        assert_eq!(normalize_query("  50%   off_by\\one "), "50\\% off\\_by\\\\one");
    }
}
