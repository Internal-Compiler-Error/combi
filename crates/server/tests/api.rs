//! Each test gets a fresh database with every migration applied and `fixtures/family.sql` loaded.

use axum_test::TestServer;
use serde_json::{Value, json};
use sqlx::PgPool;

fn server(pool: PgPool) -> TestServer {
    TestServer::new(combi_server::router(pool, None))
}

async fn search(pool: PgPool, q: &str) -> Vec<i64> {
    let res = server(pool).get("/api/search").add_query_param("q", q).await;
    res.assert_status_ok();
    res.json::<Vec<Value>>().iter().map(|p| p["id"].as_i64().unwrap()).collect()
}

#[sqlx::test(migrations = "../../migrations", fixtures("family"))]
async fn search_ignores_accents_and_case(pool: PgPool) {
    assert_eq!(search(pool.clone(), "GODEL").await, [6]);
    assert_eq!(search(pool, "gauss").await, [1]);
}

#[sqlx::test(migrations = "../../migrations", fixtures("family"))]
async fn search_skips_middle_names(pool: PgPool) {
    assert_eq!(search(pool, "felix klein").await, [4]);
}

#[sqlx::test(migrations = "../../migrations", fixtures("family"))]
async fn search_tolerates_typos(pool: PgPool) {
    assert_eq!(search(pool, "lipshitz").await, [5]);
}

#[sqlx::test(migrations = "../../migrations", fixtures("family"))]
async fn search_by_id_ranks_exact_id_first(pool: PgPool) {
    assert_eq!(search(pool, "4").await.first(), Some(&4));
}

#[sqlx::test(migrations = "../../migrations", fixtures("family"))]
async fn search_treats_wildcards_literally(pool: PgPool) {
    assert!(search(pool.clone(), "%").await.is_empty());
    assert!(search(pool, "   ").await.is_empty());
}

#[sqlx::test(migrations = "../../migrations", fixtures("family"))]
async fn person_lists_advisors_students_and_descendants(pool: PgPool) {
    let res = server(pool).get("/api/mathematicians/4").await;
    res.assert_status_ok();
    let p: Value = res.json();
    assert_eq!(p["name"], "C. Felix Klein");
    assert_eq!(p["country"], "Germany");
    assert_eq!(p["student_count"], 1);
    assert_eq!(p["descendant_count"], 1);
    let advisors: Vec<_> = p["advisors"].as_array().unwrap().iter().map(|a| a["id"].clone()).collect();
    assert_eq!(advisors, [json!(3), json!(5)]);
    assert_eq!(p["students"][0]["id"], 7);
}

#[sqlx::test(migrations = "../../migrations", fixtures("family"))]
async fn person_counts_each_descendant_once(pool: PgPool) {
    let p: Value = server(pool).get("/api/mathematicians/1").await.json();
    assert_eq!(p["descendant_count"], 4);
}

#[sqlx::test(migrations = "../../migrations", fixtures("family"))]
async fn blank_dissertation_is_null(pool: PgPool) {
    let p: Value = server(pool).get("/api/mathematicians/5").await.json();
    assert_eq!(p["dissertation"], Value::Null);
}

#[sqlx::test(migrations = "../../migrations", fixtures("family"))]
async fn unknown_person_is_404(pool: PgPool) {
    let s = server(pool);
    s.get("/api/mathematicians/999").await.assert_status_not_found();
    s.get("/api/mathematicians/999/graph").await.assert_status_not_found();
}

#[sqlx::test(migrations = "../../migrations", fixtures("family"))]
async fn graph_walks_the_requested_generations(pool: PgPool) {
    let res = server(pool).get("/api/mathematicians/3/graph").add_query_param("up", 1).add_query_param("down", 1).await;
    res.assert_status_ok();
    let g: Value = res.json();

    let mut nodes: Vec<(i64, i64)> = g["nodes"].as_array().unwrap().iter()
        .map(|n| (n["id"].as_i64().unwrap(), n["depth"].as_i64().unwrap()))
        .collect();
    nodes.sort();
    // Lipschitz (5) advises Klein but is not an ancestor of Plücker, so he is not walked
    assert_eq!(nodes, [(2, -1), (3, 0), (4, 1)]);

    let mut links: Vec<(i64, i64)> = g["links"].as_array().unwrap().iter()
        .map(|l| (l["advisor"].as_i64().unwrap(), l["student"].as_i64().unwrap()))
        .collect();
    links.sort();
    assert_eq!(links, [(2, 3), (3, 4)]);
    assert_eq!(g["truncated"], false);
}

#[sqlx::test(migrations = "../../migrations", fixtures("family"))]
async fn stats_summarise_the_database(pool: PgPool) {
    let s: Value = server(pool).get("/api/stats").await.json();
    assert_eq!(s["mathematicians"], 7);
    assert_eq!(s["relations"], 5);
    assert_eq!(s["first_year"], 1799);
    assert_eq!(s["last_scraped"], "2026-10-01T12:00:00+00:00");
}

#[sqlx::test(migrations = "../../migrations", fixtures("family"))]
async fn notable_ranks_by_student_count(pool: PgPool) {
    let n: Vec<Value> = server(pool).get("/api/notable").await.json();
    assert_eq!(n.len(), 5);
    assert!(n.iter().all(|p| p["student_count"] == 1));
}
