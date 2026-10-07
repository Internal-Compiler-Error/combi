pub mod api;

use std::path::Path;

use axum::Router;
use axum::routing::get;
use sqlx::PgPool;
use tower_http::compression::CompressionLayer;
use tower_http::services::{ServeDir, ServeFile};
use tower_http::trace::TraceLayer;

/// The API under `/api`. When `static_dir` holds a built web app, it is served for every
/// other path, with unknown paths falling back to `index.html` so client-side routes work.
pub fn router(pool: PgPool, static_dir: Option<&Path>) -> Router {
    let api = Router::new()
        .route("/stats", get(api::stats))
        .route("/notable", get(api::notable))
        .route("/search", get(api::search))
        .route("/mathematicians/{id}", get(api::person))
        .route("/mathematicians/{id}/graph", get(api::graph))
        .with_state(pool);

    let mut app = Router::new().nest("/api", api);
    if let Some(dir) = static_dir {
        app = app.fallback_service(ServeDir::new(dir).fallback(ServeFile::new(dir.join("index.html"))));
    }
    app.layer(CompressionLayer::new()).layer(TraceLayer::new_for_http())
}
