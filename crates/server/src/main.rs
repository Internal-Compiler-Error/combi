use std::path::PathBuf;

use color_eyre::eyre::WrapErr;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info,tower_http=debug".into()))
        .init();

    let database_url = std::env::var("DATABASE_URL").wrap_err("DATABASE_URL is not set")?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(16)
        .connect(&database_url)
        .await
        .wrap_err("could not connect to the database")?;

    // in development Vite serves the web app; in production point this at `web/dist`
    let static_dir = std::env::var_os("STATIC_DIR").map(PathBuf::from);
    if let Some(dir) = &static_dir {
        info!("serving the web app from {}", dir.display());
    }

    let addr = std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("listening on http://{addr}");
    axum::serve(listener, combi_server::router(pool, static_dir.as_deref())).await?;
    Ok(())
}
