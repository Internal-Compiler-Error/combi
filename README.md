# Combi

Crawls the [Mathematics Genealogy Project](https://www.mathgenealogy.org) into Postgres and
serves a website to search every mathematician in the database and explore their advisors
and students.

## Layout

| Path | What it is |
|---|---|
| `crates/scraper` | `combi-scraper`: walks the site breadth-first from one or more IDs and stores what it finds |
| `crates/server` | `combi-server`: JSON API under `/api`; in production it also serves the built web app |
| `web/` | The website: Vite, React, TypeScript, TanStack Query, d3 for the radial family tree |
| `migrations/` | Database schema, shared by both crates (`sqlx migrate`) |
| `.sqlx/` | Offline query metadata, so the crates build without a running database |

The API's response types are Rust structs that derive `ts_rs::TS`. Running the server's
tests writes the matching TypeScript to `web/src/api/types/`, so the web app's types always
match the API. Never edit those files by hand.

## Develop

Everything runs in Docker; the only requirement is Docker with Compose.

```sh
docker compose up
```

| Service | URL | Notes |
|---|---|---|
| `web` | http://localhost:5173 | Vite dev server with hot reload; proxies `/api` to `server` |
| `server` | http://localhost:3000 | Rebuilt and restarted by watchexec when Rust files change |
| `db` | `postgres://combi@localhost:5432/combi` | Data persists in the `pgdata` volume |

`migrate` runs `sqlx migrate run` before the server starts. The first start compiles the
server from scratch and takes a few minutes; later restarts are incremental.

### Fill the database

The site is empty until something is crawled. To crawl Donald Knuth (ID 10416) and all of
his descendants:

```sh
docker compose run --rm dev cargo run -p combi-scraper -- --start 10416
```

`--start`/`--end` crawl a range of root IDs and `--concurrency` caps parallel downloads.
Pages fetched successfully in the last 24 hours are skipped.

### Common tasks

All of these run in the `dev` container (`docker compose run --rm dev <command>`), or on your
machine if you have Rust and Node installed and the `db` service running.

| Task | Command |
|---|---|
| Run every Rust test, and regenerate the TypeScript types | `cargo test --workspace` |
| Add a migration | `sqlx migrate add -r <name>` |
| Refresh `.sqlx/` after changing a query | `cargo sqlx prepare --workspace` |
| Typecheck the web app | `npm --prefix web run typecheck` |
| Production build of the web app | `npm --prefix web run build` |

The API tests use `#[sqlx::test]`, which creates a throwaway database per test, applies
every migration and loads `crates/server/tests/fixtures/family.sql`.

### Without Docker

With Postgres reachable at the `DATABASE_URL` in `.env`:

```sh
cargo install sqlx-cli --no-default-features --features rustls,postgres
sqlx migrate run
cargo run -p combi-server        # http://localhost:3000
npm --prefix web install
npm --prefix web run dev         # http://localhost:5173
```

## API

| Endpoint | Returns |
|---|---|
| `GET /api/search?q=&limit=` | People whose name matches, ignoring case and accents and tolerating typos. Words must appear in order; an all-digit query also matches the MGP ID |
| `GET /api/mathematicians/{id}` | One person with their advisors, students and descendant count |
| `GET /api/mathematicians/{id}/graph?up=&down=` | Their neighbourhood: `up` generations of advisors and `down` of students (0–6 each, capped at 1,500 people) |
| `GET /api/stats` | Counts for the whole database |
| `GET /api/notable` | The 12 people with the most students on record |

## Deploy

Build the web app, then run the server with `STATIC_DIR` pointing at it. The server answers
`/api/*` itself and serves the app for every other path.

```sh
npm --prefix web ci && npm --prefix web run build
cargo build --release -p combi-server
DATABASE_URL=postgres://… STATIC_DIR=web/dist ./target/release/combi-server
```

`LISTEN_ADDR` (default `0.0.0.0:3000`) and `RUST_LOG` are also read.
