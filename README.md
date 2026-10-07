# Combi

Crawls the [Mathematics Genealogy Project](https://www.mathgenealogy.org) into Postgres and
serves a website to search every mathematician in the database and explore their advisors
and students. The site runs on Cloudflare Workers.

## Layout

| Path | What it is |
|---|---|
| `crates/scraper` | `combi-scraper` (Rust): walks the site breadth-first from one or more IDs and stores what it finds |
| `migrations/` | Database schema (`sqlx migrate`) |
| `.sqlx/` | Offline query metadata, so the scraper builds without a running database |
| `web/src` | The website: Vite, React, TypeScript, TanStack Query, d3 for the radial family tree |
| `web/worker` | The API: a Cloudflare Worker (Hono + postgres.js) serving `/api/*`, reaching Postgres through Hyperdrive |
| `web/shared` | Types used by both the API and the website |

## Develop

Everything runs in Docker; the only requirement is Docker with Compose.

```sh
docker compose up
```

| Service | URL | Notes |
|---|---|---|
| `web` | http://localhost:5173 | Vite with the API worker running inside it in the real Workers runtime; both hot-reload |
| `db` | `postgres://combi@localhost:5432/combi` | Data persists in the `pgdata` volume |

`migrate` runs `sqlx migrate run` before the site starts.

### Fill the database

The site is empty until something is crawled. To crawl Donald Knuth (ID 10416) and all of
his descendants:

```sh
docker compose run --rm dev cargo run -p combi-scraper -- --start 10416
```

`--start`/`--end` crawl a range of root IDs and `--concurrency` caps parallel downloads.
Pages fetched successfully in the last 24 hours are skipped.

### Common tasks

| Task | Command |
|---|---|
| API tests (needs `db` running) | `npm --prefix web test` |
| Typecheck the site, the worker and the configs | `npm --prefix web run typecheck` |
| Scraper tests | `docker compose run --rm dev cargo test --workspace` |
| Add a migration | `docker compose run --rm dev sqlx migrate add -r <name>` |
| Refresh `.sqlx/` after changing a scraper query | `docker compose run --rm dev cargo sqlx prepare --workspace` |

The API tests create a throwaway database, apply every migration, load
`web/worker/test/fixtures/family.sql` and drop the database afterwards. They connect as
`TEST_DATABASE_URL`, falling back to `DATABASE_URL` and then the local dev database.

### Without Docker

With Postgres running and reachable at `postgres://combi@localhost:5432/combi`:

```sh
cargo install sqlx-cli --no-default-features --features rustls,postgres
sqlx migrate run
npm --prefix web install
npm --prefix web run dev         # http://localhost:5173
```

To use a different database in development, set
`CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE`.

## API

| Endpoint | Returns |
|---|---|
| `GET /api/search?q=&limit=` | People whose name matches, ignoring case and accents and tolerating typos. Words must appear in order; an all-digit query also matches the MGP ID |
| `GET /api/mathematicians/{id}` | One person with their advisors, students and descendant count |
| `GET /api/mathematicians/{id}/graph?up=&down=` | Their neighbourhood: `up` generations of advisors and `down` of students (0–6 each, capped at 1,500 people) |
| `GET /api/stats` | Counts for the whole database |
| `GET /api/notable` | The 12 people with the most students on record |

## Deploy

The site and API deploy together as one Worker. Postgres has to be hosted somewhere the
Worker can reach (for example Neon or Supabase) and must allow the `unaccent` and `pg_trgm`
extensions.

1. Apply the migrations to the hosted database: `DATABASE_URL=postgres://… sqlx migrate run`
2. Create a Hyperdrive config for it, and paste the ID it prints into `web/wrangler.jsonc`:
   ```sh
   cd web
   npx wrangler hyperdrive create combi --connection-string="postgres://user:password@host:5432/db"
   ```
3. Deploy: `npm run deploy`
4. Crawl into the hosted database by pointing the scraper at it:
   `DATABASE_URL=postgres://… cargo run -p combi-scraper -- --start 10416`
