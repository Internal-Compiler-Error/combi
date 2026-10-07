# Combi

A website to search mathematicians from the [Mathematics Genealogy Project](https://www.mathgenealogy.org)
(MGP) and explore their advisors and students. Visitors crawl MGP pages into Postgres on
demand from the site itself. It runs on Cloudflare Workers.

## Layout

| Path | What it is |
|---|---|
| `migrations/` | Database schema (`sqlx migrate`) |
| `web/src` | The website: Vite, React, TypeScript, TanStack Query, d3 for the radial family tree and the world map (`/map`, Natural Earth outlines from `world-atlas`) |
| `web/worker` | The API: a Cloudflare Worker (Hono + postgres.js) serving `/api/*`, reaching Postgres through Hyperdrive. `crawl.ts` fetches and parses MGP pages |
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

The site is empty until something is crawled. Search for anyone: a name with no matches here
is looked up on MGP's own search, and every MGP result has a **Crawl** button (searching an MGP
ID offers to crawl it directly). Or open someone, for example http://localhost:5173/m/10416
for Donald Knuth, and press **Crawl their tree**.

A crawl fetches that person's page straight away, then walks their tree in the background
(`web/worker/walk.ts`): their students, their students' students and so on, and their advisors up
the line. Each step of a walk is a message on the `combi-crawl` queue that crawls up to 12
pages, four at a time, and queues the next step; a walk stops after 2,000 fetches.

Every request to MGP, from walks, crawl buttons and MGP searches alike, first takes a turn from
one token bucket in Postgres (`web/worker/mgp-budget.ts`), so the whole site stays under 4
requests a second however many walks are running.

Pages are only fetched when due (`crawl_schedule`). Every crawl stores a hash of what the page
said: unchanged, the page waits twice as long before the next check; changed, half as long
(between 3 days and a year). New pages start at 14 days for people early in their career or with
recent students, and up to 180 days for settled ones. A walk follows pages that aren't due
through the database, so it still reaches everyone below them.

### Common tasks

| Task | Command |
|---|---|
| API tests (needs `db` running) | `npm --prefix web test` |
| Typecheck the site, the worker and the configs | `npm --prefix web run typecheck` |
| Add a migration | `docker compose run --rm dev sqlx migrate add -r <name>` |

The API tests create throwaway databases with every migration applied, load
`web/worker/test/fixtures/family.sql` into one of them and drop them afterwards. The crawler
tests parse saved MGP pages from `web/worker/test/fixtures/mgp/` and never reach the real site. They connect as
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
| `POST /api/mathematicians/{id}/crawl` | Fetches their MGP page into the database (`"status": "crawled"`, or `"fresh"` when it isn't due) and starts a walk through their tree. 404 when MGP has no such ID, 429 past the rate limit |
| `GET /api/mgp/search?q=` | MGP's own search, marking who is already in the database. One word is a family name; with more, the first is the given name and the last the family name. Shares the crawl rate limit |
| `GET /api/countries` | Mathematicians and schools per country, by where the degree was awarded |
| `GET /api/countries/{country}/schools` | That country's schools with their mathematician counts; `country` is MGP's name, e.g. `UnitedStates` |
| `GET /api/schools/{id}` | A school: its countries, degrees per decade and graduates, newest first (up to 2,000) |
| `GET /api/schools/search?q=&limit=` | Schools whose name matches, with the same accent- and typo-tolerant matching as people |
| `GET /api/flows?from=&to=` | Advisor–student links that cross borders, as advisor's degree country → student's, optionally for students who graduated in a year range; plus cross-border links per decade |
| `GET /api/relation?a=&b=` | Two people's nearest shared academic ancestor and the shortest line down from it to each |
| `GET /api/walks/{id}` | Progress of a walk started by a crawl: pages fetched, already up to date, not on MGP, and still to visit |
| `GET /api/stats` | Counts for the whole database |
| `GET /api/notable` | The 12 people with the most students on record |

## Deploy

The site and API deploy together as one Worker; pushes to `main` deploy through the
Cloudflare GitHub integration, which runs `npm run build` and `npx wrangler deploy` in `web/`. Postgres is the `combi` Neon project (linked in the gitignored
`.neon`), reached through the `combi` Hyperdrive config whose ID is in `web/wrangler.jsonc`.
Hyperdrive pools connections itself, so it uses Neon's direct (unpooled) endpoint. Its query
cache is off (`--caching-disabled`) so a crawl shows up immediately.

1. Apply the migrations to Neon. `neon connection-string` prints the direct URL:
   ```sh
   DATABASE_URL="$(neon connection-string)" docker compose run --rm --no-deps -e DATABASE_URL dev sqlx migrate run
   ```
2. If the Neon password or endpoint changes, repoint Hyperdrive (the ID stays the same):
   ```sh
   cd web
   npx wrangler hyperdrive update 36c14748c0aa49f3a88aaec5498dcdcc --connection-string="postgres://user:password@host:5432/db"
   ```
3. Deploy: `npm run deploy`
4. Crawl from the live site. The deployed Worker allows each visitor 10 crawls a minute
   (`MGP_LIMITER` in `web/wrangler.jsonc`, shared with MGP searches).
