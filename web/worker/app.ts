import { Hono } from "hono";
import postgres from "postgres";
import {
  MAX_GRAPH_DEPTH,
  MAX_GRAPH_NODES,
  MAX_RELATION_DEPTH,
  MAX_SCHOOL_PEOPLE,
  type ApiErrorBody,
  type CountryStat,
  type CrawlResult,
  type Flows,
  type Graph,
  type GraphLink,
  type MgpHit,
  type Person,
  type PersonDetail,
  type Relation,
  type SchoolDetail,
  type SchoolHit,
  type SchoolStat,
  type Stats,
  type WalkStatus,
} from "../shared/types";
import { prettyCountry } from "../shared/names";
import { latestWalk, startWalk, walkStatus } from "./walk";
import { crawl, fetchMgpPage, fetchMgpSearch, mgpQuery, MgpNotFound, parseSearchResults, UpstreamError, type FetchPage, type FetchSearch } from "./crawl";

type Sql = postgres.Sql;
type Vars = { sql: Sql };

class NotFound extends Error {}


/** Escape LIKE wildcards and collapse whitespace so user input is matched literally. */
export function normalizeQuery(q: string): string {
  return q
    .split(/\s+/)
    .filter(Boolean)
    .map((w) => w.replace(/[\\%_]/g, (c) => `\\${c}`))
    .join(" ");
}

const clamp = (n: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, n));

/** Parse an optional integer query parameter, falling back when it is missing or malformed. */
function intParam(raw: string | undefined, fallback: number): number {
  const n = raw === undefined ? NaN : Number(raw);
  return Number.isInteger(n) ? n : fallback;
}

type PersonRow = {
  id: number;
  name: string | null;
  year: number | null;
  school: string | null;
  school_id: number | null;
  country: string | null;
  student_count: number | null;
};

function toPerson(r: PersonRow): Person {
  return {
    id: r.id,
    name: r.name ?? `Unknown (ID ${r.id})`,
    year: r.year,
    school: r.school,
    school_id: r.school_id,
    country: r.country ? prettyCountry(r.country) : null,
    student_count: r.student_count ?? 0,
  };
}

// Every person-shaped query selects these columns, so rows map straight onto `Person`.
const personColumns = (sql: Sql | postgres.TransactionSql) => sql`
  m.id, m.name, m.graduating_year as year, m.school,
  (select id from schools s where s.name = m.school) as school_id,
  (select string_agg(country, ', ' order by country) from school_locations l where l.school = m.school) as country,
  (select count(*)::int from advisor_relations sc where sc.advisor = m.id) as student_count`;

/** Requests that reach MGP, a volunteer-run site, are capped per visitor. */
async function withinMgpLimit(env: Env, ip: string | undefined): Promise<boolean> {
  const outcome = await env.MGP_LIMITER?.limit({ key: ip ?? "unknown" });
  return outcome?.success ?? true;
}

type Crawled = { last_crawled: Date | null; next_crawl: Date | null };

// When someone's own page was last crawled and is next due, from the schedule or, for pages
// crawled before it existed, two weeks after the log's last success (as crawl() does).
const crawledColumns = (sql: Sql) => sql`
  coalesce((select last_crawled from crawl_schedule where page = m.id),
           (select max(date) from scrape_logs where page_scraped = m.id and result = 'success')) as last_crawled,
  coalesce((select next_due from crawl_schedule where page = m.id),
           (select max(date) + interval '14 days' from scrape_logs where page_scraped = m.id and result = 'success')) as next_crawl`;

const iso = (d: Date | null) => (d ? new Date(d).toISOString() : null);
const crawledFields = (r: Crawled) => ({ last_crawled: iso(r.last_crawled), next_crawl: iso(r.next_crawl) });

export function createApp({ fetchPage = fetchMgpPage, fetchSearch = fetchMgpSearch }: { fetchPage?: FetchPage; fetchSearch?: FetchSearch } = {}) {
  const app = new Hono<{ Bindings: Env; Variables: Vars }>().basePath("/api");

  // One small connection pool per request: Hyperdrive does the real pooling at the edge.
  app.use(async (c, next) => {
    const sql = postgres(c.env.HYPERDRIVE.connectionString, { max: 5, fetch_types: false });
    c.set("sql", sql);
    try {
      await next();
    } finally {
      const closing = sql.end();
      try {
        c.executionCtx.waitUntil(closing);
      } catch {
        await closing; // no execution context outside the Workers runtime (tests)
      }
    }
  });

  app.onError((err, c) => {
    if (err instanceof NotFound) return c.json<ApiErrorBody>({ error: "No mathematician with that ID is in the database" }, 404);
    if (err instanceof MgpNotFound) return c.json<ApiErrorBody>({ error: "The Mathematics Genealogy Project has no one with that ID" }, 404);
    if (err instanceof UpstreamError) return c.json<ApiErrorBody>({ error: err.message }, 502);
    console.error(err);
    return c.json<ApiErrorBody>({ error: "Database error" }, 500);
  });

  app.notFound((c) => c.json<ApiErrorBody>({ error: "No such API endpoint" }, 404));

  app.get("/search", async (c) => {
    const q = normalizeQuery(c.req.query("q") ?? "");
    if (!q) return c.json<Person[]>([]);
    const limit = clamp(intParam(c.req.query("limit"), 20), 1, 100);

    // Words must appear in order ("donald knuth" finds "Donald Ervin Knuth"); word similarity
    // catches typos ("knuht"). An all-digit query also matches the MGP ID exactly.
    // pg_trgm's default threshold of 0.6 misses one-letter typos in short names, so loosen it
    // for this transaction only.
    const rows = await c.var.sql.begin(async (tx) => {
      await tx`set local pg_trgm.word_similarity_threshold = 0.45`;
      return tx<PersonRow[]>`
        with q as (select lower(immutable_unaccent(${q})) as q)
        select ${personColumns(tx)}
        from mathematicians m, q
        where m.search_name like '%' || replace(q.q, ' ', '%') || '%'
           or q.q <% m.search_name
           or m.id::text = ${q}
        order by m.id::text = ${q} desc,
                 m.search_name like replace(q.q, ' ', '%') || '%' desc,
                 word_similarity(q.q, m.search_name) desc,
                 student_count desc,
                 m.name
        limit ${limit}`;
    });
    return c.json<Person[]>(rows.map(toPerson));
  });

  app.get("/mathematicians/:id{[0-9]+}", async (c) => {
    const sql = c.var.sql;
    const id = Number(c.req.param("id"));

    const [row] = await sql<(PersonRow & { dissertation: string | null } & Crawled)[]>`
      select ${personColumns(sql)}, m.dissertation, ${crawledColumns(sql)}
      from mathematicians m where m.id = ${id}`;
    if (!row) throw new NotFound();

    const [advisors, students, [{ count }], walk] = await Promise.all([
      sql<PersonRow[]>`
        select ${personColumns(sql)}
        from advisor_relations r join mathematicians m on m.id = r.advisor
        where r.advisee = ${id}
        order by m.graduating_year nulls last, m.name`,
      sql<PersonRow[]>`
        select ${personColumns(sql)}
        from advisor_relations r join mathematicians m on m.id = r.advisee
        where r.advisor = ${id}
        order by m.graduating_year nulls last, m.name`,
      sql<[{ count: number }]>`
        with recursive d(id) as (
          select advisee from advisor_relations where advisor = ${id}
          union
          select r.advisee from advisor_relations r join d on r.advisor = d.id
        )
        select count(*)::int as count from d`,
      latestWalk(sql, id),
    ]);

    return c.json<PersonDetail>({
      ...toPerson(row),
      dissertation: row.dissertation?.trim() || null,
      advisors: advisors.map(toPerson),
      students: students.map(toPerson),
      descendant_count: count,
      ...crawledFields(row),
      walk,
    });
  });

  app.post("/mathematicians/:id{[0-9]+}/crawl", async (c) => {
    const id = Number(c.req.param("id"));
    if (id < 1 || id > 2 ** 31 - 1) return c.json<ApiErrorBody>({ error: "Not a valid MGP ID" }, 400);

    if (!(await withinMgpLimit(c.env, c.req.header("cf-connecting-ip")))) return c.json<ApiErrorBody>({ error: "Too many requests to MGP; try again in a minute" }, 429);

    // their own page now, so it shows straight away; the rest of their tree in the background
    const page = await crawl(c.var.sql, id, fetchPage);
    let walk: WalkStatus | null = null;
    if (c.env.CRAWL_QUEUE) {
      const started = await startWalk(c.var.sql, id);
      if (started.started) await c.env.CRAWL_QUEUE.send({ walk: started.walk.id });
      walk = started.walk;
    }
    return c.json<CrawlResult>({
      status: page.status,
      last_crawled: page.last_crawled.toISOString(),
      next_crawl: page.next_crawl.toISOString(),
      changed: page.changed,
      walk,
    });
  });

  app.get("/walks/:id{[0-9]+}", async (c) => {
    const walk = await walkStatus(c.var.sql, Number(c.req.param("id")));
    return walk ? c.json<WalkStatus>(walk) : c.json<ApiErrorBody>({ error: "No such walk" }, 404);
  });

  /** MGP's own search, for people we haven't crawled; each hit says whether we already have them. */
  app.get("/mgp/search", async (c) => {
    const query = mgpQuery(c.req.query("q") ?? "");
    if (!query) return c.json<MgpHit[]>([]);
    if (!(await withinMgpLimit(c.env, c.req.header("cf-connecting-ip")))) return c.json<ApiErrorBody>({ error: "Too many requests to MGP; try again in a minute" }, 429);

    const hits = parseSearchResults(await fetchSearch(query)).slice(0, 100);
    if (!hits.length) return c.json<MgpHit[]>([]);
    // an array literal, as in the graph query: these are integers parsed from MGP's links
    const ids = `{${hits.map((h) => h.id).join(",")}}`;
    const sql = c.var.sql;
    const known = await sql<({ id: number } & Crawled)[]>`
      select m.id, ${crawledColumns(sql)} from mathematicians m where m.id = any(${ids}::int[])`;
    const byId = new Map(known.map((k) => [k.id, k]));
    return c.json<MgpHit[]>(
      hits.map((h) => {
        const k = byId.get(h.id);
        return { ...h, known: k !== undefined, ...(k ? crawledFields(k) : { last_crawled: null, next_crawl: null }) };
      }),
    );
  });

  app.get("/mathematicians/:id{[0-9]+}/graph", async (c) => {
    const sql = c.var.sql;
    const id = Number(c.req.param("id"));
    const up = clamp(intParam(c.req.query("up"), 2), 0, MAX_GRAPH_DEPTH);
    const down = clamp(intParam(c.req.query("down"), 2), 0, MAX_GRAPH_DEPTH);

    const [{ exists }] = await sql<[{ exists: boolean }]>`select exists(select 1 from mathematicians where id = ${id})`;
    if (!exists) throw new NotFound();

    // Walk students downward and advisors upward, keeping each person at their nearest
    // generation. Fetch one extra row so we know whether the cap cut anything off.
    const rows = await sql<(PersonRow & { depth: number })[]>`
      with recursive
      down(id, depth) as (
        select ${id}::int, 0
        union
        select r.advisee, d.depth + 1 from advisor_relations r join down d on r.advisor = d.id where d.depth < ${down}::int
      ),
      up(id, depth) as (
        select ${id}::int, 0
        union
        select r.advisor, u.depth - 1 from advisor_relations r join up u on r.advisee = u.id where u.depth > -(${up}::int)
      ),
      nearest as (
        select id, (array_agg(depth order by abs(depth), depth))[1] as depth
        from (select * from down union all select * from up) both_ways
        group by id
      )
      select ${personColumns(sql)}, n.depth
      from nearest n join mathematicians m on m.id = n.id
      order by abs(n.depth), n.depth, m.graduating_year nulls last, m.id
      limit ${MAX_GRAPH_NODES + 1}`;

    const kept = rows.slice(0, MAX_GRAPH_NODES);
    // without fetch_types (needed behind Hyperdrive) postgres.js can't serialize arrays, so send
    // an array literal; the ids are integers read from the database, never user input
    const ids = `{${kept.map((r) => r.id).join(",")}}`;
    const links = await sql<GraphLink[]>`
      select advisor, advisee as student
      from advisor_relations
      where advisor = any(${ids}::int[]) and advisee = any(${ids}::int[])`;

    return c.json<Graph>({
      focus: id,
      nodes: kept.map((r) => ({ ...toPerson(r), depth: r.depth })),
      links: [...links],
      truncated: rows.length > MAX_GRAPH_NODES,
    });
  });

  app.get("/stats", async (c) => {
    const [s] = await c.var.sql<[Omit<Stats, "last_scraped"> & { last_scraped: Date | null }]>`
      select (select count(*)::int from mathematicians) as mathematicians,
             (select count(*)::int from advisor_relations) as relations,
             (select count(*)::int from countries) as countries,
             (select min(graduating_year) from mathematicians) as first_year,
             (select max(graduating_year) from mathematicians) as last_year,
             (select max(date) from scrape_logs where result = 'success') as last_scraped`;
    return c.json<Stats>({ ...s, last_scraped: s.last_scraped ? new Date(s.last_scraped).toISOString() : null });
  });

  app.get("/countries", async (c) => {
    const rows = await c.var.sql<Omit<CountryStat, "name">[]>`
      select l.country, count(distinct m.id)::int as mathematicians, count(distinct l.school)::int as schools
      from school_locations l join mathematicians m on m.school = l.school
      group by l.country
      order by mathematicians desc, l.country`;
    return c.json<CountryStat[]>(rows.map((r) => ({ ...r, name: prettyCountry(r.country) })));
  });

  app.get("/countries/:country/schools", async (c) => {
    const rows = await c.var.sql<SchoolStat[]>`
      select s.id, s.name as school, count(*)::int as mathematicians
      from school_locations l join schools s on s.name = l.school join mathematicians m on m.school = l.school
      where l.country = ${c.req.param("country")}
      group by s.id, s.name
      order by mathematicians desc, s.name
      limit 500`;
    return c.json<SchoolStat[]>(rows);
  });

  app.get("/schools/:id{[0-9]+}", async (c) => {
    const sql = c.var.sql;
    const id = Number(c.req.param("id"));
    const [school] = await sql<{ name: string }[]>`select name from schools where id = ${id}`;
    if (!school) return c.json<ApiErrorBody>({ error: "No school with that ID is in the database" }, 404);

    const [countries, [summary], decades, people] = await Promise.all([
      sql<{ country: string }[]>`select country from school_locations where school = ${school.name} order by country`,
      sql<[{ mathematicians: number; first_year: number | null; last_year: number | null }]>`
        select count(*)::int as mathematicians, min(graduating_year) as first_year, max(graduating_year) as last_year
        from mathematicians where school = ${school.name}`,
      sql<{ decade: number; count: number }[]>`
        select (graduating_year / 10 * 10)::int as decade, count(*)::int as count
        from mathematicians where school = ${school.name} and graduating_year is not null
        group by 1 order by 1`,
      sql<PersonRow[]>`
        select ${personColumns(sql)} from mathematicians m
        where m.school = ${school.name}
        order by m.graduating_year desc nulls last, m.name
        limit ${MAX_SCHOOL_PEOPLE}`,
    ]);
    return c.json<SchoolDetail>({
      id,
      name: school.name,
      countries: countries.map((r) => ({ country: r.country, name: prettyCountry(r.country) })),
      ...summary,
      decades: [...decades],
      people: people.map(toPerson),
    });
  });

  app.get("/schools/search", async (c) => {
    const q = normalizeQuery(c.req.query("q") ?? "");
    if (!q) return c.json<SchoolHit[]>([]);
    const limit = clamp(intParam(c.req.query("limit"), 8), 1, 50);
    // the same matching as people's names, against the school's lower-cased, accent-free name
    const rows = await c.var.sql.begin(async (tx) => {
      await tx`set local pg_trgm.word_similarity_threshold = 0.45`;
      return tx<(Omit<SchoolHit, "country"> & { country: string | null })[]>`
        with q as (select lower(immutable_unaccent(${q})) as q)
        select s.id, s.name,
               (select string_agg(country, ', ' order by country) from school_locations l where l.school = s.name) as country,
               (select count(*)::int from mathematicians m where m.school = s.name) as mathematicians
        from schools s, q
        where s.search_name like '%' || replace(q.q, ' ', '%') || '%' or q.q <% s.search_name
        order by s.search_name like replace(q.q, ' ', '%') || '%' desc,
                 word_similarity(q.q, s.search_name) desc,
                 mathematicians desc
        limit ${limit}`;
    });
    return c.json<SchoolHit[]>(rows.map((r) => ({ ...r, country: r.country && r.country.split(", ").map(prettyCountry).join(", ") })));
  });

  app.get("/flows", async (c) => {
    const sql = c.var.sql;
    const from = c.req.query("from");
    const to = c.req.query("to");
    // without a range, links whose student has no recorded year count too
    const inRange = from || to ? sql`and s.graduating_year between ${intParam(from, -9999)} and ${intParam(to, 9999)}` : sql``;
    const crossBorder = sql`
      from advisor_relations r
      join mathematicians a on a.id = r.advisor join school_locations la on la.school = a.school
      join mathematicians s on s.id = r.advisee join school_locations ls on ls.school = s.school
      where la.country <> ls.country`;
    const [flows, decades] = await Promise.all([
      sql<Flows["flows"]>`
        select la.country as "from", ls.country as "to", count(*)::int as count
        ${crossBorder} ${inRange}
        group by 1, 2 order by 3 desc, 1, 2`,
      sql<Flows["decades"]>`
        select (s.graduating_year / 10 * 10)::int as decade, count(*)::int as count
        ${crossBorder} and s.graduating_year is not null
        group by 1 order by 1`,
    ]);
    return c.json<Flows>({ flows: [...flows], decades: [...decades] });
  });

  app.get("/relation", async (c) => {
    const sql = c.var.sql;
    const a = Number(c.req.query("a"));
    const b = Number(c.req.query("b"));
    if (!Number.isInteger(a) || !Number.isInteger(b)) return c.json<ApiErrorBody>({ error: "Give two MGP IDs as a and b" }, 400);

    // everyone above a person, each at their nearest generation, with the student one step
    // nearer the person (`via`) so the shortest line back down can be followed
    type Up = { id: number; depth: number; via: number | null };
    const up = (id: number) => sql<Up[]>`
      with recursive up(id, depth, via) as (
        select ${id}::int, 0, null::int
        union
        select r.advisor, u.depth + 1, u.id from advisor_relations r join up u on r.advisee = u.id where u.depth < ${MAX_RELATION_DEPTH}
      )
      select distinct on (id) id, depth, via from up order by id, depth, via`;
    const [upA, upB] = await Promise.all([up(a), up(b)]);
    const fromA = new Map(upA.map((r) => [r.id, r]));
    const fromB = new Map(upB.map((r) => [r.id, r]));

    // nearest shared ancestor: fewest generations in total, then the most even split
    let best: { id: number; total: number; spread: number } | null = null;
    for (const [id, ra] of fromA) {
      const rb = fromB.get(id);
      if (!rb) continue;
      const total = ra.depth + rb.depth;
      const spread = Math.abs(ra.depth - rb.depth);
      if (!best || total < best.total || (total === best.total && spread < best.spread)) best = { id, total, spread };
    }
    const down = (from: Map<number, Up>, top: number) => {
      const ids = [top];
      for (let at = from.get(top)!; at.via !== null; at = from.get(at.via)!) ids.push(at.via);
      return ids;
    };
    const idsA = best ? down(fromA, best.id) : [];
    const idsB = best ? down(fromB, best.id) : [];

    // an array literal, as in the graph query: these are integers, from the URL or the database
    const all = `{${[...new Set([a, b, ...idsA, ...idsB])].join(",")}}`;
    const rows = await sql<PersonRow[]>`select ${personColumns(sql)} from mathematicians m where m.id = any(${all}::int[])`;
    const byId = new Map(rows.map((r) => [r.id, toPerson(r)]));
    if (!byId.has(a) || !byId.has(b)) throw new NotFound();
    return c.json<Relation>({
      a: byId.get(a)!,
      b: byId.get(b)!,
      ancestor: best ? byId.get(best.id)! : null,
      pathA: idsA.map((id) => byId.get(id)!),
      pathB: idsB.map((id) => byId.get(id)!),
    });
  });

  /** The advisors with the most students on record, as starting points for browsing. */
  app.get("/notable", async (c) => {
    const rows = await c.var.sql<PersonRow[]>`
      select m.id, m.name, m.graduating_year as year, m.school,
             (select id from schools s where s.name = m.school) as school_id,
             (select string_agg(country, ', ' order by country) from school_locations l where l.school = m.school) as country,
             c.n as student_count
      from (select advisor, count(*)::int as n from advisor_relations group by advisor order by n desc, advisor limit 12) c
      join mathematicians m on m.id = c.advisor
      order by c.n desc, m.name`;
    return c.json<Person[]>(rows.map(toPerson));
  });

  return app;
}
