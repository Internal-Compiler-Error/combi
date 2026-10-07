import type postgres from "postgres";
import { MAX_WALK_FETCHES, type WalkStatus } from "../shared/types";
import { crawl, fetchMgpPage, MgpNotFound, UpstreamError, type FetchPage } from "./crawl";

/** The queue message that advances a walk by one step. */
export type WalkMessage = { walk: number };

/** what one step may fetch from MGP before it hands over to the next message */
export const STEP_FETCHES = 12;
/** what one step may look at in all, counting pages that aren't due and are only read from the database */
const STEP_VISITS = 40;
/** pages crawled at once; MGP's latency, not CPU, is what a step spends most of its time on */
export const PARALLEL = 4;

type WalkRow = { id: number; root: number; status: WalkStatus["status"]; fetched: number; skipped: number; failed: number };

export async function walkStatus(sql: postgres.Sql, id: number): Promise<WalkStatus | null> {
  const [w] = await sql<(WalkRow & { todo: number })[]>`
    select w.id::int, w.root, w.status, w.fetched, w.skipped, w.failed,
           (select count(*)::int from crawl_frontier f where f.walk = w.id and not f.done) as todo
    from crawl_walks w where w.id = ${id}`;
  return w ? { ...w } : null;
}

/** The walk to report on someone's page: their latest, if it started in the last day. */
export async function latestWalk(sql: postgres.Sql, root: number): Promise<WalkStatus | null> {
  const [w] = await sql<{ id: number }[]>`
    select id::int from crawl_walks where root = ${root} and started > now() - interval '1 day' order by started desc limit 1`;
  return w ? walkStatus(sql, w.id) : null;
}

/**
 * Start walking someone's tree, both down through their students and up through their advisors.
 * A walk already running from them, or one that finished in the last hour, is returned instead,
 * so repeated clicks don't pile up. `started` says whether a new walk needs a queue message.
 */
export async function startWalk(sql: postgres.Sql, root: number): Promise<{ walk: WalkStatus; started: boolean }> {
  const [recent] = await sql<{ id: number }[]>`
    select id::int from crawl_walks
    where root = ${root} and (status = 'running' or finished > now() - interval '1 hour')
    order by started desc limit 1`;
  if (recent) return { walk: (await walkStatus(sql, recent.id))!, started: false };

  const id = await sql.begin(async (tx) => {
    const [w] = await tx<{ id: number }[]>`insert into crawl_walks (root) values (${root}) returning id::int`;
    await tx`insert into crawl_frontier (walk, page, depth, direction) values (${w!.id}, ${root}, 0, 'both')`;
    return w!.id;
  });
  return { walk: (await walkStatus(sql, id))!, started: true };
}

/**
 * Advance a walk breadth-first: crawl the nearest pages that are due, a few at a time, and from
 * every page visited queue its students (going down) and advisors (going up), as the database now
 * knows them. Returns "more" when there's work left, "wait" when MGP couldn't be reached, "done" otherwise.
 */
export async function walkStep(
  sql: postgres.Sql,
  id: number,
  { fetchPage = fetchMgpPage, maxFetches = MAX_WALK_FETCHES }: { fetchPage?: FetchPage; maxFetches?: number } = {},
): Promise<"more" | "wait" | "done"> {
  const [walk] = await sql<WalkRow[]>`select id::int, root, status, fetched, skipped, failed from crawl_walks where id = ${id}`;
  if (!walk || walk.status !== "running") return "done";

  const finish = async (status: "done" | "capped") => {
    await sql`update crawl_walks set status = ${status}, finished = now() where id = ${id}`;
    return "done" as const;
  };

  type At = { page: number; depth: number; direction: "both" | "down" | "up" };
  const visit = async (at: At): Promise<"fetched" | "skipped" | "failed"> => {
    let outcome: "fetched" | "skipped" | "failed";
    try {
      outcome = (await crawl(sql, at.page, fetchPage)).status === "crawled" ? "fetched" : "skipped";
    } catch (e) {
      if (!(e instanceof MgpNotFound)) throw e;
      outcome = "failed";
    }
    await sql.begin(async (tx) => {
      await tx`
        insert into crawl_frontier (walk, page, depth, direction)
        select ${id}::bigint, advisee, ${at.depth + 1}::int, 'down' from advisor_relations where advisor = ${at.page} and ${at.direction !== "up"}
        union all
        select ${id}::bigint, advisor, ${at.depth + 1}::int, 'up' from advisor_relations where advisee = ${at.page} and ${at.direction !== "down"}
        on conflict do nothing`;
      await tx`update crawl_frontier set done = true where walk = ${id} and page = ${at.page}`;
      await tx`
        update crawl_walks set fetched = fetched + ${outcome === "fetched" ? 1 : 0}, skipped = skipped + ${outcome === "skipped" ? 1 : 0},
          failed = failed + ${outcome === "failed" ? 1 : 0}
        where id = ${id}`;
    });
    return outcome;
  };

  let fetched = 0;
  let visited = 0;
  while (fetched < STEP_FETCHES && visited < STEP_VISITS) {
    if (walk.fetched + fetched >= maxFetches) return finish("capped");
    const batch = await sql<At[]>`
      select page, depth, direction from crawl_frontier where walk = ${id} and not done order by depth, page limit ${PARALLEL}`;
    if (!batch.length) return finish("done");
    let outcomes;
    try {
      outcomes = await Promise.all(batch.map(visit));
    } catch (e) {
      // pages already visited in this batch stay done; the rest are tried again after a pause
      if (e instanceof UpstreamError) return "wait";
      // two pages listing the same people can still collide; their next step tries again
      if ((e as { code?: string }).code === "40P01") return "more";
      throw e;
    }
    visited += outcomes.length;
    fetched += outcomes.filter((o) => o === "fetched").length;
  }
  return "more";
}
