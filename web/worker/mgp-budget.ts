import type postgres from "postgres";
import { UpstreamError, type FetchPage, type FetchSearch } from "./crawl";

/** MGP sees at most this many requests a second from the whole site, with bursts of the same size */
export const MGP_REQUESTS_PER_SECOND = 4;
/** a request that can't get a turn this soon gives up, so a crawl button doesn't hang */
const MAX_WAIT_MS = 20_000;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Wait for a turn to call MGP, from the token bucket in mgp_budget shared by every Worker invocation. */
export async function mgpTurn(sql: postgres.Sql): Promise<void> {
  const rate = MGP_REQUESTS_PER_SECOND;
  const deadline = Date.now() + MAX_WAIT_MS;
  for (;;) {
    // refill for the time since the last spend, capped at one burst, then spend one if there is
    // one. The arithmetic is on the row itself so that when requests race, Postgres rechecks the
    // condition against the row as the winner left it.
    const refilled = sql`least(${rate}::real, tokens + extract(epoch from clock_timestamp() - updated)::real * ${rate})`;
    const spent = await sql`update mgp_budget set tokens = ${refilled} - 1, updated = clock_timestamp() where ${refilled} >= 1 returning 1`;
    if (spent.length) return;
    const [row] = await sql<{ tokens: number }[]>`select ${refilled} as tokens from mgp_budget`;
    if (Date.now() > deadline) throw new UpstreamError("MGP is busy with other crawls; try again in a moment");
    await sleep(Math.max(25, ((1 - row!.tokens) / rate) * 1000));
  }
}

export const budgeted =
  (sql: postgres.Sql, fetchPage: FetchPage): FetchPage =>
  async (id) => (await mgpTurn(sql), fetchPage(id));

export const budgetedSearch =
  (sql: postgres.Sql, fetchSearch: FetchSearch): FetchSearch =>
  // a search is two requests: the form, then its results page
  async (query) => (await mgpTurn(sql), await mgpTurn(sql), fetchSearch(query));
