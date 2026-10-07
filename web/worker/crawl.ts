import { parse, type HTMLElement } from "node-html-parser";
import type postgres from "postgres";
import { MAX_RECRAWL_DAYS, MIN_RECRAWL_DAYS, mgpUrl, NOT_FOUND_RECHECK_DAYS } from "../shared/types";

type Ref = { id: number; name: string };
export type Student = Ref & { school: string | null; year: number | null };

/** What one Mathematics Genealogy Project page says about a person. */
export type MgpPage = {
  id: number;
  name: string;
  dissertation: string | null;
  school: string | null;
  /** as MGP names its flag images: "UnitedStates" */
  country: string | null;
  year: number | null;
  advisors: Ref[];
  students: Student[];
};

export type FetchPage = (id: number) => Promise<string>;
/** Runs a search on MGP and returns the results page */
export type FetchSearch = (query: MgpQuery) => Promise<string>;
export type MgpQuery = { given_name?: string; family_name: string };

export class MgpNotFound extends Error {}
export class UpstreamError extends Error {}

const ID_IN_HREF = /id\.php\?id=(\d+)/;
// the site answers unknown IDs with a 200 and this sentence
const NOT_FOUND = "You have specified an ID that does not exist in the database. Please back up and try again.";

const squash = (s: string) => s.trim().replace(/\s+/g, " ");
// MGP has typos like a degree "200" from a university founded in 1989; no degree on it predates 1000
const plausibleYear = (n: number) => (Number.isInteger(n) && n >= 1000 && n <= new Date().getFullYear() + 1 ? n : null);
const orNull = (s: string | undefined) => (s ? squash(s) || null : null);

function hrefId(a: HTMLElement): number | null {
  const m = ID_IN_HREF.exec(a.getAttribute("href") ?? "");
  return m ? Number(m[1]) : null;
}

/** Student tables list "Hall, Jr., Marshall"; the person's own page says "Marshall Hall, Jr.". */
function unsurname(listed: string): string {
  const [surname = "", ...rest] = listed.split(",").map(squash);
  return rest.length ? [...rest, surname].join(" ") : surname;
}

/** Student tables and search results share a row layout: linked name, school, year. */
function parseListedPerson(row: HTMLElement): Student | null {
  const [nameCell, schoolCell, yearCell] = row.querySelectorAll("td");
  const a = nameCell?.querySelector("a");
  const id = a && hrefId(a);
  if (!a || id == null) return null;
  return { id, name: unsurname(a.text), school: orNull(schoolCell?.text), year: plausibleYear(Number(yearCell?.text.trim() || NaN)) };
}

/** Returns null when MGP has no one with this ID. */
export function parsePage(id: number, html: string): MgpPage | null {
  const doc = parse(html);
  const paragraphs = doc.querySelectorAll("p");
  if (paragraphs.some((p) => p.text.trim() === NOT_FOUND)) return null;

  const name = orNull(doc.querySelector("h2")?.text);
  if (!name) throw new Error(`MGP page ${id} has no name`);

  // <span>Ph.D. <span>School</span>1963</span>: the year is a direct text child
  const degree = doc.querySelector("div > span");
  const yearText = degree?.childNodes.map((n) => n.nodeType === 3 ? n.text.trim() : "").find((t) => /^\d+$/.test(t));

  const advisors: Ref[] = [];
  for (const p of paragraphs.filter((p) => p.text.trim().startsWith("Advisor"))) {
    for (const a of p.querySelectorAll("a")) {
      const advisor = hrefId(a);
      if (advisor !== null) advisors.push({ id: advisor, name: squash(a.text) });
    }
  }

  // the header row has no cells, so it drops out
  const students = (doc.querySelector("table")?.querySelectorAll("tr") ?? []).map(parseListedPerson).filter((s) => s !== null);

  return {
    id,
    name,
    dissertation: orNull(doc.querySelector("#thesisTitle")?.text),
    school: orNull(degree?.querySelector("span")?.text),
    country: orNull(doc.querySelector("div > img")?.getAttribute("alt")),
    year: yearText ? plausibleYear(Number(yearText)) : null,
    advisors,
    students,
  };
}

async function mgpFetch(url: string, init: RequestInit = {}): Promise<Response> {
  let res: Response;
  try {
    res = await fetch(url, { ...init, headers: { "user-agent": "combi-crawler", ...init.headers } });
  } catch (e) {
    throw new UpstreamError(`Could not reach the Mathematics Genealogy Project: ${e}`);
  }
  if (!res.ok && init.redirect !== "manual") throw new UpstreamError(`The Mathematics Genealogy Project answered ${res.status}`);
  return res;
}

export const fetchMgpPage: FetchPage = async (id) => (await mgpFetch(mgpUrl(id))).text();

/** MGP's search form posts the query, then redirects to a results page tied to a PHP session. */
export const fetchMgpSearch: FetchSearch = async (query) => {
  const posted = await mgpFetch("https://www.mathgenealogy.org/query-prep.php", {
    method: "POST",
    body: new URLSearchParams({ chrono: "0", given_name: "", other_names: "", school: "", year: "", thesis: "", country: "", msc: "", ...query }),
    redirect: "manual",
  });
  const results = posted.headers.get("location");
  if (posted.status !== 302 || !results) throw new UpstreamError(`The Mathematics Genealogy Project's search answered ${posted.status}`);
  const session = posted.headers.get("set-cookie")?.split(";")[0];
  return (await mgpFetch(new URL(results, "https://www.mathgenealogy.org/").toString(), { headers: session ? { cookie: session } : {} })).text();
};

/** One word searches family names; more searches the first as a given name and the last as the family name. */
export function mgpQuery(q: string): MgpQuery | null {
  const words = q.split(/\s+/).filter(Boolean);
  if (!words.length) return null;
  return words.length === 1 ? { family_name: words[0]! } : { given_name: words[0]!, family_name: words.at(-1)! };
}

export const parseSearchResults = (html: string): Student[] =>
  parse(html).querySelectorAll("#mainContent tr").map(parseListedPerson).filter((s) => s !== null);

const unique = <T>(xs: T[]) => [...new Set(xs)];
const byId = <T extends Ref>(xs: T[]) => [...new Map(xs.map((x) => [x.id, x])).values()];

const DAY = 86_400_000;

/** What a crawl of one page did, and when that page is next worth fetching. */
export type PageCrawl = { status: "crawled" | "fresh"; last_crawled: Date; next_crawl: Date; changed: boolean | null };

/**
 * The first recrawl interval, before there's any history of changes: someone with a recent degree
 * or recent students may still gain students; someone whose last student graduated decades ago won't.
 */
export function firstInterval(page: MgpPage, now: Date): number {
  const latest = Math.max(page.year ?? 0, ...page.students.map((s) => s.year ?? 0));
  const year = now.getUTCFullYear();
  // early in a career (a degree in the last 25 years) or still graduating students
  if (page.year === null || page.year >= year - 25 || latest >= year - 10) return 14;
  return latest >= year - 40 ? 60 : 180;
}

/** Halve the interval when the page changed, double it when it didn't, within the bounds. */
export const nextInterval = (previous: number, changed: boolean) =>
  Math.min(MAX_RECRAWL_DAYS, Math.max(MIN_RECRAWL_DAYS, changed ? previous / 2 : previous * 2));

async function contentHash(page: MgpPage): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(JSON.stringify(page)));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, "0")).join("");
}

/**
 * Crawl one person's page unless it isn't due yet (see crawl_schedule). Their advisors and
 * students are stored from what the page says about them (name, and school and year for students)
 * without being crawled themselves; a later crawl of theirs fills in the rest.
 */
export async function crawl(sql: postgres.Sql, id: number, fetchPage: FetchPage = fetchMgpPage, now = new Date()): Promise<PageCrawl> {
  // pages crawled before the schedule existed, or by an older crawler, fall back to the log
  const [known] = await sql<{ last: Date | null; next: Date | null; interval_days: number | null; content_hash: string | null; missing: Date | null }[]>`
    select coalesce(s.last_crawled, l.last) as last,
           coalesce(s.next_due, l.last + interval '14 days') as next,
           s.interval_days, s.content_hash,
           (select max(date) from scrape_logs where page_scraped = ${id} and result = 'failed') as missing
    from (select ${id}::int as page) p
    left join crawl_schedule s on s.page = p.page
    left join lateral (select max(date) as last from scrape_logs where page_scraped = p.page and result = 'success') l on true`;
  const last = known?.last ? new Date(known.last) : null;
  if (known?.missing && (!last || last < new Date(known.missing)) && now.getTime() - new Date(known.missing).getTime() < NOT_FOUND_RECHECK_DAYS * DAY) {
    throw new MgpNotFound();
  }
  if (last && known?.next && now < new Date(known.next)) {
    return { status: "fresh", last_crawled: last, next_crawl: new Date(known.next), changed: null };
  }

  const page = parsePage(id, await fetchPage(id));
  if (!page) {
    await sql`insert into scrape_logs (date, page_scraped, result) values (${now}, ${id}, 'failed')`;
    throw new MgpNotFound();
  }

  const hash = await contentHash(page);
  const changed = known?.content_hash ? hash !== known.content_hash : null;
  const interval = changed === null || known?.interval_days == null ? firstInterval(page, now) : nextInterval(known.interval_days, changed);
  const next = new Date(now.getTime() + interval * DAY);

  const students = byId(page.students.filter((s) => s.id !== id));
  const advisors = byId(page.advisors.filter((a) => a.id !== id));
  // rows go in sorted, so crawls running side by side take their locks in the same order
  const schools = unique([page.school, ...students.map((s) => s.school)].filter((s): s is string => s !== null)).sort();

  await sql.begin(async (tx) => {
    if (page.country) await tx`insert into countries (name) values (${page.country}) on conflict do nothing`;
    if (schools.length) await tx`insert into schools ${tx(schools.map((name) => ({ name })))} on conflict do nothing`;
    if (page.school && page.country) {
      await tx`insert into school_locations (school, country) values (${page.school}, ${page.country}) on conflict do nothing`;
    }

    await tx`
      insert into mathematicians (id, name, dissertation, graduating_year, school)
      values (${id}, ${page.name}, ${page.dissertation}, ${page.year}, ${page.school})
      on conflict (id) do update set name = excluded.name, dissertation = excluded.dissertation,
        graduating_year = excluded.graduating_year, school = excluded.school`;

    const stubs = [
      ...students.map((s) => ({ id: s.id, name: s.name, graduating_year: s.year, school: s.school })),
      ...advisors.filter((a) => !students.some((s) => s.id === a.id)).map((a) => ({ id: a.id, name: a.name, graduating_year: null, school: null })),
    ];
    stubs.sort((a, b) => a.id - b.id);
    if (stubs.length) await tx`insert into mathematicians ${tx(stubs)} on conflict (id) do nothing`;

    const relations = [...advisors.map((a) => ({ advisor: a.id, advisee: id })), ...students.map((s) => ({ advisor: id, advisee: s.id }))];
    relations.sort((a, b) => a.advisor - b.advisor || a.advisee - b.advisee);
    if (relations.length) await tx`insert into advisor_relations ${tx(relations)} on conflict do nothing`;

    await tx`insert into scrape_logs (date, page_scraped, result) values (${now}, ${id}, 'success')`;
    await tx`
      insert into crawl_schedule (page, last_crawled, next_due, interval_days, content_hash, changes)
      values (${id}, ${now}, ${next}, ${interval}, ${hash}, ${changed ? 1 : 0})
      on conflict (page) do update set last_crawled = excluded.last_crawled, next_due = excluded.next_due,
        interval_days = excluded.interval_days, content_hash = excluded.content_hash,
        crawls = crawl_schedule.crawls + 1, changes = crawl_schedule.changes + excluded.changes`;
  });

  return { status: "crawled", last_crawled: now, next_crawl: next, changed };
}
