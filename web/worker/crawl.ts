import { parse, type HTMLElement } from "node-html-parser";
import type postgres from "postgres";
import { mgpUrl, RECRAWL_AFTER_DAYS, type CrawlResult } from "../shared/types";

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

export class MgpNotFound extends Error {}
export class UpstreamError extends Error {}

const ID_IN_HREF = /id\.php\?id=(\d+)/;
// the site answers unknown IDs with a 200 and this sentence
const NOT_FOUND = "You have specified an ID that does not exist in the database. Please back up and try again.";

const squash = (s: string) => s.trim().replace(/\s+/g, " ");
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

  const students: Student[] = [];
  // the first row is the header
  for (const row of doc.querySelector("table")?.querySelectorAll("tr").slice(1) ?? []) {
    const [nameCell, schoolCell, yearCell] = row.querySelectorAll("td");
    const a = nameCell?.querySelector("a");
    const student = a && hrefId(a);
    if (!a || student == null) continue;
    const year = Number(yearCell?.text.trim() || NaN);
    students.push({ id: student, name: unsurname(a.text), school: orNull(schoolCell?.text), year: Number.isInteger(year) ? year : null });
  }

  return {
    id,
    name,
    dissertation: orNull(doc.querySelector("#thesisTitle")?.text),
    school: orNull(degree?.querySelector("span")?.text),
    country: orNull(doc.querySelector("div > img")?.getAttribute("alt")),
    year: yearText ? Number(yearText) : null,
    advisors,
    students,
  };
}

export const fetchMgpPage: FetchPage = async (id) => {
  let res: Response;
  try {
    res = await fetch(mgpUrl(id), { headers: { "user-agent": "combi-crawler" } });
  } catch (e) {
    throw new UpstreamError(`Could not reach the Mathematics Genealogy Project: ${e}`);
  }
  if (!res.ok) throw new UpstreamError(`The Mathematics Genealogy Project answered ${res.status}`);
  return res.text();
};

const unique = <T>(xs: T[]) => [...new Set(xs)];
const byId = <T extends Ref>(xs: T[]) => [...new Map(xs.map((x) => [x.id, x])).values()];

/**
 * Crawl one person's page, unless it was crawled in the last RECRAWL_AFTER_DAYS days. Their
 * advisors and students are stored from what the page says about them (name, and school and
 * year for students) without being crawled themselves; a later crawl of theirs fills in the rest.
 */
export async function crawl(sql: postgres.Sql, id: number, fetchPage: FetchPage = fetchMgpPage, now = new Date()): Promise<CrawlResult> {
  const [last] = await sql<{ date: Date; result: string }[]>`
    select date, result from scrape_logs
    where page_scraped = ${id} and result in ('success', 'failed')
    order by date desc, id desc limit 1`;
  if (last && now.getTime() - new Date(last.date).getTime() < RECRAWL_AFTER_DAYS * 86_400_000) {
    if (last.result === "failed") throw new MgpNotFound();
    return { status: "fresh", last_crawled: new Date(last.date).toISOString() };
  }

  const page = parsePage(id, await fetchPage(id));
  if (!page) {
    await sql`insert into scrape_logs (date, page_scraped, result) values (${now}, ${id}, 'failed')`;
    throw new MgpNotFound();
  }

  const students = byId(page.students.filter((s) => s.id !== id));
  const advisors = byId(page.advisors.filter((a) => a.id !== id));
  const schools = unique([page.school, ...students.map((s) => s.school)].filter((s): s is string => s !== null));

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
    if (stubs.length) await tx`insert into mathematicians ${tx(stubs)} on conflict (id) do nothing`;

    const relations = [...advisors.map((a) => ({ advisor: a.id, advisee: id })), ...students.map((s) => ({ advisor: id, advisee: s.id }))];
    if (relations.length) await tx`insert into advisor_relations ${tx(relations)} on conflict do nothing`;

    await tx`insert into scrape_logs (date, page_scraped, result) values (${now}, ${id}, 'success')`;
  });

  return { status: "crawled", last_crawled: now.toISOString() };
}
