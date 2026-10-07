import { readFileSync } from "node:fs";
import { join } from "node:path";
import postgres from "postgres";
import { afterAll, describe, expect, inject, test, vi } from "vitest";
import type { CrawlResult, MgpHit, PersonDetail, Relation, WalkStatus } from "../shared/types";
import { createApp } from "./app";
import { crawl, firstInterval, mgpQuery, nextInterval, parsePage, parseSearchResults, type MgpQuery } from "./crawl";
import { startWalk, walkStep } from "./walk";
import { mgpTurn, MGP_REQUESTS_PER_SECOND } from "./mgp-budget";

const fixture = (name: string) => readFileSync(join(import.meta.dirname, "test/fixtures/mgp", `${name}.html`), "utf8");
const pages: Record<number, string> = { 10416: fixture("knuth"), 135101: fixture("rajesh") };
const missing = "<html><body><p>You have specified an ID that does not exist in the database. Please back up and try again.</p></body></html>";

describe("parsePage", () => {
  test("knuth", () => {
    const knuth = parsePage(10416, fixture("knuth"))!;
    expect(knuth).toMatchObject({
      name: "Donald Ervin Knuth",
      dissertation: "Finite Semifields and Projective Planes",
      school: "California Institute of Technology",
      country: "UnitedStates",
      year: 1963,
      advisors: [{ id: 6807, name: "Marshall Hall, Jr." }],
    });
    expect(knuth.students[0]).toEqual({ id: 61940, name: "Bruce Baumgart", school: "Stanford University", year: 1974 });
  });

  test("rajesh", () => {
    const rajesh = parsePage(1, fixture("rajesh"))!;
    expect(rajesh).toMatchObject({
      name: "Rajesh Pereira",
      dissertation: "Trace Vectors in Matrix Analysis",
      school: "University of Toronto",
      country: "Canada",
      year: 2003,
      advisors: [{ id: 15957, name: "Man-Duen Choi" }],
    });
    expect(rajesh.students).toEqual([
      { id: 235835, name: "George Hutchinson", school: "University of Guelph", year: 2018 },
      { id: 197636, name: "Jeremy Levick", school: "University of Guelph", year: 2015 },
      { id: 190371, name: "Preeti Mohindru", school: "University of Guelph", year: 2014 },
      { id: 190372, name: "Jeffrey Tsang", school: "University of Guelph", year: 2014 },
    ]);
  });

  test("tai-yih", () => {
    const tai = parsePage(1, fixture("Tai-Yih"))!;
    expect(tai.name).toBe("Tai-Yih Tso");
    expect(tai.students).toEqual([]);
  });

  test("an unknown advisor and no dissertation", () => {
    const abu = parsePage(1, fixture("abu"))!;
    expect(abu.name).toBe("Abu Sahl 'Isa ibn Yahya al-Masihi");
    expect(abu.dissertation).toBeNull();
    expect(abu.advisors).toEqual([]);
  });

  test("an ID MGP doesn't have", () => expect(parsePage(1, missing)).toBeNull());

  test("a year that can't be right is unknown", () => {
    expect(parsePage(261324, fixture("knuth").replace(">1963</span>", ">200</span>"))!.year).toBeNull();
  });
});

describe("MGP search", () => {
  test("parses the results page", () => {
    const hits = parseSearchResults(fixture("search-knuth"));
    expect(hits.map((h) => h.id)).toEqual([10416, 116483, 294297, 199949, 217934, 63323]);
    expect(hits[0]).toEqual({ id: 10416, name: "Donald Knuth", school: "California Institute of Technology", year: 1963 });
    expect(hits[2]).toEqual({ id: 294297, name: "Eric Knuth", school: null, year: null });
    expect(hits[3]!.school).toBe("Georg-August-Universität Göttingen");
  });

  test("splits a query into given and family names", () => {
    expect(mgpQuery("knuth")).toEqual({ family_name: "knuth" });
    expect(mgpQuery(" donald  ervin knuth ")).toEqual({ given_name: "donald", family_name: "knuth" });
    expect(mgpQuery("  ")).toBeNull();
  });
});

describe("crawl", () => {
  const fetchPage = vi.fn(async (id: number) => pages[id] ?? missing);
  const fetchSearch = vi.fn(async (_: MgpQuery) => fixture("search-knuth"));
  const app = createApp({ fetchPage, fetchSearch });
  const env = { HYPERDRIVE: { connectionString: inject("emptyDatabaseUrl") } };
  const post = (id: number) => app.request(`/api/mathematicians/${id}/crawl`, { method: "POST" }, env);
  const person = async (id: number) => (await app.request(`/api/mathematicians/${id}`, {}, env)).json() as Promise<PersonDetail>;

  test("stores the person, their advisors and their students", async () => {
    const res = await post(10416);
    expect(res.status).toBe(200);
    expect(((await res.json()) as CrawlResult).status).toBe("crawled");

    const knuth = await person(10416);
    expect(knuth).toMatchObject({ name: "Donald Ervin Knuth", year: 1963, country: "United States", advisors: [{ id: 6807 }] });
    expect(knuth.last_crawled).not.toBeNull();
    expect(knuth.students.length).toBe(parsePage(10416, pages[10416]!)!.students.length);

    // a student is known from Knuth's list but not crawled yet
    const student = await person(61940);
    expect(student).toMatchObject({ name: "Bruce Baumgart", year: 1974, school: "Stanford University", last_crawled: null });
  });

  test("doesn't fetch a page crawled in the last 14 days", async () => {
    fetchPage.mockClear();
    const res = await post(10416);
    expect(((await res.json()) as CrawlResult).status).toBe("fresh");
    expect(fetchPage).not.toHaveBeenCalled();
  });

  test("links a crawled person to an advisor known only by name", async () => {
    await post(135101);
    const rajesh = await person(135101);
    expect(rajesh.dissertation).toBe("Trace Vectors in Matrix Analysis");
    expect(rajesh.advisors).toMatchObject([{ id: 15957, name: "Man-Duen Choi" }]);
  });

  test("is 404 for an ID MGP doesn't have, and remembers it", async () => {
    expect((await post(999_999)).status).toBe(404);
    fetchPage.mockClear();
    expect((await post(999_999)).status).toBe(404);
    expect(fetchPage).not.toHaveBeenCalled();
  });

  test("MGP search marks who is in the database", async () => {
    // runs after Knuth's crawl above, which stored him but none of the other Knuths
    const res = await app.request("/api/mgp/search?q=knuth", {}, env);
    const hits = (await res.json()) as MgpHit[];
    expect(fetchSearch).toHaveBeenCalledWith({ family_name: "knuth" });
    expect(hits.find((h) => h.id === 10416)).toMatchObject({ known: true, last_crawled: expect.any(String) });
    expect(hits.find((h) => h.id === 116483)).toMatchObject({ known: false, last_crawled: null });
  });

  test("two students of the same advisor are related through them", async () => {
    const r = (await (await app.request("/api/relation?a=61940&b=47202", {}, env)).json()) as Relation;
    expect(r.ancestor?.id).toBe(10416);
    expect(r.pathA.map((p) => p.id)).toEqual([10416, 61940]);
    expect(r.pathB.map((p) => p.id)).toEqual([10416, 47202]);
  });

  test("rejects IDs out of range", async () => expect((await post(0)).status).toBe(400));
});

describe("recrawl schedule", () => {
  const year = new Date().getUTCFullYear();
  const page = (y: number | null, studentYears: number[]) => ({ ...parsePage(1, fixture("Tai-Yih"))!, year: y, students: studentYears.map((s, i) => ({ id: i, name: "", school: null, year: s })) });

  test("starts short for people still taking students, long for settled pages", () => {
    expect(firstInterval(page(year - 3, []), new Date())).toBe(14);
    expect(firstInterval(page(1960, [year - 2]), new Date())).toBe(14);
    expect(firstInterval(page(1960, [1990]), new Date())).toBe(60);
    expect(firstInterval(page(1900, [1930]), new Date())).toBe(180);
  });

  test("halves on a change and doubles otherwise, within bounds", () => {
    expect(nextInterval(14, false)).toBe(28);
    expect(nextInterval(14, true)).toBe(7);
    expect(nextInterval(4, true)).toBe(3);
    expect(nextInterval(300, false)).toBe(365);
  });
});

describe("adaptive recrawling and walks", () => {
  const sql = postgres(inject("emptyDatabaseUrl"), { max: 1, fetch_types: false, onnotice: () => {} });
  afterAll(() => sql.end());
  const DAY = 86_400_000;
  const missing = "<html><body><p>You have specified an ID that does not exist in the database. Please back up and try again.</p></body></html>";
  let html = fixture("Tai-Yih");
  const fetchPage = vi.fn(async (id: number) => (id === 777 ? html : missing));

  test("a page that doesn't change is checked less and less often, one that does more often", async () => {
    const t0 = new Date("2026-01-01T00:00:00Z");
    const first = await crawl(sql, 777, fetchPage, t0);
    expect(first).toMatchObject({ status: "crawled", changed: null });
    const days = (from: Date, to: Date) => (to.getTime() - from.getTime()) / DAY;
    const firstGap = days(first.last_crawled, first.next_crawl);

    expect((await crawl(sql, 777, fetchPage, new Date(t0.getTime() + DAY))).status).toBe("fresh");

    const t1 = first.next_crawl;
    const second = await crawl(sql, 777, fetchPage, t1);
    expect(second).toMatchObject({ status: "crawled", changed: false });
    expect(days(t1, second.next_crawl)).toBe(firstGap * 2);

    html = html.replace("</h2>", " Jr.</h2>");
    const t2 = second.next_crawl;
    const third = await crawl(sql, 777, fetchPage, t2);
    expect(third).toMatchObject({ status: "crawled", changed: true });
    expect(days(t2, third.next_crawl)).toBe(firstGap);
  });

  test("the crawl button starts a walk once and queues its first step", async () => {
    const send = vi.fn();
    const app = createApp({ fetchPage: async (id) => pages[id] ?? missing });
    const env = { HYPERDRIVE: { connectionString: inject("emptyDatabaseUrl") }, CRAWL_QUEUE: { send } };
    const res = (await (await app.request("/api/mathematicians/10416/crawl", { method: "POST" }, env)).json()) as CrawlResult;
    expect(res.walk).toMatchObject({ root: 10416, status: "running", todo: 1 });
    expect(send).toHaveBeenCalledWith({ walk: res.walk!.id });

    const again = (await (await app.request("/api/mathematicians/10416/crawl", { method: "POST" }, env)).json()) as CrawlResult;
    expect(again.walk!.id).toBe(res.walk!.id);
    expect(send).toHaveBeenCalledTimes(1);
  });

  test("a walk goes down through students and up through advisors until it runs out", async () => {
    const { walk } = await startWalk(sql, 10416);
    let step;
    while ((step = await walkStep(sql, walk.id, { fetchPage: async (id) => pages[id] ?? missing })) === "more");
    expect(step).toBe("done");
    const [w] = await sql<WalkStatus[]>`select status, fetched, skipped, failed from crawl_walks where id = ${walk.id}`;
    const students = parsePage(10416, pages[10416]!)!.students.length;
    // Knuth was just crawled, so he is skipped; his students and his advisor aren't on MGP in this test
    expect(w).toMatchObject({ status: "done", fetched: 0, skipped: 1, failed: students + 1 });
  });

  test("a walk stops at its fetch limit", async () => {
    const { walk } = await startWalk(sql, 135101);
    expect(await walkStep(sql, walk.id, { fetchPage: async (id) => pages[id] ?? missing, maxFetches: 0 })).toBe("done");
    const [w] = await sql<WalkStatus[]>`select status from crawl_walks where id = ${walk.id}`;
    expect(w!.status).toBe("capped");
  });
});

test("every request to MGP shares one budget of a few a second", async () => {
  const sql = postgres(inject("emptyDatabaseUrl"), { max: 4, fetch_types: false, onnotice: () => {} });
  try {
    // let the bucket fill, then ask for two bursts' worth at once, as parallel walks would
    await new Promise((r) => setTimeout(r, 1100));
    const start = Date.now();
    await Promise.all(Array.from({ length: 2 * MGP_REQUESTS_PER_SECOND }, () => mgpTurn(sql)));
    const seconds = (Date.now() - start) / 1000;
    expect(seconds).toBeGreaterThan(0.7);
    expect(seconds).toBeLessThan(2.5);
  } finally {
    await sql.end();
  }
});
