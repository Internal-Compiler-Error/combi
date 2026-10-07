import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, inject, test, vi } from "vitest";
import type { CrawlResult, PersonDetail } from "../shared/types";
import { createApp } from "./app";
import { parsePage } from "./crawl";

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
});

describe("crawl", () => {
  const fetchPage = vi.fn(async (id: number) => pages[id] ?? missing);
  const app = createApp({ fetchPage });
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

  test("rejects IDs out of range", async () => expect((await post(0)).status).toBe(400));
});
