import { describe, expect, inject, test } from "vitest";
import type { CountryStat, Flows, Graph, Person, Relation, PersonDetail, SchoolDetail, SchoolHit, SchoolStat, Stats } from "../shared/types";
import { createApp, normalizeQuery } from "./app";

// The database holds tests/fixtures/family.sql: a small slice of a real lineage with one
// student who has two advisors (Klein) and one person unconnected to the rest (Gödel).
const app = createApp();
const env = { HYPERDRIVE: { connectionString: inject("databaseUrl") } };
const get = (path: string) => app.request(`/api${path}`, {}, env);
const json = async <T>(path: string): Promise<T> => {
  const res = await get(path);
  expect(res.status).toBe(200);
  return res.json() as Promise<T>;
};
const search = async (q: string) => (await json<Person[]>(`/search?q=${encodeURIComponent(q)}`)).map((p) => p.id);

describe("search", () => {
  test("ignores accents and case", async () => {
    expect(await search("GODEL")).toEqual([6]);
    expect(await search("gauss")).toEqual([1]);
  });
  test("skips middle names", async () => expect(await search("felix klein")).toEqual([4]));
  test("tolerates typos", async () => expect(await search("lipshitz")).toEqual([5]));
  test("ranks an exact ID first", async () => expect((await search("4"))[0]).toBe(4));
  test("treats wildcards literally", async () => {
    expect(await search("%")).toEqual([]);
    expect(await search("   ")).toEqual([]);
  });
});

describe("person", () => {
  test("lists advisors, students and descendants", async () => {
    const p = await json<PersonDetail>("/mathematicians/4");
    expect(p).toMatchObject({ name: "C. Felix Klein", school_id: 3, country: "Germany", student_count: 1, descendant_count: 1 });
    expect(p.advisors.map((a) => a.id)).toEqual([3, 5]);
    expect(p.students.map((s) => s.id)).toEqual([7]);
  });
  test("counts each descendant once", async () => expect((await json<PersonDetail>("/mathematicians/1")).descendant_count).toBe(4));
  test("turns a blank dissertation into null", async () => expect((await json<PersonDetail>("/mathematicians/5")).dissertation).toBeNull());
  test("is 404 when unknown", async () => {
    expect((await get("/mathematicians/999")).status).toBe(404);
    expect((await get("/mathematicians/999/graph")).status).toBe(404);
  });
});

test("graph walks the requested generations", async () => {
  const g = await json<Graph>("/mathematicians/3/graph?up=1&down=1");
  // Lipschitz (5) advises Klein but is not an ancestor of Plücker, so he is not walked
  expect(g.nodes.map((n) => [n.id, n.depth]).sort()).toEqual([[2, -1], [3, 0], [4, 1]]);
  expect(g.links.map((l) => [l.advisor, l.student]).sort()).toEqual([[2, 3], [3, 4]]);
  expect(g.truncated).toBe(false);
});

test("stats summarise the database", async () => {
  expect(await json<Stats>("/stats")).toEqual({
    mathematicians: 7,
    relations: 5,
    countries: 2,
    first_year: 1799,
    last_year: 1929,
    last_scraped: "2026-10-01T12:00:00.000Z",
  });
});

test("notable ranks by student count", async () => {
  const n = await json<Person[]>("/notable");
  expect(n).toHaveLength(5);
  expect(n.every((p) => p.student_count === 1)).toBe(true);
});

test("countries count mathematicians by their school's country", async () => {
  expect(await json<CountryStat[]>("/countries")).toEqual([
    { country: "Germany", name: "Germany", mathematicians: 5, schools: 3 },
    { country: "Austria", name: "Austria", mathematicians: 1, schools: 1 },
  ]);
  // school ids follow the fixture's insert order
  expect(await json<SchoolStat[]>("/countries/Germany/schools")).toEqual([
    { id: 3, school: "Universität Bonn", mathematicians: 2 },
    { id: 1, school: "Universität Helmstedt", mathematicians: 2 },
    { id: 2, school: "Universität Marburg", mathematicians: 1 },
  ]);
  expect(await json<SchoolStat[]>("/countries/Atlantis/schools")).toEqual([]);
});

describe("schools", () => {
  test("list their graduates, newest first, with degrees by decade", async () => {
    expect(await json<SchoolDetail>("/schools/3")).toMatchObject({
      name: "Universität Bonn",
      countries: [{ country: "Germany", name: "Germany" }],
      mathematicians: 2,
      first_year: 1853,
      last_year: 1868,
      decades: [
        { decade: 1850, count: 1 },
        { decade: 1860, count: 1 },
      ],
      people: [{ id: 4 }, { id: 5 }],
    });
    expect((await get("/schools/999")).status).toBe(404);
  });

  test("are searchable ignoring accents", async () => {
    expect((await json<SchoolHit[]>("/schools/search?q=bonn")).map((s) => s.id)).toEqual([3]);
    expect(await json<SchoolHit[]>("/schools/search?q=wien")).toEqual([{ id: 4, name: "Universität Wien", country: "Austria", mathematicians: 1 }]);
    expect((await json<SchoolHit[]>("/schools/search?q=universitat")).length).toBe(4);
  });
});

describe("relation", () => {
  const relate = (a: number, b: number) => json<Relation>(`/relation?a=${a}&b=${b}`);
  const ids = (people: Person[]) => people.map((p) => p.id);

  test("finds the nearest shared ancestor, even when it is one of them", async () => {
    // Lindemann (7) studied under Klein (4), a student of Lipschitz (5)
    const r = await relate(7, 5);
    expect(r.ancestor?.id).toBe(5);
    expect(ids(r.pathA)).toEqual([5, 4, 7]);
    expect(ids(r.pathB)).toEqual([5]);
  });

  test("follows the shortest line down", async () => {
    const r = await relate(7, 2);
    expect(r.ancestor?.id).toBe(2);
    expect(ids(r.pathA)).toEqual([2, 3, 4, 7]);
  });

  test("says when the data links them nowhere", async () => {
    const r = await relate(6, 1);
    expect(r).toMatchObject({ ancestor: null, pathA: [], pathB: [], a: { id: 6 }, b: { id: 1 } });
  });

  test("is 404 for someone not in the database and 400 without two IDs", async () => {
    expect((await get("/relation?a=1&b=999")).status).toBe(404);
    expect((await get("/relation?a=1")).status).toBe(400);
  });
});

test("flows only count links that cross a border", async () => {
  // everyone linked in the fixture studied in Germany
  expect(await json<Flows>("/flows")).toEqual({ flows: [], decades: [] });
});

test("helpers", () => {
  expect(normalizeQuery("  50%   off_by\\one ")).toBe("50\\% off\\_by\\\\one");
});
