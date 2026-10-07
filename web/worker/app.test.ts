import { describe, expect, inject, test } from "vitest";
import type { Graph, Person, PersonDetail, Stats } from "../shared/types";
import { createApp, normalizeQuery, prettyCountry } from "./app";

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
    expect(p).toMatchObject({ name: "C. Felix Klein", country: "Germany", student_count: 1, descendant_count: 1 });
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

test("helpers", () => {
  expect(prettyCountry("UnitedStates")).toBe("United States");
  expect(prettyCountry("HongKong")).toBe("Hong Kong");
  expect(normalizeQuery("  50%   off_by\\one ")).toBe("50\\% off\\_by\\\\one");
});
