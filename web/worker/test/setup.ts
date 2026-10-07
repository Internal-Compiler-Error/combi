// Vitest global setup: create throwaway databases with every migration in ../migrations applied,
// and hand the tests their connection strings. Dropped afterwards. `databaseUrl` also holds the
// fixture family; `emptyDatabaseUrl` is for tests that write, so they can't disturb the others.
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import postgres from "postgres";
import type { TestProject } from "vitest/node";

const ADMIN_URL = process.env.TEST_DATABASE_URL ?? process.env.DATABASE_URL ?? "postgres://combi@localhost:5432/combi";
const root = join(import.meta.dirname, "../../..");

declare module "vitest" {
  export interface ProvidedContext {
    databaseUrl: string;
    emptyDatabaseUrl: string;
  }
}

export default async function setup(project: TestProject) {
  const admin = postgres(ADMIN_URL, { max: 1, onnotice: () => {} });
  const migrations = (await readdir(join(root, "migrations"))).filter((f) => f.endsWith(".up.sql")).sort();
  const names: string[] = [];

  async function createDatabase(fixture?: string) {
    const name = `combi_test_${process.pid}_${Date.now()}_${names.length}`;
    await admin.unsafe(`create database ${name}`);
    names.push(name);

    const url = new URL(ADMIN_URL);
    url.pathname = `/${name}`;
    const db = postgres(url.toString(), { max: 1, onnotice: () => {} });
    for (const f of migrations) await db.unsafe(await readFile(join(root, "migrations", f), "utf8"));
    if (fixture) await db.unsafe(await readFile(join(import.meta.dirname, "fixtures", fixture), "utf8"));
    await db.end();
    return url.toString();
  }

  project.provide("databaseUrl", await createDatabase("family.sql"));
  project.provide("emptyDatabaseUrl", await createDatabase());
  return async () => {
    for (const name of names) await admin.unsafe(`drop database ${name} with (force)`);
    await admin.end();
  };
}
