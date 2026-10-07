// Vitest global setup: create a throwaway database, apply every migration in ../migrations,
// load the fixture family, and hand the tests its connection string. Dropped afterwards.
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import postgres from "postgres";
import type { TestProject } from "vitest/node";

const ADMIN_URL = process.env.TEST_DATABASE_URL ?? process.env.DATABASE_URL ?? "postgres://combi@localhost:5432/combi";
const root = join(import.meta.dirname, "../../..");

declare module "vitest" {
  export interface ProvidedContext {
    databaseUrl: string;
  }
}

export default async function setup(project: TestProject) {
  const name = `combi_test_${process.pid}_${Date.now()}`;
  const admin = postgres(ADMIN_URL, { max: 1, onnotice: () => {} });
  await admin.unsafe(`create database ${name}`);

  const url = new URL(ADMIN_URL);
  url.pathname = `/${name}`;
  const db = postgres(url.toString(), { max: 1, onnotice: () => {} });
  const migrations = (await readdir(join(root, "migrations"))).filter((f) => f.endsWith(".up.sql")).sort();
  for (const f of migrations) await db.unsafe(await readFile(join(root, "migrations", f), "utf8"));
  await db.unsafe(await readFile(join(import.meta.dirname, "fixtures/family.sql"), "utf8"));
  await db.end();

  project.provide("databaseUrl", url.toString());
  return async () => {
    await admin.unsafe(`drop database ${name} with (force)`);
    await admin.end();
  };
}
