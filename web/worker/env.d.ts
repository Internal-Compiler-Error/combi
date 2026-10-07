interface Env {
  /** Postgres via Hyperdrive in production; `localConnectionString` in wrangler.jsonc in development */
  HYPERDRIVE: { connectionString: string };
}
