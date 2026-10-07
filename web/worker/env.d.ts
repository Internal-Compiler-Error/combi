interface Env {
  /** Postgres via Hyperdrive in production; `localConnectionString` in wrangler.jsonc in development */
  HYPERDRIVE: { connectionString: string };
  /** caps crawl requests per visitor; absent in tests */
  CRAWL_LIMITER?: RateLimit;
}
