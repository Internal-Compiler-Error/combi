interface Env {
  /** Postgres via Hyperdrive in production; `localConnectionString` in wrangler.jsonc in development */
  HYPERDRIVE: { connectionString: string };
  /** caps requests that reach MGP (crawls and MGP searches) per visitor; absent in tests */
  MGP_LIMITER?: RateLimit;
  /** advances crawl walks one step per message (see walk.ts); absent in tests */
  CRAWL_QUEUE?: Queue<import("./walk").WalkMessage>;
}
