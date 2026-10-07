import postgres from "postgres";
import { createApp } from "./app";
import { PARALLEL, walkStep, type WalkMessage } from "./walk";

/** between steps of a walk; MGP's load is capped by the MGP budget, not by this */
const STEP_PAUSE_S = 1;
/** after MGP couldn't be reached */
const RETRY_PAUSE_S = 60;

const app = createApp();

// Requests for /api/* reach this worker; everything else is served from the built web app
// (see `assets` in wrangler.jsonc). Crawl walks advance through CRAWL_QUEUE, a step per message,
// each step queueing the next until the walk is done.
export default {
  fetch: app.fetch,

  async queue(batch, env) {
    const sql = postgres(env.HYPERDRIVE.connectionString, { max: PARALLEL, fetch_types: false });
    try {
      for (const message of batch.messages) {
        const next = await walkStep(sql, message.body.walk);
        if (next !== "done") await env.CRAWL_QUEUE!.send(message.body, { delaySeconds: next === "wait" ? RETRY_PAUSE_S : STEP_PAUSE_S });
        message.ack();
      }
    } finally {
      await sql.end();
    }
  },
} satisfies ExportedHandler<Env, WalkMessage>;
