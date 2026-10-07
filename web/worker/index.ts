import { createApp } from "./app";

// Requests for /api/* reach this worker; everything else is served from the built web app
// (see `assets` in wrangler.jsonc).
export default createApp() satisfies ExportedHandler<Env>;
