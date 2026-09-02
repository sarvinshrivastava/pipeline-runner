/**
 * Entrypoint. Order matters: config is validated before anything opens a
 * socket, so a misconfigured deploy fails loudly at boot rather than silently
 * running without auth.
 */
import { assertConfig, config } from "./config.ts";
import { openDb } from "./db.ts";
import { startScheduler } from "./scheduler.ts";
import { startServer } from "./server.ts";

assertConfig();
openDb();

const stopScheduler = await startScheduler();
const server = startServer();

console.log(`[boot] ${config.SERVICE_NAME} listening on :${config.PORT}`);

function shutdown(signal: string) {
  console.log(`[shutdown] ${signal} received`);
  stopScheduler();
  void server.stop(false);
  process.exit(0);
}

process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
