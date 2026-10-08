/**
 * xmtp-sender bootstrap.
 *
 * 1. Load config (fails fast on missing secrets).
 * 2. Create the 4-client pool (4 independent inboxes).
 * 3. Optional warmup of DM conversations.
 * 4. Serve HTTP API.
 * 5. Graceful shutdown on SIGTERM/SIGINT; never crash on send errors.
 */
import { loadConfig } from "./config.js";
import { Metrics } from "./metrics.js";
import { ClientPool } from "./pool.js";
import { Sender } from "./sender.js";
import { buildServer } from "./server.js";
import { mkdir } from "node:fs/promises";

const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };
function makeLogger(level) {
  const threshold = LEVELS[level] ?? 20;
  const emit = (lv, obj, msg) => {
    if (LEVELS[lv] < threshold) return;
    const line = { ts: new Date().toISOString(), level: lv, msg, ...obj };
    // Never log private keys or message bodies; callers are responsible,
    // this is a second line of defense for known key names.
    const s = JSON.stringify(line, (k, v) =>
      /private[_-]?key|sender[_-]?key|db[_-]?encryption[_-]?key|^text$|message/i.test(k) ? "[redacted]" : v,
    );
    (lv === "error" ? console.error : console.log)(s);
  };
  return {
    debug: (o, m) => emit("debug", o, m),
    info: (o, m) => emit("info", o, m),
    warn: (o, m) => emit("warn", o, m),
    error: (o, m) => emit("error", o, m),
  };
}

async function main() {
  const config = loadConfig();
  const log = makeLogger(config.logLevel);
  const metrics = new Metrics();

  if (config.storageMode === "file") {
    await mkdir(config.dataDir, { recursive: true });
  }

  const pool = new ClientPool(config, log, metrics);
  await pool.start();
  await pool.warmup(config.warmupAddresses);

  const sender = new Sender(pool, config, log, metrics);
  const app = buildServer({ config, log, pool, sender, metrics });
  await app.listen({ port: config.port, host: "0.0.0.0" });
  log.info(
    { port: config.port, env: config.env, storage: config.storageMode },
    "xmtp-sender listening",
  );

  let shuttingDown = false;
  async function shutdown(signal) {
    if (shuttingDown) return;
    shuttingDown = true;
    log.info({ signal }, "shutting down");
    try {
      await app.close();
    } catch { /* best effort */ }
    await pool.close();
    process.exit(0);
  }
  process.on("SIGTERM", () => shutdown("SIGTERM"));
  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("unhandledRejection", (err) => {
    // A failed send must never take down the service.
    log.error({ err: String(err?.message ?? err) }, "unhandled rejection");
  });
}

main().catch((e) => {
  console.error(JSON.stringify({ ts: new Date().toISOString(), level: "fatal", err: String(e?.message ?? e) }));
  process.exit(1);
});
