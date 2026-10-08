/**
 * HTTP API:
 *   POST /send-dm   { to, text, idempotencyKey? } -> { messageId, clientIndex, inboxId, latencyMs }
 *   GET  /health    -> pool status
 *   GET  /metrics   -> counters + latency percentiles
 *
 * Guards: bearer token auth, address validation, text length cap,
 * token-bucket rate limit on /send-dm.
 */
import Fastify from "fastify";
import { isValidEthAddress } from "./signer.js";

export function buildServer({ config, log, pool, sender, metrics }) {
  const app = Fastify({ logger: false });

  // --- token bucket for /send-dm ---
  let tokens = config.apiRateLimitQps;
  let lastRefill = Date.now();
  function takeToken() {
    const now = Date.now();
    tokens = Math.min(config.apiRateLimitQps, tokens + ((now - lastRefill) / 1000) * config.apiRateLimitQps);
    lastRefill = now;
    if (tokens < 1) return false;
    tokens -= 1;
    return true;
  }

  app.addHook("onRequest", async (req, reply) => {
    if (req.url === "/health" || req.url === "/metrics") return;
    const auth = req.headers.authorization ?? "";
    if (auth !== `Bearer ${config.apiToken}`) {
      return reply.code(401).send({ error: "unauthorized" });
    }
  });

  app.post("/send-dm", async (req, reply) => {
    if (!takeToken()) {
      return reply.code(429).send({ error: "rate limited" });
    }
    const { to, text, idempotencyKey } = req.body ?? {};
    if (!isValidEthAddress(to)) {
      return reply.code(400).send({ error: "invalid 'to': must be 0x + 40 hex chars" });
    }
    if (typeof text !== "string" || text.length === 0 || text.length > config.maxTextLen) {
      return reply.code(400).send({ error: `'text' must be 1..${config.maxTextLen} chars` });
    }
    if (idempotencyKey !== undefined && typeof idempotencyKey !== "string") {
      return reply.code(400).send({ error: "'idempotencyKey' must be a string" });
    }
    try {
      const res = await sender.sendDm(to, text, idempotencyKey);
      return reply.send(res);
    } catch (err) {
      log.error({ err: String(err?.message ?? err) }, "send-dm failed");
      const msg = String(err?.message ?? err);
      const code = /no healthy/i.test(msg) ? 503 : 502;
      return reply.code(code).send({ error: msg });
    }
  });

  app.get("/health", async () => ({ ok: pool.healthyCount() > 0, pool: pool.status() }));
  app.get("/metrics", async () => metrics.snapshot());

  app.setErrorHandler((err, _req, reply) => {
    log.error({ err: String(err?.message ?? err) }, "request error");
    reply.code(500).send({ error: "internal error" });
  });

  return app;
}
