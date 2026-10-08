/**
 * Send pipeline: pick a random healthy client -> get/create DM -> sendText.
 *
 * - Per-client concurrency semaphore (libxmtp serializes same-group sends
 *   internally via its per-group locks; the semaphore bounds intent buildup).
 * - Per-send timeout; on failure retry with a *different* client (up to
 *   SEND_MAX_RETRIES), then surface the error.
 * - Idempotency: caller-supplied key wins; otherwise sha256(to|text|minute),
 *   so a retried HTTP request within the same minute dedupes server-side
 *   (SDK enforces idempotency_key uniqueness per group).
 */
import { createHash } from "node:crypto";

function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    timer.unref?.();
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

export class Sender {
  constructor(pool, config, log, metrics) {
    this.pool = pool;
    this.config = config;
    this.log = log;
    this.metrics = metrics;
    // per-client semaphore counters live on the ManagedClient (inflight)
  }

  defaultIdempotencyKey(to, text) {
    const minute = Math.floor(Date.now() / 60000);
    return createHash("sha256").update(`${to.toLowerCase()}|${text}|${minute}`).digest("hex");
  }

  async acquire(client) {
    while (client.inflight >= this.config.clientConcurrency) {
      await new Promise((r) => setTimeout(r, 10));
    }
    client.inflight++;
  }

  release(client) {
    client.inflight = Math.max(0, client.inflight - 1);
  }

  async sendDm(to, text, idempotencyKey) {
    const key = idempotencyKey || this.defaultIdempotencyKey(to, text);
    const tried = new Set();
    let lastErr = null;

    for (let attempt = 0; attempt <= this.config.sendMaxRetries; attempt++) {
      const client = this.pool.pick();
      if (!client) {
        lastErr = new Error("no healthy XMTP client available");
        break;
      }
      if (tried.has(client.index) && this.pool.healthyCount() > tried.size) continue;
      tried.add(client.index);

      const started = Date.now();
      await this.acquire(client);
      try {
        const dm = await withTimeout(client.getDm(to), this.config.sendTimeoutMs, "getDm");
        const messageId = await withTimeout(
          dm.sendText(text, { idempotencyKey: key }),
          this.config.sendTimeoutMs,
          "sendText",
        );
        const latency = Date.now() - started;
        client.recordSuccess();
        this.metrics.recordSend(client.index, true, latency);
        return { messageId, clientIndex: client.index, inboxId: client.inboxId, latencyMs: latency };
      } catch (err) {
        const latency = Date.now() - started;
        lastErr = err;
        client.recordFailure(err);
        this.metrics.recordSend(client.index, false, latency);
        if (attempt < this.config.sendMaxRetries) {
          this.metrics.retries++;
          this.log.warn(
            { attempt: attempt + 1, client: client.index, err: String(err?.message ?? err) },
            "send failed, retrying with another client",
          );
        }
      } finally {
        this.release(client);
      }
    }
    throw lastErr ?? new Error("send failed");
  }
}
