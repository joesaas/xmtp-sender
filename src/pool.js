/**
 * Pool of 4 managed clients.
 *
 * - Random routing among healthy clients (per spec). Because the 4 clients are
 *   4 independent inboxes, each recipient maps to 4 independent DM groups, so
 *   random selection also spreads libxmtp's per-group lock contention.
 * - Rolling generational rotation: rebuild clients one at a time on a timer.
 *   In `file` storage mode this is the history janitor (fresh DB file);
 *   in `memory` mode it is a periodic self-heal.
 */
import { ManagedClient } from "./xmtpClient.js";

export class ClientPool {
  constructor(config, log, metrics) {
    this.config = config;
    this.log = log;
    this.metrics = metrics;
    this.clients = config.signers.map(
      (entry, i) => new ManagedClient(i, entry, config, log, metrics),
    );
    this.rotationTimer = null;
    this.rotationCursor = 0;
    this.rotating = false;
  }

  async start() {
    // Create clients in parallel; each registration is independent.
    await Promise.all(this.clients.map((c) => c.start()));
    this.log.info("all 4 clients ready");
    if (this.config.rotationIntervalMs > 0) {
      this.rotationTimer = setInterval(() => this.rotateOne().catch(() => {}), this.config.rotationIntervalMs);
      this.rotationTimer.unref?.();
    }
  }

  /** Uniform random pick among healthy clients. Returns null if none healthy. */
  pick() {
    const healthy = this.clients.filter((c) => c.healthy);
    if (!healthy.length) return null;
    return healthy[Math.floor(Math.random() * healthy.length)];
  }

  healthyCount() {
    return this.clients.filter((c) => c.healthy).length;
  }

  /**
   * Rotate a single client, round-robin: every client gets its turn, so each
   * client's in-memory DB (and DM cache) is periodically dropped and rebuilt.
   * This is what bounds memory growth — without it, group_messages /
   * group_intents would accumulate forever.
   */
  async rotateOne() {
    if (this.rotating) return;
    this.rotating = true;
    try {
      for (let i = 0; i < this.clients.length; i++) {
        const idx = (this.rotationCursor + i) % this.clients.length;
        const candidate = this.clients[idx];
        if (candidate.state === "closed") continue;
        this.rotationCursor = (idx + 1) % this.clients.length;
        this.metrics.rotations++;
        this.log.info({ client: candidate.index }, "scheduled rotation");
        await candidate.rebuild("scheduled-rotation");
        return;
      }
    } finally {
      this.rotating = false;
    }
  }

  async warmup(addresses) {
    if (!addresses.length) return;
    await Promise.all(
      this.clients.map(async (c) => {
        for (const a of addresses) {
          try {
            await c.getDm(a);
          } catch (e) {
            this.log.warn({ client: c.index, err: String(e?.message ?? e) }, "warmup failed for address");
          }
        }
      }),
    );
    this.log.info({ count: addresses.length }, "warmup done");
  }

  async close() {
    if (this.rotationTimer) clearInterval(this.rotationTimer);
    await Promise.all(this.clients.map((c) => c.close()));
  }

  status() {
    return {
      healthy: this.healthyCount(),
      total: this.clients.length,
      clients: this.clients.map((c) => c.status()),
    };
  }
}
