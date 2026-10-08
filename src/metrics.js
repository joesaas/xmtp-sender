/** Minimal in-memory metrics: counters + latency samples. No message content. */
export class Metrics {
  constructor() {
    this.sendsTotal = 0;
    this.sendsOk = 0;
    this.sendsFailed = 0;
    this.retries = 0;
    this.rebuilds = 0;
    this.rotations = 0;
    this.revokeFailures = 0;
    this.latencyMs = []; // ring buffer of recent send latencies
    this.startedAt = Date.now();
    this.perClient = new Map(); // index -> { ok, failed }
  }

  recordSend(index, ok, latencyMs) {
    this.sendsTotal++;
    if (ok) this.sendsOk++;
    else this.sendsFailed++;
    const c = this.perClient.get(index) ?? { ok: 0, failed: 0 };
    if (ok) c.ok++;
    else c.failed++;
    this.perClient.set(index, c);
    this.latencyMs.push(latencyMs);
    if (this.latencyMs.length > 1000) this.latencyMs.shift();
  }

  percentile(p) {
    if (!this.latencyMs.length) return 0;
    const s = [...this.latencyMs].sort((a, b) => a - b);
    return Math.round(s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))]);
  }

  snapshot() {
    const perClient = {};
    for (const [k, v] of this.perClient) perClient[`client_${k}`] = v;
    return {
      uptimeSec: Math.round((Date.now() - this.startedAt) / 1000),
      sendsTotal: this.sendsTotal,
      sendsOk: this.sendsOk,
      sendsFailed: this.sendsFailed,
      retries: this.retries,
      rebuilds: this.rebuilds,
      rotations: this.rotations,
      revokeFailures: this.revokeFailures,
      latencyMs: { p50: this.percentile(50), p95: this.percentile(95), p99: this.percentile(99) },
      perClient,
    };
  }
}
