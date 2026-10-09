/**
 * One managed XMTP client: lifecycle (create/close/rebuild), health, DM cache.
 *
 * Design notes from libxmtp source:
 * - `Client.create` registers the installation (wallet signature, one-time per boot).
 * - `close()` is idempotent; call it before dropping the reference.
 * - `close()` does NOT release the network connection: it cancels workers and
 *   disconnects the DB; the gRPC channel dies only when the last Rust-side
 *   reference drops (GC of the JS wrapper). So refresh = drain in-flight work,
 *   retire, close, null every reference, and let refcounting reclaim the
 *   connection. Watch `openFds` in /metrics: it must return to baseline after
 *   each rebuild; a monotonic climb means a stale reference somewhere.
 * - Installation hygiene is precise, never wholesale: a retiring client revokes
 *   ONLY its own installation id (`revokeInstallations([ownId])`), never
 *   `revokeAllOtherInstallations()` — the latter would kick unrelated
 *   installations of the same wallet (e.g. a second instance mid-deploy).
 *   If the self-revoke fails (usually the network fault that triggered the
 *   rebuild), the id is queued in a persisted pending list and the next
 *   generation revokes exactly that id after it builds.
 * - `dbPath: null` => pure in-memory DB: no disk growth, nothing to clean up.
 * - `useSingleConnection: true` is the SDK's recommended mode for many clients
 *   in one process.
 */
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { Client } from "@xmtp/node-sdk";

const ETHEREUM = 0;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Race a promise against a timeout; the loser keeps running in background. */
async function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

const toHex = (bytes) => Buffer.from(bytes).toString("hex");
const fromHex = (hex) => Uint8Array.from(Buffer.from(hex, "hex"));

export class ManagedClient {
  constructor(index, entry, config, log, metrics) {
    this.index = index;
    this.entry = entry; // { signer, address }
    this.config = config;
    this.log = log;
    this.metrics = metrics;
    this.client = null;
    this.inboxId = null;
    this.state = "init"; // init | healthy | unhealthy | rebuilding | closed
    this.consecutiveFailures = 0;
    this.inflight = 0;
    this.dmCache = new Map(); // addressLower -> Dm (LRU, capped)
    this.maxDmCacheSize = config.maxDmCacheSize ?? 10000;
    this.rebuildTimer = null;
    this.lastRebuildAt = 0;
    // Installation ids (hex) this lineage retired but could not revoke yet.
    // Persisted so a process restart can still clean up exactly its own
    // previous installation — and nothing else.
    this.pendingRevokes = new Set();
    this.loadPendingRevokes();
  }

  pendingRevokesPath() {
    return `${this.config.dataDir}/pending-revokes-${this.index}.json`;
  }

  loadPendingRevokes() {
    try {
      const arr = JSON.parse(readFileSync(this.pendingRevokesPath(), "utf8"));
      if (Array.isArray(arr)) {
        for (const h of arr) if (typeof h === "string") this.pendingRevokes.add(h);
      }
    } catch { /* no file yet: fine */ }
  }

  persistPendingRevokes() {
    try {
      mkdirSync(this.config.dataDir, { recursive: true });
      writeFileSync(this.pendingRevokesPath(), JSON.stringify([...this.pendingRevokes]));
    } catch { /* best effort: in-memory set still applies for this process */ }
  }

  get healthy() {
    return this.state === "healthy";
  }

  dbPath() {
    if (this.config.storageMode === "file") {
      // Generational files: each rebuild gets a fresh DB; old file is deleted.
      return `${this.config.dataDir}/client-${this.index}-${Date.now()}.db3`;
    }
    return null; // in-memory
  }

  /**
   * Boot is fault-tolerant per client: a failed initial registration (e.g.
   * one transient network error) must not take down the whole service.
   * The client starts `unhealthy` and heals via the normal backoff rebuild;
   * the pool (and HTTP API) come up regardless.
   */
  async start() {
    try {
      await this.build();
    } catch (e) {
      this.consecutiveFailures++;
      this.state = "unhealthy";
      this.log.error(
        { client: this.index, err: String(e?.message ?? e) },
        "initial client build failed; will retry in background",
      );
      this.scheduleRebuild("boot-failed");
    }
  }

  async build() {
    const { signer, address } = this.entry;
    this.log.info({ client: this.index, address }, "creating XMTP client");
    const client = await Client.create(signer, {
      env: this.config.env,
      dbPath: this.dbPath(),
      dbEncryptionKey: this.config.dbEncryptionKey,
      useSingleConnection: true,
      disableDeviceSync: true,
      loggingLevel: this.config.xmtpLogLevel,
    });
    // Clean up the previous generation's DB file (file mode only).
    this.client = client;
    this.inboxId = client.inboxId;
    this.state = "healthy";
    this.consecutiveFailures = 0;
    this.dmCache.clear();
    this.log.info(
      { client: this.index, inboxId: this.inboxId, registered: client.isRegistered },
      "client ready",
    );
    // Precise installation hygiene: revoke exactly the ids this lineage
    // retired earlier but could not revoke at retire time (persisted queue).
    // Normally empty — retire-time self-revoke handles the common path.
    await this.flushPendingRevokes();
  }

  /**
   * Revoke ONLY this client's own installation, while it is still alive to
   * sign/publish the identity update. Best-effort with a hard timeout: on
   * failure the id joins the persisted pending queue and the next generation
   * revokes exactly that id (see flushPendingRevokes). Never touches any
   * other installation of this wallet.
   */
  async retireSelf() {
    const client = this.client;
    if (!client) return;
    let idHex;
    try {
      const idBytes = client.installationIdBytes;
      idHex = toHex(idBytes);
      await withTimeout(
        client.revokeInstallations([idBytes]),
        this.config.revokeTimeoutMs,
        "revokeInstallations",
      );
      this.metrics.revokesOk++;
      this.log.info({ client: this.index, installation: idHex }, "revoked own installation");
    } catch (e) {
      this.metrics.revokeFailures++;
      if (idHex) {
        this.pendingRevokes.add(idHex);
        this.persistPendingRevokes();
      }
      this.log.warn(
        { client: this.index, err: String(e?.message ?? e) },
        "self-revoke failed; queued for next generation",
      );
    }
  }

  /**
   * After a fresh build: revoke exactly the queued ids from earlier
   * generations of THIS client slot. One batched call, ids only ever come
   * from our own lineage — unrelated installations are never touched.
   */
  async flushPendingRevokes() {
    if (!this.client || this.pendingRevokes.size === 0) return;
    const currentHex = toHex(this.client.installationIdBytes);
    const ids = [...this.pendingRevokes].filter((h) => h !== currentHex);
    if (ids.length === 0) return;
    try {
      await withTimeout(
        this.client.revokeInstallations(ids.map(fromHex)),
        this.config.revokeTimeoutMs,
        "revokeInstallations",
      );
      for (const h of ids) this.pendingRevokes.delete(h);
      this.persistPendingRevokes();
      this.metrics.revokesOk += ids.length;
      this.log.info({ client: this.index, count: ids.length }, "revoked queued retired installations");
    } catch (e) {
      this.metrics.revokeFailures++;
      this.log.warn(
        { client: this.index, err: String(e?.message ?? e) },
        "flushing pending revokes failed (will retry on next build)",
      );
    }
  }

  /**
   * Graceful close, ordered for connection hygiene:
   * 1. drain: state != healthy stops new picks; wait for in-flight sends to
   *    settle (bounded by drainTimeoutMs) instead of cancelling them.
   * 2. retire: revoke our own installation while the client can still sign.
   * 3. close + null the reference so GC can drop the Rust client and its
   *    gRPC channel (close() alone does not release the connection).
   */
  async close() {
    if (this.rebuildTimer) clearTimeout(this.rebuildTimer);
    this.dmCache.clear();
    if (this.client) {
      this.state = "draining";
      const deadline = Date.now() + this.config.drainTimeoutMs;
      while (this.inflight > 0 && Date.now() < deadline) {
        await sleep(25);
      }
      if (this.inflight > 0) {
        this.log.warn(
          { client: this.index, inflight: this.inflight },
          "drain timed out; closing with sends still in flight",
        );
      }
      await this.retireSelf();
      try {
        await this.client.close();
      } catch (e) {
        this.log.warn({ client: this.index, err: String(e) }, "error during client close");
      }
      this.client = null;
    }
    this.state = "closed";
  }

  /** Rolling rebuild: used by circuit breaker and by generational rotation. */
  async rebuild(reason) {
    if (this.state === "rebuilding" || this.state === "closed" || this.state === "draining") return;
    this.state = "rebuilding";
    this.metrics.rebuilds++;
    this.log.warn({ client: this.index, reason }, "rebuilding client");
    try {
      await this.close();
    } catch { /* best effort */ }
    this.state = "rebuilding";
    const delay = this.backoffDelay();
    await new Promise((r) => setTimeout(r, delay));
    try {
      await this.build(); // build() ends with flushPendingRevokes()
      if (this.config.storageMode === "file") {
        this.pruneOldDbFiles();
      }
    } catch (e) {
      this.log.error({ client: this.index, err: String(e) }, "rebuild failed, will retry");
      this.state = "unhealthy";
      this.scheduleRebuild("rebuild-failed");
    }
  }

  backoffDelay() {
    const attempts = Math.min(this.consecutiveFailures, 6);
    const jitter = Math.random() * 1000;
    return this.config.rebuildBaseDelayMs * 2 ** attempts + jitter;
  }

  scheduleRebuild(reason) {
    if (this.rebuildTimer || this.state === "closed" || this.state === "draining" || this.state === "rebuilding") return;
    const delay = this.backoffDelay();
    this.rebuildTimer = setTimeout(() => {
      this.rebuildTimer = null;
      this.rebuild(reason).catch(() => {});
    }, delay);
    this.rebuildTimer.unref?.();
  }

  recordSuccess() {
    this.consecutiveFailures = 0;
    if (this.state === "unhealthy") this.state = "healthy";
  }

  recordFailure(err) {
    this.consecutiveFailures++;
    this.log.warn(
      { client: this.index, failures: this.consecutiveFailures, err: String(err?.message ?? err) },
      "send failure",
    );
    if (this.consecutiveFailures >= this.config.circuitBreakerThreshold) {
      this.state = "unhealthy";
      this.scheduleRebuild("circuit-breaker");
    }
  }

  /**
   * Disappearing-message settings for newly created DMs.
   * fromNs = now: every message this service sends is sent after DM creation,
   * so all of them get expire_at_ns = sent_at + inNs.
   * Returns undefined when disabled (DISAPPEAR_IN_HOURS=0).
   */
  disappearingSettings() {
    const h = this.config.disappearInHours;
    if (!h || h <= 0) return undefined;
    return {
      fromNs: BigInt(Date.now()) * 1_000_000n,
      inNs: BigInt(Math.round(h * 3600 * 1e9)),
    };
  }

  /** Insert into the DM cache with LRU eviction (Map preserves insertion order). */
  cacheDm(key, dm) {
    if (this.dmCache.has(key)) this.dmCache.delete(key); // refresh recency
    this.dmCache.set(key, dm);
    while (this.dmCache.size > this.maxDmCacheSize) {
      const oldest = this.dmCache.keys().next().value;
      this.dmCache.delete(oldest);
    }
  }

  /**
   * Get (cached) or create the DM with `address`.
   * 6.1.0 has no findOrCreateDm: cache -> create -> on conflict re-fetch.
   */
  async getDm(address) {
    const key = address.toLowerCase();
    const hit = this.dmCache.get(key);
    if (hit) return hit;
    const identifier = { identifier: address, identifierKind: ETHEREUM };
    const messageDisappearingSettings = this.disappearingSettings();
    const options = messageDisappearingSettings ? { messageDisappearingSettings } : undefined;
    if (messageDisappearingSettings) {
      this.log.info(
        { client: this.index, hours: this.config.disappearInHours },
        "creating DM with disappearing messages enabled",
      );
    }
    let dm;
    try {
      dm = await this.client.conversations.createDmWithIdentifier(identifier, options);
    } catch (e) {
      // Possible race: DM already exists (e.g. created by another path).
      this.log.warn({ client: this.index, err: String(e?.message ?? e) }, "createDm failed, refetching");
      dm = await this.client.conversations.fetchDmByIdentifier(identifier);
      if (!dm) throw e;
    }
    this.cacheDm(key, dm);
    return dm;
  }

  pruneOldDbFiles() {
    // file mode: keep only the newest DB file for this client.
    import("node:fs/promises").then(async (fs) => {
      try {
        const files = await fs.readdir(this.config.dataDir);
        const mine = files
          .filter((f) => f.startsWith(`client-${this.index}-`) && f.endsWith(".db3"))
          .sort();
        for (const f of mine.slice(0, -1)) {
          await fs.unlink(`${this.config.dataDir}/${f}`).catch(() => {});
          this.log.info({ client: this.index, file: f }, "pruned old DB file");
        }
      } catch { /* best effort */ }
    });
  }

  status() {
    return {
      index: this.index,
      state: this.state,
      inboxId: this.inboxId,
      consecutiveFailures: this.consecutiveFailures,
      inflight: this.inflight,
      dmCacheSize: this.dmCache.size,
      pendingRevokes: this.pendingRevokes.size,
    };
  }
}
