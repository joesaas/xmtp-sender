/**
 * One managed XMTP client: lifecycle (create/close/rebuild), health, DM cache.
 *
 * Design notes from libxmtp source:
 * - `Client.create` registers the installation (wallet signature, one-time per boot).
 * - `close()` is idempotent; call it before dropping the reference.
 * - `dbPath: null` => pure in-memory DB: no disk growth, nothing to clean up.
 * - `useSingleConnection: true` is the SDK's recommended mode for many clients
 *   in one process.
 */
import { Client } from "@xmtp/node-sdk";

const ETHEREUM = 0;

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

  async start() {
    await this.build();
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
    // Self-heal the installation count: every fresh build (boot, rebuild,
    // rotation) leaves at most 1 active installation for the inbox, so the
    // MAX_INSTALLATIONS_PER_INBOX (10) limit is never approached.
    // No-op when there is nothing else to revoke.
    await this.revokeOthers("build");
  }

  /**
   * Revoke all other installations of this inbox. Best-effort: a failure is
   * logged and counted; the next successful build/rebuild retries, and a
   * single success clears the whole backlog.
   */
  async revokeOthers(context) {
    try {
      await this.client.revokeAllOtherInstallations();
      this.log.info({ client: this.index, context }, "revoked older installations");
    } catch (e) {
      this.metrics.revokeFailures++;
      this.log.warn(
        { client: this.index, context, err: String(e?.message ?? e) },
        "revokeAllOtherInstallations failed (will retry on next build)",
      );
    }
  }

  async close() {
    this.state = "closed";
    if (this.rebuildTimer) clearTimeout(this.rebuildTimer);
    this.dmCache.clear();
    if (this.client) {
      try {
        await this.client.close();
      } catch (e) {
        this.log.warn({ client: this.index, err: String(e) }, "error during client close");
      }
      this.client = null;
    }
  }

  /** Rolling rebuild: used by circuit breaker and by generational rotation. */
  async rebuild(reason) {
    if (this.state === "rebuilding" || this.state === "closed") return;
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
      await this.build(); // build() ends with revokeOthers()
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
    if (this.rebuildTimer || this.state === "closed") return;
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
    };
  }
}
