import { createEoaSigner } from "./signer.js";

function req(name, def) {
  const v = process.env[name] ?? def;
  if (v === undefined || v === "") throw new Error(`missing required env: ${name}`);
  return v;
}
function num(name, def) {
  const v = process.env[name];
  return v === undefined || v === "" ? def : Number(v);
}

export function loadConfig() {
  const keys = [1, 2, 3, 4].map((i) => req(`SENDER_KEY_${i}`));
  const signers = keys.map((k) => createEoaSigner(k));

  const dbKeyHex = req("DB_ENCRYPTION_KEY");
  if (!/^[0-9a-fA-F]{64}$/.test(dbKeyHex)) {
    throw new Error("DB_ENCRYPTION_KEY must be 64 hex chars (32 bytes)");
  }

  return {
    env: process.env.XMTP_ENV ?? "dev",
    port: num("PORT", 3000),
    signers, // [{ signer, address }]
    apiToken: req("API_TOKEN"),
    dbEncryptionKey: Uint8Array.from(Buffer.from(dbKeyHex, "hex")),
    storageMode: (process.env.STORAGE_MODE ?? "memory").toLowerCase(),
    dataDir: process.env.DATA_DIR ?? "./data",
    clientConcurrency: num("CLIENT_CONCURRENCY", 8),
    sendTimeoutMs: num("SEND_TIMEOUT_MS", 30000),
    sendMaxRetries: num("SEND_MAX_RETRIES", 2),
    apiRateLimitQps: num("API_RATE_LIMIT_QPS", 20),
    maxTextLen: num("MAX_TEXT_LEN", 4096),
    circuitBreakerThreshold: num("CIRCUIT_BREAKER_THRESHOLD", 5),
    rebuildBaseDelayMs: num("REBUILD_BASE_DELAY_MS", 2000),
    rotationIntervalMs: num("ROTATION_INTERVAL_MS", 21600000),
    healthProbeIntervalMs: num("HEALTH_PROBE_INTERVAL_MS", 60000),
    warmupAddresses: (process.env.WARMUP_ADDRESSES ?? "")
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean),
    logLevel: process.env.LOG_LEVEL ?? "info",
    xmtpLogLevel: process.env.XMTP_LOG_LEVEL ?? "warn",
  };
}
