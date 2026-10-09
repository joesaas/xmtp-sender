/**
 * Offline smoke test: boots the REAL service (src/index.js, unmodified)
 * against the simulated SDK in ./stub-sdk.mjs and verifies the full
 * lifecycle over HTTP:
 *
 *   Scenario A (happy path):
 *     boot -> listening -> /health 4/4 -> /metrics -> 401 without token
 *     -> POST /send-dm 200 -> SIGTERM -> exit 0, each client revoked
 *        exactly its own installation during shutdown.
 *
 *   Scenario B (one registration fails at boot):
 *     boot STILL reaches listening (regression test: boot used to be
 *     all-or-nothing Promise.all and exited fatally), the failed client
 *     heals in the background -> /health reaches 4/4.
 *
 * Run: npm run sim    (no network, no real keys — dummy env is injected)
 */
import { spawn } from "node:child_process";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = `${dirname(fileURLToPath(import.meta.url))}/..`;
const PORT = 3109;
const BASE = `http://127.0.0.1:${PORT}`;

const results = [];
function check(name, cond, extra = "") {
  results.push([name, !!cond]);
  console.log(`${cond ? "PASS" : "FAIL"}  ${name}${extra ? ` — ${extra}` : ""}`);
}

function simEnv(extra = {}) {
  return {
    ...process.env,
    XMTP_ENV: "dev",
    PORT: String(PORT),
    SENDER_KEY_1: "11".repeat(32),
    SENDER_KEY_2: "22".repeat(32),
    SENDER_KEY_3: "33".repeat(32),
    SENDER_KEY_4: "44".repeat(32),
    API_TOKEN: "sim-token",
    DB_ENCRYPTION_KEY: "aa".repeat(32),
    ROTATION_INTERVAL_MS: "0",
    REBUILD_BASE_DELAY_MS: "300",
    ...extra,
  };
}

function boot(extraEnv) {
  const child = spawn(
    process.execPath,
    ["--import", "./sim/register.mjs", "src/index.js"],
    { cwd: repoRoot, env: simEnv(extraEnv) },
  );
  let out = "";
  child.stdout.on("data", (d) => { out += d; });
  child.stderr.on("data", (d) => { out += d; });
  return { child, transcript: () => out };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitFor(fn, timeoutMs, label) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    try {
      if (await fn()) return true;
    } catch { /* not up yet */ }
    await sleep(200);
  }
  throw new Error(`timeout waiting for: ${label}`);
}

async function stop(child) {
  child.kill("SIGTERM");
  const exited = await Promise.race([
    new Promise((r) => child.once("exit", (code) => r(code))),
    sleep(8000).then(() => { child.kill("SIGKILL"); return "killed"; }),
  ]);
  return exited;
}

async function getJson(path) {
  const res = await fetch(`${BASE}${path}`);
  return { status: res.status, body: await res.json() };
}

async function scenarioA() {
  console.log("\n=== Scenario A: happy-path boot ===");
  const { child, transcript } = boot();
  try {
    await waitFor(async () => transcript().includes("xmtp-sender listening"), 15000, "listening");
    check("A1 boots and listens", true);

    const health = await getJson("/health");
    check("A2 /health 4/4 healthy", health.body.pool?.healthy === 4 && health.body.ok === true,
      `healthy=${health.body.pool?.healthy}`);

    const metrics = await getJson("/metrics");
    check("A3 /metrics openFds is a number", typeof metrics.body.openFds === "number" && metrics.body.openFds > 0,
      `openFds=${metrics.body.openFds}`);

    const unauth = await fetch(`${BASE}/send-dm`, {
      method: "POST", headers: { "Content-Type": "application/json" }, body: "{}",
    });
    check("A4 /send-dm without token -> 401", unauth.status === 401);

    const send = await fetch(`${BASE}/send-dm`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Authorization: "Bearer sim-token" },
      body: JSON.stringify({ to: `0x${"ab".repeat(20)}`, text: "hello sim" }),
    });
    const sendBody = await send.json();
    check("A5 /send-dm with token -> 200 + messageId",
      send.status === 200 && /^msg-\d+$/.test(sendBody.messageId ?? ""),
      `messageId=${sendBody.messageId} clientIndex=${sendBody.clientIndex}`);

    const metrics2 = await getJson("/metrics");
    check("A6 metrics counted the send", metrics2.body.sendsOk === 1, `sendsOk=${metrics2.body.sendsOk}`);

    const code = await stop(child);
    check("A7 SIGTERM -> exit 0", code === 0, `exit=${code}`);
    const revokes = (transcript().match(/revoked own installation/g) ?? []).length;
    check("A8 shutdown revoked each client's own installation (x4)", revokes === 4, `count=${revokes}`);
  } catch (e) {
    check(`A aborted: ${e.message}`, false);
    child.kill("SIGKILL");
    console.log("--- child transcript ---\n" + transcript());
  }
}

async function scenarioB() {
  console.log("\n=== Scenario B: client #3 registration fails at boot ===");
  const { child, transcript } = boot({ SIM_FAIL_CREATION: "3" });
  try {
    await waitFor(async () => transcript().includes("xmtp-sender listening"), 15000, "listening");
    check("B1 boot reaches listening despite one failed registration", true);

    const h0 = await getJson("/health");
    check("B2 /health initially degraded (3/4)", h0.body.pool?.healthy === 3,
      `healthy=${h0.body.pool?.healthy}`);

    await waitFor(async () => (await getJson("/health")).body.pool?.healthy === 4, 15000, "heal to 4/4");
    check("B3 failed client heals in background -> 4/4", true);

    const code = await stop(child);
    check("B4 SIGTERM -> exit 0", code === 0, `exit=${code}`);
  } catch (e) {
    check(`B aborted: ${e.message}`, false);
    child.kill("SIGKILL");
    console.log("--- child transcript ---\n" + transcript());
  }
}

await scenarioA();
await scenarioB();

const failed = results.filter(([, ok]) => !ok);
console.log(`\n${failed.length === 0 ? "ALL SIM CHECKS PASSED" : `${failed.length} CHECK(S) FAILED`} (${results.length} total)`);
process.exit(failed.length === 0 ? 0 : 1);
