# xmtp-sender

High-throughput XMTP DM sending service built on [`@xmtp/node-sdk@6.1.0`](https://www.npmjs.com/package/@xmtp/node-sdk)
(which wraps [`xmtp/libxmtp`](https://github.com/xmtp/libxmtp) via native bindings).

- Boots **4 independent XMTP clients** (4 wallets → 4 inboxes), picks one **at random** per send.
- `POST /send-dm` — send a DM to any Ethereum address.
- Designed from a source-level analysis of libxmtp for **stability, history cleanup,
  auto-recovery, and 5+ QPS**. See [`docs/DESIGN.md`](docs/DESIGN.md) for the full
  analysis → decision mapping (network, storage, efficiency, security).

## Why this shape (one-paragraph version)

`send()` in libxmtp is fully synchronous (~2 gRPC round-trips per send, plus a
per-group lock that serializes same-DM sends inside one client) and every send
appends rows to `group_intents` / `group_messages` that are **never pruned**.
So the design is: 4 independent inboxes (no shared MLS group state, no cross-client
lock contention, random routing spreads the per-group lock), **pure in-memory DB**
(`dbPath: null` — history never hits disk, so there is nothing to clean up),
a per-client DM cache (first-DM creation costs 3–4 round-trips; cached DMs cost ~2),
and per-client circuit breakers + rolling rebuilds for self-healing.

## Quickstart

```bash
cp .env.example .env
# fill in SENDER_KEY_1..4 (64 hex chars each), API_TOKEN, DB_ENCRYPTION_KEY (64 hex)
# (.env in the working directory is auto-loaded at startup; requires Node >= 20.12)

npm install
npm start
```

Send a DM:

```bash
curl -X POST http://localhost:3000/send-dm \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"to":"0x...","text":"hello"}'
# -> {"messageId":"...","clientIndex":2,"inboxId":"...","latencyMs":312}
```

Health & metrics:

```bash
curl http://localhost:3000/health
curl http://localhost:3000/metrics
```

Docker:

```bash
docker build -t xmtp-sender .
docker run --env-file .env -p 3000:3000 xmtp-sender
```

## Configuration

See `.env.example`. Key knobs:

| var | default | notes |
|---|---|---|
| `XMTP_ENV` | `dev` | `dev` \| `production` |
| `STORAGE_MODE` | `memory` | `memory` = no disk growth, nothing to clean. `file` = generational DB rotation janitor |
| `CLIENT_CONCURRENCY` | `8` | max in-flight sends per client |
| `SEND_TIMEOUT_MS` | `30000` | per-send timeout |
| `API_RATE_LIMIT_QPS` | `20` | token bucket on `/send-dm` |
| `ROTATION_INTERVAL_MS` | `21600000` | rolling client rebuild every 6h (`0` = off) |
| `CIRCUIT_BREAKER_THRESHOLD` | `5` | consecutive failures before fencing a client |
| `DISAPPEAR_IN_HOURS` | `0` | disappearing messages: `0` = off; e.g. `24` = sent messages expire 24h after sending |

### Disappearing messages (阅后即焚)

Set `DISAPPEAR_IN_HOURS` (e.g. `24`) and every newly created DM gets
`messageDisappearingSettings: { fromNs: <now>, inNs: <hours> }`, so each message
expires `inNs` after it is sent and compliant apps delete it from local storage.

Heads-up from the [official docs](https://docs.xmtp.org/chat-apps/core-messaging/disappearing-messages):
this is **app-level enforcement** — it removes messages from participants' UIs and
local storage, but does **not** delete them from the XMTP network. A recipient
using an app without disappearing-message support will still see everything.

gRPC keepalive can be tuned without code changes via
`XMTP_GRPC_KEEPALIVE_INTERVAL_SECS` / `XMTP_GRPC_KEEPALIVE_TIMEOUT_SECS` /
`XMTP_GRPC_TCP_KEEPALIVE_SECS` (see `docs/DESIGN.md`).

## API

### `POST /send-dm` (auth required)

```json
{ "to": "0x...", "text": "hello", "idempotencyKey": "optional" }
```

- `to`: Ethereum address (`0x` + 40 hex).
- `text`: 1–4096 chars (tunable via `MAX_TEXT_LEN`).
- `idempotencyKey`: optional. If omitted, the server uses `sha256(to|text|minute)`,
  so retrying the same request within a minute is deduplicated by the SDK.

Responses: `200 { messageId, clientIndex, inboxId, latencyMs }`,
`400` validation, `401` auth, `429` rate limited, `502` send failed, `503` no healthy client.

### `GET /health` / `GET /metrics` (no auth)

Pool status per client; counters + p50/p95/p99 send latency.

## Security notes

- Private keys live only in env vars and memory; they are never logged (the logger
  redacts key-like fields as a second line of defense).
- Message bodies are never logged.
- The local DB is always created with `dbEncryptionKey` (SQLCipher); with
  `STORAGE_MODE=memory` nothing is persisted at all.
- DM content is MLS end-to-end encrypted; the XMTP network only sees ciphertext
  envelopes plus metadata.

## Offline smoke test (simulated network)

No XMTP network reachable? Verify the full boot lifecycle against a stubbed
SDK (real service code, fake network layer):

```bash
npm run sim
```

Scenario A: clean boot -> /health 4/4 -> /send-dm 200 -> graceful shutdown
(each client revokes exactly its own installation). Scenario B: one client's
registration is made to fail at boot — the service must still come up
degraded (3/4) and heal to 4/4 in the background. Boot is fault-tolerant per
client; a single failed registration never blocks startup.

## Limitations

- The native gRPC client (tonic) does **not** support HTTP proxies — deploy where
  direct egress to the XMTP network is available.
- 6.1.0 has no `findOrCreateDm`; the service keeps its own DM cache per client.
- Throughput per single DM is bounded by libxmtp's per-group lock (~1/latency);
  aggregate QPS scales with distinct recipients × 4 inboxes.
