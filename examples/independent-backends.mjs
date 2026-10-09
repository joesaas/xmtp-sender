/**
 * 不共享 backend 的多 client 模板（@xmtp/node-sdk 6.1.0）
 *
 * 结论（经 libxmtp 源码验证）：
 * - 每个 client 独立 `createBackend()` → 每次 `build_optional_d14n()` 新建
 *   GrpcClient → `connect_lazy()` 建独立 Channel → 独立的 TCP/HTTP2 长连接。
 * - 反之：多个 client 传同一个 `backend` → 共用同一个 GrpcClient/Channel
 *   → 同一条 TCP 连接（`ClientBundle` 里是 Arc 包裹的已建好客户端，
 *   `from_bundle` 只做 Arc clone，不会重建连接）。
 *
 * 为什么选独立：
 * 1. 故障隔离：一条连接半死只影响 1 个 client，可独立熔断/重建；
 *    共享时 `close()` 不碰 backend，重建换不掉坏连接。
 * 2. 避开单 TCP 的队头阻塞：丢包只 stall 1/N 流量。
 * 3. 限流独立：tower rate_limit 5000req/60s 是 per-channel 的。
 *
 * 运行：node examples/independent-backends.mjs
 * 需要：SENDER_KEY_1..N（64 hex）、API_TOKEN（本例不用）、直连 XMTP 网络。
 */
import { Client, createBackend } from "@xmtp/node-sdk";
import { createEoaSigner } from "../src/signer.js";

const XMTP_ENV = process.env.XMTP_ENV ?? "dev";
const N = Number(process.env.CLIENT_COUNT ?? 4);

/**
 * 创建 N 个网络完全独立的 client。
 * 每个 client：独立 Backend → 独立 GrpcClient → 独立 TCP 连接。
 */
export async function createIndependentClients(signers, options = {}) {
  const clients = [];
  for (const { signer, address } of signers) {
    // 关键：每个 client 调一次 createBackend()，不复用
    const backend = await createBackend({ env: XMTP_ENV });
    const client = await Client.create(signer, {
      env: XMTP_ENV,
      backend, // ← 独立 backend（不传则 SDK 内部也会新建，效果等同）
      dbPath: null, // 纯内存 DB（示例用；生产按需改）
      useSingleConnection: true, // 单进程多 client 的推荐模式
      disableDeviceSync: true,
      ...options,
    });
    console.log(`client ready: ${address} -> inbox ${client.inboxId}`);
    clients.push(client);
  }
  return clients;
}

/**
 * 独立重建其中一个 client：只影响它自己，
 * 其他 client 的连接不受影响（close 不碰 backend，backend 也是独立的）。
 *
 * 刷新顺序（连接卫生的关键）：
 * 1. 旧 client 还活着时，只吊销它自己这一个 installation
 *    （revokeInstallations([ownId])，不用 revokeAllOtherInstallations —
 *    后者会误踢同钱包的其他 installation，如部署重叠期的另一实例）。
 * 2. close + 丢弃引用，GC 才能回收 Rust client 及其 gRPC 通道
 *    （close() 本身不释放连接，只停 worker/断 DB）。
 * 3. 若第 1 步失败（多半正是触发重建的网络故障），新 client 建好后
 *    用同一个 id 精确补吊销。
 */
export async function rebuildOne(clients, index, signer) {
  const old = clients[index];
  const oldIdBytes = old.installationIdBytes;
  let selfRevoked = true;
  await old.revokeInstallations([oldIdBytes]).catch((e) => {
    selfRevoked = false;
    console.warn("self-revoke failed (will retry precisely after rebuild):", e.message);
  });
  await old.close(); // 幂等；只关自己的 worker/stream/DB
  const backend = await createBackend({ env: XMTP_ENV });
  const fresh = await Client.create(signer, {
    env: XMTP_ENV,
    backend,
    dbPath: null,
    useSingleConnection: true,
    disableDeviceSync: true,
  });
  if (!selfRevoked) {
    await fresh.revokeInstallations([oldIdBytes]).catch((e) => {
      console.warn("precise revoke of retired installation failed:", e.message);
    });
  }
  clients[index] = fresh;
  return fresh;
}

// ---- 对比：共享 backend 的写法（不推荐用于高可用发送服务） ----
// const sharedBackend = await createBackend({ env: XMTP_ENV });
// const c1 = await Client.create(signer1, { env: XMTP_ENV, backend: sharedBackend });
// const c2 = await Client.create(signer2, { env: XMTP_ENV, backend: sharedBackend });
// → c1/c2 共用一条 TCP 连接；一条挂则全挂，且单 client 重建换不掉坏连接。

// ---- 可运行示例 ----
if (import.meta.url === `file://${process.argv[1]}`) {
  const keys = Array.from({ length: N }, (_, i) => process.env[`SENDER_KEY_${i + 1}`]);
  if (keys.some((k) => !k)) {
    console.error(`set SENDER_KEY_1..${N} (64 hex chars each)`);
    process.exit(1);
  }
  const signers = keys.map((k) => createEoaSigner(k));
  const clients = await createIndependentClients(signers);

  // 演示：随机挑一个 client 发 DM
  const to = process.env.DEMO_TO;
  if (to) {
    const pick = clients[Math.floor(Math.random() * clients.length)];
    const dm = await pick.conversations.createDmWithIdentifier({
      identifier: to,
      identifierKind: 0, // Ethereum
    });
    const id = await dm.sendText("hello from independent-backend template");
    console.log("sent:", id);
  }

  for (const c of clients) await c.close();
  console.log("done");
}
