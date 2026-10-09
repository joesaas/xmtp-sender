# xmtp-sender 架构设计

> 基于 `github.com/xmtp/libxmtp` 源码（main 分支，2026-10-08）与 `@xmtp/node-sdk@6.1.0` 的逐行分析，
> 结论先行：**4 个独立 inbox（4 个钱包）+ 纯内存 DB + DM 会话缓存 + per-client 熔断/轮转**。

---

## 1. 源码级事实（设计依据）

### 1.1 发送链路：`send()` 是完全同步的重量级操作

源码：`crates/xmtp_mls/src/groups/mod.rs:1439 send_message`

```
send_message(text)
 ├─ is_active / ensure_not_paused 检查
 ├─ maybe_update_installations(5s节流)          # mls_sync.rs:4304
 │    └─ 超过5秒 → 走网络 load_identity_updates 查成员身份更新（一次gRPC往返）
 ├─ commit_pending_proposals_if_any
 ├─ prepare_message                            # mod.rs:1740
 │    └─ 本地 MLS 加密 → 写 group_messages(状态Unpublished) → queue SendMessage intent
 └─ sync_until_last_intent_resolved             # 重试循环 MAX_GROUP_SYNC_RETRIES
      └─ sync_with_conn_locked（持有 per-group 锁全程）
           ├─ publish_intents                   # mls_sync.rs:3108
           │    └─ 取出所有 ToPublish intent，逐个 MLS 加密/签名 → gRPC publish（串行）
           ├─ receive()                         # 每次 send 都执行！
           │    └─ query_group_messages + process_messages（拉取+解密，即使纯发送不读）
           └─ post_commit()
```

关键数字：
- **稳态每次 send ≈ 2 次 gRPC 往返**（publish + query），外加最多每 5 秒一次 identity 更新查询
  （`SEND_MESSAGE_UPDATE_INSTALLATIONS_INTERVAL_NS = 5s`，`crates/xmtp_configuration/src/common/mls.rs:27`）。
- **首次建 DM ≈ 3–4 次往返**：`create_dm_by_inbox_id`（`client.rs:728`）→ 建空 MLS group →
  `add_members`（`mod.rs:1902`）→ `load_identity_updates`（查身份）→ `fetch_key_packages`
  （拉 key package）→ publish membership commit → `send_welcomes`（按安装数分块）。
- JS 层的 `send()` 默认走同步版本；Rust 侧虽有 `send_message_optimistic`
  （`mod.rs:1524`），但 6.1.0 的 JS API 未暴露乐观发送——**每次 send 都要等网络 publish 完成**。

### 1.2 并发模型：per-group 两层锁

- `MutexRegistry`（`mutex_registry.rs`）：按 `group_id` 分配 `Arc<Mutex>`，`sync_with_conn_locked`
  全程持有 → **同一 client 内，同一个 DM 的并发 send 完全串行化**。
- `GroupCommitLock`（`lib.rs:50`）：MLS 状态读写时再加一道 per-group 锁。
- 不同 group 可并发；**不同 client（不同 context）锁完全独立**。

推论：单 client 单 DM 的吞吐上限 ≈ 1 / 单次send耗时；要打满 5 QPS，必须把流量分散到多个
DM group 上。4 个 client 若共享 1 个 inbox（4 个 installation），发给同一 recipient 时是
**同一个 MLS group**——各 installation 本地 group 状态互相独立，并发发送会造成 epoch 冲突、
触发重试回路，白白浪费往返。**因此 4 个 client 必须是 4 个独立 inbox（4 个钱包）**，
每个 recipient 对应 4 个独立 DM group，随机选 client 天然打散锁竞争。

### 1.3 存储：无界增长是"越发越慢"的根因

表结构：`crates/xmtp_db/src/encrypted_store/schema_gen.rs`

| 表 | 增长行为 | 能否删 |
|---|---|---|
| `group_messages` | 每次 send 写一行（含**明文** `decrypted_message_bytes`）；SQLCipher 整库加密，无 VACUUM | 应用消息可删（见下） |
| `group_intents` | 每次 send 留一行，成功后只标 `Processed`，**源码中无任何 prune 逻辑** | 可删（已 Processed） |
| `identity` (rowid=1) | inbox_id / installation_keys | **不可删** |
| `openmls_key_store` / `openmls_key_value` | MLS group 状态 + 密钥材料，in-place 更新 | **不可删** |
| `identity_updates` | 随成员变更增长，membership 校验依赖 | **不可删** |
| `key_package_history` | KeyPackageCleaner worker 按 `delete_at_ns` 自动清理 | 自动 |
| `refresh_state` | sync cursor | 删消息不影响 |

内置清理只有 `delete_expired_messages`（`group_message.rs:1322`）——仅删已过期的
disappearing messages。**JS binding 层没有暴露 raw SQL**，`DELETE FROM ... WHERE sent_at < cutoff`
这种"删 N 天前历史"在 6.1.0 根本做不到。

**设计决策**：`dbPath: null`（纯内存 DB，`ClientOptions` 文档原话：
"No database will be created and all data will be lost once the client disconnects"）。
发送服务是 fire-and-forget，不需要读历史、不需要离线 inbox——**让历史数据根本不落盘，
从根上消除"越发越慢"**。代价：重启后 installation 丢失，需用钱包重新注册（见 1.5），
DM 首次发送重走 3–4 次往返（用 DM 缓存 + 预热摊薄）。

`STORAGE_MODE=file` 作为可选项保留：此时 janitor 采用**代际轮转**——定期逐个重建 client
并换新 DB 文件、revoke 旧 installation、删除旧文件。不需要 raw SQL，同样把增长封顶。

### 1.4 网络层

- `crates/xmtp_api_grpc/src/grpc_client/native.rs`：tonic `Channel` + `connect_lazy()`
  （单 client 连接复用，长连接）；HTTP/2 keepalive 默认 45s interval / 20s timeout，
  可用环境变量调：`XMTP_GRPC_KEEPALIVE_INTERVAL_SECS` / `XMTP_GRPC_KEEPALIVE_TIMEOUT_SECS` /
  `XMTP_GRPC_TCP_KEEPALIVE_SECS` / `XMTP_GRPC_KEEPALIVE_WHILE_IDLE`。
- `ApiClientWrapper` 包 `Retry::default()`（`builder.rs`），publish 内有 `retry_async!`。
- **client 侧源码中未发现限流器**——服务端限流阈值未知，保守起见 API 层自带 token bucket。
- 注意：tonic **不支持 HTTP 代理**（直连）。部署在需要代理的环境会失败，
  这是 bindings 的硬限制，不是本项目能修的。

### 1.5 身份与密钥

- 单 inbox 安装数上限 `MAX_INSTALLATIONS_PER_INBOX = 10`（`xmtp_configuration/.../mls.rs:31`）。
  本项目 4 inbox × 1 installation，远离上限；轮转时精确 revoke 自己退役的那个
  installation 防止堆积——用 `revokeInstallations([ownId])`，**绝不用**
  `revokeAllOtherInstallations()`：后者无差别踢掉同钱包全部其他 installation，
  会误伤部署重叠期的另一实例（或同钱包的任何其他接入）。
- 钱包签名**只在身份注册时需要**（建 inbox / 加 installation，`identity.rs:490` 起的
  `SignatureRequestBuilder`）；日常 send 和 key package 轮换只用 installation key。
  → 私钥只需在进程启动/重建时参与签名，平时不触碰。
- `rotate_and_upload_key_package`（`identity.rs:710`）：后台 worker 自动轮换，
  旧 key package 进 `key_package_history` 由 KeyPackageCleaner 清理——无需人工干预。

### 1.6 安全边界

- DB 加密：SQLCipher，32 字节密钥由调用方传入（`DbOptions.encryptionKey`，
  `bindings/node/src/client/create_client.rs:108`）；**不传则 `build_unencrypted()` 明文落盘**。
  → 即使 `dbPath: null`（内存 DB）也传入 `dbEncryptionKey`，纵深防御。
- E2E：DM 内容经 MLS 加密（`prepare_message → into_envelope`）；服务端只能看到密文
  envelope + 元数据（group_id、sender installation、时间戳、cursor）。
- Key package 上传到服务端的是公钥材料，无私钥泄露面。

---

## 2. 总体架构

```
                    ┌──────────────────────────────────────────────┐
                    │                 xmtp-sender                  │
  POST /send-dm ──▶ │  Fastify ─▶ 限流(token bucket) ─▶ 路由层      │
  {to, text}        │                                              │
                    │  路由层：随机选一个 healthy client             │
                    │    ├─ client[0] (inbox A) ─ DM cache ─▶ send │
                    │    ├─ client[1] (inbox B) ─ DM cache ─▶ send │
                    │    ├─ client[2] (inbox C) ─ DM cache ─▶ send │
                    │    └─ client[3] (inbox D) ─ DM cache ─▶ send │
                    │                                              │
                    │  每 client 独立：熔断器 / 健康探针 / 定时轮转    │
                    └──────────────────────────────────────────────┘
```

### 2.1 Client 池（`src/pool.js`）

- 启动时用 4 个钱包私钥并行 `Client.create`，`dbPath: null`，`useSingleConnection: true`
  （SDK 文档：专为"单进程多 client"设计），`disableDeviceSync: true`（发送服务不需要），
  `dbEncryptionKey` 必传。
- **随机路由**：只在 `healthy` 的 client 里均匀随机；`GET /health` 可见每 client 状态。
- **per-client 并发**：信号量（默认 8）。同一 DM 的并发在 libxmtp 层自动串行，
  不同 DM 可并行；信号量防止 intent 堆积。
- **熔断器**：连续失败 ≥ N 次（默认 5）→ 标记 `unhealthy`，停止路由，指数退避重建
 （`close()` 是幂等的 → `Client.create` 同一 signer；inbox_id 是钱包派生的， deterministic，
  重建后 inbox 不变）。
- **优雅刷新**（重建与定时轮转共用）：`close()` 本身不释放网络连接（源码核实：只
  cancel worker、断 DB，gRPC channel 靠引用计数归零后随 GC 回收）。因此刷新顺序是：
  摘流（state=draining，在途请求不再新增）→ 等 inflight 排空（上限
  `DRAIN_TIMEOUT_MS`，不硬砍在途 send）→ 旧 client 趁还活着精确 revoke 自己这一个
  installation → `close()` + 引用置空 → 建新 client。self-revoke 若失败（多半正是
  触发重建的网络故障），该 id 进持久化 pending 队列（`DATA_DIR/pending-revokes-*.json`），
  下一代 client 建成后精确补吊销——crash-loop 重启也不会泄漏，且永不误伤他人。
  连接是否真回收看 `/metrics` 的 `openFds`：每次重建后应回到稳态基线，只涨不落
  即存在残留引用。
- **定时轮转**（默认每 6 小时，逐个滚动）：走上面的优雅刷新，既是 file 模式的
  janitor，也是无状态自愈。

### 2.2 DM 会话缓存（`src/dmCache.js`）

- `Map<addressLower, Dm>`，per-client 独立。
- miss 时 `createDmWithIdentifier({identifier, identifierKind: 0})`；
  6.1.0 没有 `findOrCreateDm`，用"先查缓存 → 不存在则创建 → 创建冲突则回查"的三段式兜底。
- 可选 `WARMUP_ADDRESSES`：启动后预热高频 recipient，摊薄首次 3–4 往返。

### 2.3 发送管线（`src/sender.js`）

- 单次 send 超时（默认 30s），超时/异常按指数退避重试（默认 2 次，换 client 重试）。
- `idempotencyKey`：调用方不传则按 `sha256(to|text|minute)` 生成——同一分钟同内容重发
  去重（SDK 原生支持，`group_messages` 有 `idempotency_key` 唯一约束，
  migration `2026-06-10`）。
- 绝不记录消息明文；只记 `to` 的脱敏前缀、耗时、messageId。

### 2.4 历史清理（需求 2 的直接回答）

1. **默认模式（memory）**：`dbPath: null` → 无落盘、无增长、无需清理。重启即清空。
2. **file 模式**：`ROTATION_INTERVAL` 触发 rolling rebuild（一次只转一个 client，
   零停机），旧 DB 文件删除 + `revokeInstallations` 清掉旧 installation。
3. 不提供"删 N 天前消息"的 SQL——JS 层没有这个能力，硬做等于绕过 SDK 写库，
   会破坏 `identity`/`openmls_key_store` 的一致性。

### 2.5 自恢复（需求 3）

| 故障 | 检测 | 恢复 |
|---|---|---|
| 单次 send 异常/超时 | try/catch + 超时器 | 换 client 重试（≤2 次） |
| client 连续失败 | 熔断计数器 | 标记 unhealthy → 指数退避 `close()+重建` |
| gRPC 长连接半死 | 健康探针（轻量 `getDmByInboxId` 本地调用 + 周期性真发送探针可选） | 重建 client（新 channel，`connect_lazy`） |
| DB 膨胀（file 模式） | 轮转计时器 / 文件大小阈值 | rolling rebuild |
| 进程崩溃 | — | 容器/Docker restart policy；启动即重建（无状态） |
| 未捕获异常 | `unhandledRejection` handler | 只记日志不退出（发送错误不致命） |
| 启动时单个 client 注册失败 | `start()` 捕获 | 服务降级启动（其余 client 先行服务），失败者标记 unhealthy 走退避重建自愈——启动不是全有或全无 |

### 2.6 性能：5 QPS 的容量核算

- 实测依据缺失（沙箱 gRPC 被 MITM 代理拦截，tonic 不支持代理，无法实测；见 1.4），
  按源码往返数估算：稳态 send ≈ 2 gRPC 往返，局域网/优质网络下单次约 200–500ms。
- 单 client 并发 8 → 理论 ~16–40 QPS；4 client → 远超 5 QPS，瓶颈只在**同一 DM 的串行锁**。
  4 inbox 设计下，同一 recipient 有 4 个独立 DM group，随机路由把单 DM 压力降为 1/4。
- API 层 token bucket 默认 20 QPS（保护性上限，可调），`POST /send-dm` 返回 202 语义
  实为同步等待 send 完成（调用方需要 messageId）；另提供 `?async=1` 纯入队模式（TODO）。

### 2.7 安全清单

- 私钥：仅 `SENDER_KEY_1..4` 环境变量；启动后只驻内存；**永不打日志**（连脱敏都不打）。
- API：`API_TOKEN` bearer 校验；`to` 必须 `0x` + 40 hex；`text` 限长（如 4KB）。
- `dbEncryptionKey`：`DB_ENCRYPTION_KEY`（64 hex），必传。
- 不记录消息内容；metrics 只记计数与延迟分布。
- `disableDeviceSync: true`：减少不必要的网络面。

---

## 3. 源码不确定项（已标注，不做过度设计）

1. node 侧每个 client 是否独立 gRPC Channel——大概率是，未逐行验证；重建 client 即重建 channel，
   不影响恢复设计。
2. 服务端 publish 限流阈值——源码无，API 层自带可调 token bucket 兜底。
3. SQLite journal/busy_timeout 具体值——`dbPath: null` 下不重要。
4. OpenMLS group 状态随发送量的增长曲线——DM 只有 2 成员，`openmls_key_store` in-place 更新，
   未见膨胀机制；且内存 DB 定期轮转，风险可控。
