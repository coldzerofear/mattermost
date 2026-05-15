# Mattermost 社区版高可用集群

本模块为 Mattermost 社区版提供多副本高可用支持，实现了 `einterfaces.ClusterInterface`，无需企业版 License 即可运行。

支持两种后端：

| 后端 | 环境变量 | 依赖 |
|---|---|---|
| PostgreSQL LISTEN/NOTIFY | `MM_CLUSTER_MODE=postgres` | 已有的 Mattermost 数据库，无额外依赖 |
| Redis Pub/Sub | `MM_CLUSTER_MODE=redis` | 独立 Redis 实例 |

---

## 快速开始

### PostgreSQL 模式（推荐）

```bash
MM_CLUSTER_MODE=postgres
MM_CLUSTER_NAME=my-mm-cluster
```

PostgreSQL 模式直接复用 Mattermost 现有数据库，首次启动自动创建 `cluster_messages` 表，无需手动建表。

### Redis 模式

```bash
MM_CLUSTER_MODE=redis
MM_CLUSTER_NAME=my-mm-cluster
MM_CLUSTER_REDIS_ADDR=redis:6379
```

---

## 完整环境变量参考

### 通用参数

| 变量 | 默认值 | 说明 |
|---|---|---|
| `MM_CLUSTER_MODE` | `none` | 集群模式：`postgres`、`redis`、`none` |
| `MM_CLUSTER_NAME` | `mm-cluster` | 集群名称，同一集群所有节点必须相同 |
| `MM_CLUSTER_NODE_ID` | `hostname-<随机8位>` | 节点唯一标识，K8s 建议用 Pod 名 |
| `MM_CLUSTER_HEARTBEAT_INTERVAL` | `10`（秒） | 心跳写入间隔 |
| `MM_CLUSTER_HEARTBEAT_TTL` | `30`（秒） | 心跳过期时间，超过视为节点下线 |

### PostgreSQL 专用参数

| 变量 | 默认值 | 说明 |
|---|---|---|
| `MM_CLUSTER_PG_DSN` | 空（复用主库 DSN） | 集群专用 PostgreSQL DSN，空则使用 Mattermost 主库 |
| `MM_CLUSTER_PG_CHANNEL` | `mm_cluster` | LISTEN/NOTIFY 频道名，同一集群必须一致 |
| `MM_CLUSTER_PG_MAX_CONNS` | `10` | 每个 Pod 的集群 DB 连接池上限 |
| `MM_CLUSTER_WEBCONN_RPC_TIMEOUT_MS` | `1000` | WebSocket 连接数 RPC 超时（毫秒） |

### Redis 专用参数

| 变量 | 默认值 | 说明 |
|---|---|---|
| `MM_CLUSTER_REDIS_ADDR` | 空（必填） | Redis 地址，格式 `host:port` |
| `MM_CLUSTER_REDIS_PASSWORD` | 空 | Redis 密码 |
| `MM_CLUSTER_REDIS_DB` | `0` | Redis 数据库编号 |
| `MM_CLUSTER_REDIS_TLS` | `false` | 是否启用 TLS |
| `MM_CLUSTER_REDIS_KEY_PREFIX` | `mm:cluster` | Redis Key 前缀 |

---

## 按副本数调优

### PostgreSQL 连接数计算

每个 Pod 的集群 DB 连接消耗：

```
总连接数 = (MM_CLUSTER_PG_MAX_CONNS + 2) × Pod 数
           └── 连接池 ──────────────────   └── listener 连接 + leader 锁连接
```

PostgreSQL `max_connections` 需能容纳集群连接 + Mattermost 主库连接：

```
max_connections ≥ (MaxConns + 2) × maxPods + 主库连接数 + 20（余量）
```

### 各规模推荐配置

#### 2–5 个副本

```bash
MM_CLUSTER_PG_MAX_CONNS=10               # 5 Pod × 12 = 60 条连接
MM_CLUSTER_WEBCONN_RPC_TIMEOUT_MS=1000
```

PostgreSQL 配置：
```
max_connections = 200    # 60 集群 + ~100 主库 + 40 余量
```

#### 5–10 个副本

```bash
MM_CLUSTER_PG_MAX_CONNS=8                # 10 Pod × 10 = 100 条连接
MM_CLUSTER_WEBCONN_RPC_TIMEOUT_MS=800    # K8s 内网延迟低，可缩短
```

PostgreSQL 配置：
```
max_connections = 300
```

#### 10–20 个副本

```bash
MM_CLUSTER_PG_MAX_CONNS=5                # 20 Pod × 7 = 140 条连接
MM_CLUSTER_WEBCONN_RPC_TIMEOUT_MS=600
```

PostgreSQL 配置：
```
max_connections = 400
# 强烈建议在 PostgreSQL 前部署 PgBouncer（transaction 模式）
```

> **超过 10 个副本时**，建议在 Mattermost 与 PostgreSQL 之间部署 PgBouncer（transaction pooling 模式），将实际 PostgreSQL 连接数压缩 5–10 倍。

---

## Kubernetes 部署示例

### Deployment 环境变量

```yaml
env:
  - name: MM_CLUSTER_MODE
    value: "postgres"
  - name: MM_CLUSTER_NAME
    value: "my-mm-cluster"
  # Pod 名作为节点 ID，天然唯一且可观测
  - name: MM_CLUSTER_NODE_ID
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
  - name: MM_CLUSTER_PG_MAX_CONNS
    value: "10"
  - name: MM_CLUSTER_WEBCONN_RPC_TIMEOUT_MS
    value: "800"
```

### 必须启用的 Mattermost 设置

```bash
# 集群功能开关（不需要 License，但开关需要打开）
MM_CLUSTERSETTINGS_ENABLE=true
MM_CLUSTERSETTINGS_CLUSTERNAME=my-mm-cluster

# 文件存储必须用共享存储（S3 或兼容接口），不能用本地磁盘
MM_FILESETTINGS_DRIVERNAME=amazons3
MM_FILESETTINGS_AMAZONS3BUCKET=your-bucket

# Session 缓存关闭（否则各副本 Session 不同步）
MM_SERVICESETTINGS_SESSIONCACHEINSECONDS=0
```

### Ingress 会话亲和性

Mattermost WebSocket 长连接需要同一客户端路由到同一 Pod，否则连接会被反复重建：

```yaml
# nginx-ingress
annotations:
  nginx.ingress.kubernetes.io/affinity: "cookie"
  nginx.ingress.kubernetes.io/session-cookie-name: "mm-route"
  nginx.ingress.kubernetes.io/session-cookie-expires: "172800"
  nginx.ingress.kubernetes.io/session-cookie-max-age: "172800"
```

---

## 工作原理

### PostgreSQL 模式

```
发送方 Pod                        接收方 Pod
──────────                        ──────────
SendClusterMessage()
  └─ INSERT cluster_messages       pq.Listener.Notify 触发
  └─ SELECT pg_notify(channel, id) └─ SELECT payload FROM cluster_messages
  └─ COMMIT                        └─ bus.Decode → Dispatch → Handler
```

**Leader 选举**：`pg_try_advisory_lock`，持有锁的 Pod 为 Leader，断连时 PostgreSQL 自动释放，后继者在 5 秒内接管。

**GC**：只有 Leader 每 30 秒清理 5 分钟前的 `cluster_messages` 行，避免多 Pod 并发 DELETE 放大写压力。

### Redis 模式

```
发送方 Pod                        接收方 Pod
──────────                        ──────────
PUBLISH mm:cluster:broadcast      SUBSCRIBE mm:cluster:broadcast
  └─ Envelope{Sender, Msg}          └─ 过滤 Sender==self → Dispatch
PUBLISH mm:cluster:node:<id>      SUBSCRIBE mm:cluster:node:<self>
  （定向消息）
```

**Leader 选举**：SCAN 所有 `mm:cluster:nodes:*` 心跳 Key，字典序最小的节点 ID 为 Leader。

---

## 功能覆盖范围

| 功能 | 状态 | 说明 |
|---|---|---|
| WebSocket 跨副本广播 | ✅ | 消息、状态变更实时同步 |
| 本地缓存跨副本失效 | ✅ | Channel/User/Config 缓存自动同步 |
| Leader 选举 | ✅ | 定时任务、后台 Job 仅在 Leader 执行 |
| 配置热更新广播 | ✅ | Admin 修改配置后所有副本自动重载 |
| 用户在线状态同步 | ✅ | 跨副本汇总 WebSocket 连接数 |
| 插件状态汇总 | ✅ | System Console 展示所有副本插件状态 |
| 跨副本统计数据 | ✅ | System Console 各节点连接数 |
| 日志/支持包聚合 | — | 返回空，各副本日志需通过日志收集系统查看 |

---

## 常见问题

**Q：`cluster_messages` 表在哪个数据库？**

默认和 Mattermost 主库在同一个数据库。如需隔离，设置 `MM_CLUSTER_PG_DSN` 指向单独的数据库。

**Q：看到 `Failed to publish postgres cluster message: begin tx: context deadline exceeded`**

PostgreSQL 连接池已满。减小 `MM_CLUSTER_PG_MAX_CONNS` 或增大 PostgreSQL `max_connections`；高并发场景建议部署 PgBouncer。

**Q：看到 `Status update channel is full. Falling back to direct update`**

瞬间大量用户断连（压测结束、滚动重启）导致状态更新队列溢出。属于正常降级，不影响数据一致性，消息写入 DB 不丢失。

**Q：Redis 模式下如何确认集群正常？**

```bash
redis-cli KEYS "mm:cluster:nodes:*"    # 应看到所有 Pod 的心跳 Key
redis-cli PUBSUB CHANNELS "mm:cluster:*"  # 应看到 broadcast 和各 node 频道
```

**Q：PostgreSQL 模式下如何确认集群正常？**

```sql
-- 查看活跃节点
SELECT * FROM clusterdiscovery WHERE type = 'mm_cluster_postgres';

-- 查看最近的集群消息（正常情况下会被 GC 清理，可能为空）
SELECT id, target, created_at FROM cluster_messages ORDER BY created_at DESC LIMIT 10;
```
