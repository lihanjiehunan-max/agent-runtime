# Enterprise Agent Runtime Kernel 0.2

企业级 Agent Runtime 的加固版一期工程基线。项目以无容器、零外部依赖的 Node.js 24 + SQLite 模块化单体验证运行时核心不变量，当前定位是 **Runtime Kernel / 架构验证基线**，不是完整的生产级 Host+Cloud Runtime。

## Python Session SSE 服务 MVP

仓库同时包含一个独立的 Python 3.12 验证服务，用于通过写死的 `shipping-operations-analyst` Skill 和 `gpt-5.6-sol` 网关验证以下最小链路：

- 运维显式部署 Agent Instance；
- 业务调用方使用逻辑 Agent ID 创建 Session；
- 通过 `POST /api/v1/runtime/sessions/{session_id}/chat` 获取真实 `text/event-stream` 输出；
- 同一 Session 保留多轮上下文，且业务响应不包含 Agent Instance 信息。

快速启动：

```bash
MODEL_API_KEY='<configured locally>' \
SERVICE_API_KEY='<configured locally>' \
uv run uvicorn apps.validation_runtime.main:app --host 0.0.0.0 --port 8000
```

详细配置、curl 示例、代理要求和三轮验收见 [`docs/operations/session-sse-service.md`](docs/operations/session-sse-service.md)。该服务使用内存状态，重启会丢失 Agent Instance、Session 和对话上下文。

## 本轮完成的加固

- Context 使用租户隔离的不透明引用；相同内容跨租户、跨密级不再发生全局主键冲突。
- Tool 幂等范围包含 Contract Version，v1/v2 不会串用结果。
- Checkpoint 使用完整性 v2 信封，覆盖身份、父链、类型、状态、事件游标、Epoch、Schema、状态正文与创建时间，并交叉校验事件账本。
- HTTP 撤销 Context 会同步协调所有依赖 Run；恢复入口还会自愈遗漏的撤销协调。系统保留但失效不安全 Checkpoint 分支，完整物化安全 DELTA head，并立即 fence 旧 Worker。
- `WAITING_MANUAL`、`RECOVERY_BLOCKED` 是自动恢复硬门禁；人工裁决和 effect 对账显式审计。
- 恢复成功、pending-effect 阻断和损坏阻断都对恢复开始时捕获的 lease、`execution_epoch` 与 `state_version` 快照执行 CAS，拒绝覆盖并发新状态。
- 外部副作用采用 durable intent + SQLite Outbox；成功、明确拒绝、结果不明分别落为 `COMMITTED / FAILED / UNKNOWN`，新 Epoch 会将旧 dispatcher 安全隔离为 `UNKNOWN`。
- 每个 Worker 写入口都会在同一事务内核对 effect ledger：`UNKNOWN` 原子进入人工门禁，未归档的 `FAILED` 原子终止 Run；活跃 effect 会阻止并发终态迁移，所有门禁都会递增 Epoch 并清除旧 lease。跨 Run 重放不安全结果时，当前 Run 的处分会在返回重放错误前一并提交。
- HTTP 使用 Bearer principal，租户、Actor、Worker、强制接管和恢复权限不再由请求正文或 `x-tenant-id` 决定。

## 本地启动

要求 Node.js 24 或更高版本，无需 Docker、外部数据库或 `npm install`。

```bash
npm run dev
```

打开 `http://127.0.0.1:3000`。本地控制台默认使用显式开发令牌 `dev-token`，对应：

- tenant：`demo-tenant`
- subject：`local-operator`
- worker：`host-runtime-01`
- permissions：本地演示所需的全部一期权限

默认数据文件位于 `data/runtime.db`，可覆盖：

```bash
AGENT_RUNTIME_DB=/absolute/path/runtime.db HOST=127.0.0.1 PORT=3000 npm run dev
```

## 生产身份配置

`npm start` 默认拒绝在缺少身份配置时启动，与 `NODE_ENV` 是否设置无关。必须提供 `AGENT_RUNTIME_IDENTITIES`；只有 `npm run dev` 会显式启用已知开发令牌。配置值是“opaque token → principal”的 JSON 映射：

```bash
NODE_ENV=production \
AGENT_RUNTIME_IDENTITIES='{"replace-with-secret-token":{"tenantId":"tenant-a","subjectId":"runtime-a","workerId":"worker-a","permissions":["run:read","run:execute"]}}' \
npm start
```

当前静态 token resolver 是可信身份边界的本地验证适配器。生产部署应在保持 principal 契约的前提下替换为 mTLS、经验证的反向代理身份或企业 OIDC/JWT 适配器，并通过密钥管理系统注入凭据。

除健康检查和静态控制台外，所有 `/api/*` 请求都要求：

```http
Authorization: Bearer <opaque-token>
```

可选的 `x-tenant-id` 只用于诊断关联；若提供，必须与 principal tenant 完全一致。

## 验证

```bash
npm test
npm run check
```

测试覆盖 Context 隔离、旧库迁移、Tool Contract 版本幂等、外部 effect 崩溃窗口与并发终态门禁、人工对账、状态机、旧 Epoch fencing、Checkpoint 元数据/事件篡改、撤销分支、HTTP 权限、SSE、路径遍历、数据库重启和端到端流程。

## 核心 API

- `GET /api/health`：公开健康检查
- `POST /api/contexts`：保存不可变 Context Reference
- `POST /api/contexts/:id/revoke`：撤销 Context，要求 `context:revoke`
- `POST /api/runs`：创建 Run
- `POST /api/runs/:id/lease`：获取 lease；`force:true` 额外要求 `run:takeover` 和 `reason`
- `POST /api/runs/:id/start|checkpoint|complete|fail`：Worker 执行边界
- `POST /api/runs/:id/tools/invoke`：按版本化 Tool Contract 调用
- `POST /api/runs/:id/recover`：接管恢复，要求 `run:recover` 和 `reason`
- `POST /api/runs/:id/effects/:effectId/resolve`：人工归档 UNKNOWN effect
- `POST /api/runs/:id/manual/resolve`：人工决定 `RESUME / FAIL`
- `GET /api/runs/:id/events`：读取不可变 Run 时间线
- `GET /api/events?run_id=...&after_seq=...`：SSE 增量事件流

## 外部副作用语义

Tool Contract 必须明确执行模式：

- `READ_ONLY`：无业务副作用，可直接执行。
- `LOCAL_TRANSACTIONAL`：副作用与 effect ledger 位于同一个 SQLite 事务；内置计数器仅用于验证该模式。
- `EXTERNAL_OUTBOX`：先提交 effect intent 与 Outbox，再调用异步外部适配器。

`EXTERNAL_OUTBOX` 提供持久化意图、稳定幂等键、重复投递隔离和结果不明保护，但 **不等于跨系统 exactly-once**。真正的端到端精确一次仍要求下游接受稳定幂等键或支持按 key 查询结果；否则超时、断连和进程中断必须保持 `UNKNOWN`，由人工或对账器裁决。

## 数据库升级

启动时自动执行幂等迁移：

- 通过 SQLite PRAGMA 结构化识别紧凑、命名约束或独立索引形式的旧 `tool_effects` 唯一范围，将其升级为 tenant + operation + contract version + key；迁移保留历史结果、Outbox 外键、非目标索引与触发器，并执行外键校验。
- 对历史 `EXTERNAL_OUTBOX` effect 校验一对一 Outbox；缺失行会按终态合成，活跃或结果不明的历史记录保守转为 `UNKNOWN`，避免产生无法人工对账的永久门禁。
- 验证旧 Checkpoint v1 的原有 parent/state hash 后，升级为完整性 v2。该过程建立升级时的新完整性基线；v1 从未保护过的旧元数据无法获得追溯证明。

建议升级生产副本前先备份数据库，并先在只读克隆上完成迁移与恢复演练。

## 当前边界与下一阶段

一期仍不实施成本/Token 预算、步骤重要性、死信队列、自动补偿、Kafka、Temporal、Kubernetes 沙箱、模型网关、Runtime Relay、跨主机 Git Handoff或微服务拆分。

下一阶段应接入一个真实模型、一个只读 Tool 和一个支持下游幂等/查询的写 Tool，建立 20 个 Golden Tasks，再根据实测负载决定 PostgreSQL、独立 Dispatcher 和工作流引擎的生产化替换。现有 HTTP/领域契约是目标契约，不承诺未来基础设施替换完全无需版本演进。
