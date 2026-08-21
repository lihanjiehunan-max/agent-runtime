# Agent Runtime MVP 交接文档

> 更新时间：2026-08-21
> 当前分支：`feat/agent-runtime-python-mvp`
> 当前 PR：[lihanjiehunan-max/agent-runtime#1](https://github.com/lihanjiehunan-max/agent-runtime/pull/1)

## 1. 交接结论

当前工程已经完成 Python/Deep Agents Runtime MVP 的主要代码、契约、确定性测试和控制台闭环。后续迭代应以 Python Runtime 为主实现，Node Runtime 不再继续扩展。

技术基线是：

- Python 3.12
- `deepagents==0.7.7`
- FastAPI API
- Python Worker
- React/TypeScript Console
- PostgreSQL + LangGraph `AsyncPostgresSaver`
- Redis
- MinIO/Object Storage
- Prometheus/Grafana

Task 1 有意保持最小化：只建立 Python 工程骨架和边界，Node 0.2/0.3 仅作为只读的行为、安全参考，不强行复用旧实现，也不提前引入 Node 0.3 兼容协议。

## 2. 总体架构

```text
React Runtime Console
        │ REST + SSE
        ▼
FastAPI Runtime API
        │
        ├── Agent Package API
        ├── Session API
        ├── Execution / Task API
        ├── Event / Trace API
        └── Runtime Status API
        │
        ├── PostgreSQL
        │     Runtime 元数据、Execution、事件、Trace、审计证据
        │     LangGraph Checkpoint / AsyncPostgresSaver
        │
        ├── Redis
        │     命令流、实时事件、取消信号、Session 互斥锁
        │
        └── Python Runtime Worker
              ├── Agent Package Loader / Agent Factory
              ├── Deep Agents Adapter
              ├── Model Gateway
              ├── Tool Gateway
              ├── Event Normalizer
              └── Trace / Metrics / Result Projection
                        │
                        ├── MinIO：Package、超大 Payload、结果工件
                        └── Prometheus/Grafana：指标与运行监控
```

核心关系：

```text
Agent Package → Session → Execution → Model / Tool / Event / Trace / Result
```

## 3. 核心领域对象

### Agent Package

Agent Package 是不可变的 Agent 发布单元，使用以下身份定位：

```text
tenant_scope + agent_id + version + digest
```

Package 主要包含：Manifest、系统提示词、运行时配置、工具绑定、权限限制、观测配置和校验摘要。Session 创建后固定 Package Digest，不允许热切换。

首期验收 Package 是：

```text
agent-metric-query:0.1.0
```

### Session

Session 是第一类对象，不等同于一次 Run。它绑定：

```text
租户 + 用户 + Agent + Package Digest + LangGraph thread_id
```

一个 Session 支持多轮 Execution，但同一时间最多只能有一个活动 Execution。Session 状态由 LangGraph Checkpointer 权威保存，不能另建一套可变聊天历史。

### Execution

Execution 表示一次用户 Turn，记录：

- 执行模式、状态和时间
- Trace ID、Session ID、Package 身份
- 模型调用和工具调用计数
- Token 与耗时统计
- 事件序列和 Checkpoint 引用
- 结果引用或错误信息

## 4. 一次请求的执行链路

1. API 从认证 Principal 获取租户、用户、Actor 和 Worker 身份。
2. 创建或读取 Session，确认 Package Digest 未变化。
3. 创建 Execution，生成新的 `execution_epoch` 和 Trace ID。
4. 将执行命令写入 Redis Stream。
5. Worker 获取 Session 锁并校验 Epoch CAS。
6. Loader 根据 Digest 加载 Package，Agent Factory 创建受限 Deep Agent。
7. Deep Agent 使用 Session 对应的 `thread_id` 执行模型和工具调用。
8. Event Normalizer 将 Deep Agents 事件转换为统一 Runtime Event。
9. 事件写入持久化投影，同时通过 SSE/Redis 实时发送给 Console。
10. 大型 Payload 写入 MinIO，事件中只保留不可变引用。
11. Execution 和 Session 通过 Epoch CAS 写入终态，旧 Worker 的迟到写入被拒绝。

## 5. 已完成的任务范围

| 任务 | 内容 | 状态 |
|---|---|---|
| Task 1 | 冻结 Node 基线，建立 Python 骨架 | 已完成，保持最小实现 |
| Task 2 | 契约、身份、错误和事件信封 | 已完成 |
| Task 3 | PostgreSQL 元数据模型和迁移 | 已完成 |
| Task 4 | Agent Package、Resolver、Digest 校验 | 已完成 |
| Task 5 | MinIO 来源、本地缓存、Singleflight | 已完成 |
| Task 6 | Deep Agents 0.7.7 Agent Factory 和最小权限 Profile | 已完成 |
| Task 7 | Tool Gateway 和只读 `query_metric` | 已完成 |
| Task 8 | Session Manager、Thread 和 Checkpointer | 已完成 |
| Task 9 | Execution Manager、事件规范化、SSE | 已完成 |
| Task 10 | 超时、协作式取消、异步任务 | 已完成 |
| Task 11 | Trace、Metrics、Payload Offload、Retention | 已完成 |
| Task 12 | Agent、Session、Chat、Trace、Dashboard Console | 已完成 |
| Task 13 | 恢复、并发、Chaos 和性能门禁 | 已完成确定性门禁 |
| Task 14 | 部署组合、就绪探针、切换和最终验收 | 已完成代码与确定性门禁 |

## 6. 当前验证证据

最近一次 Task 14 修复轮验证：

```text
Python tests: 291 passed, 12 skipped, 1 warning
Pyright: 0 errors, 0 warnings, 0 informations
Scoped Ruff: passed
Runtime Console: 20 Vitest tests passed
TypeScript check: passed
Vite production build: passed
git diff --check: passed
```

Task 13 确定性性能门禁：

| 指标 | 结果 |
|---|---:|
| Session 数量 | 40 |
| Package cache 命中率 | 100% |
| Cached Definition P95 | 4.458 ms |
| Platform overhead P95 | 0.123 ms |
| 协作式取消 | 0.001130 s |
| Trace 覆盖率 | 100% |
| 短任务成功率 | 100% |
| 持久化原始 Payload | 0 |
| 持久化凭据 | 0 |

以上是本地确定性测试结果，不等同于生产容量证明。

## 7. 当前不能误判的内容

以下能力尚未在当前工作区完成真实生产依赖验证：

- PostgreSQL、Redis、MinIO 实例联调
- 真实 Model Gateway
- 真实 Tool Gateway
- 部署方提供的 Principal Verifier Factory
- 真实 Worker 进程重启后的第四轮对话
- 生产代理、域名和流量切换
- 非协作式模型调用的强制取消
- Locust 真实压测

因此，`/health/live` 正常不代表 `/health/ready` 可以生产接流量。生产就绪必须通过依赖探针、迁移、真实认证和 Task 14 Live Acceptance。

## 8. 后续迭代顺序

### 第一步：完成真实运行环境闭环

1. 准备 PostgreSQL、Redis、MinIO、Model Gateway 和 Tool Gateway。
2. 安装部署方的 `RUNTIME_PRINCIPAL_VERIFIER_FACTORY`。
3. 使用 `deploy/env.example` 生成未入库的运行环境文件。
4. 执行 `uv run alembic upgrade head`。
5. 创建并验证 `RUNTIME_MINIO_BUCKET`。
6. 启动 API 和 Worker，确认 `/health/ready` 返回 200。
7. 开启 `RUNTIME_TASK14_LIVE=1`，执行真实三轮、重启后第四轮验收。

### 第二步：完成生产部署闭环

1. 构建并安装 Worker 的正式启动入口。
2. 在目标机安装 systemd 服务和 Python 依赖。
3. 部署 Console 反向代理，让 `/api/v1/runtime` 指向 Python API。
4. 保留 Node `/api/runs` 只读兼容路径。
5. 通过灰度流量完成 Python Runtime 切换。

### 第三步：补充产品能力

优先级建议：

1. Agent Package 注册、发布和版本管理界面。
2. Session 列表、恢复、关闭和执行历史。
3. Trace 详情、事件筛选和 Payload 引用查看。
4. Tool Gateway 真实数据权限和审计查询。
5. 写工具的 Effect Ledger、Outbox 和人工确认门禁。
6. 预算、限流、HITL、评测和质量反馈。

以下内容不应在当前 MVP 阶段提前引入：在线 Agent Builder、动态子 Agent 团队、任意 Shell/代码执行、长期跨 Session 记忆、多地域调度和多 Runtime 编排。

## 9. 开发约束

后续开发必须保持以下不变量：

1. 不继续扩展 Node Runtime，不把 Node 代码复制到 Python。
2. 不维护第二套可变聊天历史；LangGraph Checkpointer 是唯一状态源。
3. Redis 锁只负责协调，不能替代 PostgreSQL Epoch CAS。
4. 身份从认证 Principal 获取，禁止信任请求 JSON 中的租户或用户字段。
5. 所有 Worker 写入都必须带租户、Session、Lease、Epoch 和状态版本校验。
6. Package Digest、Session、Execution、模型和工具信息必须进入 Trace。
7. 模型不能自行扩大工具白名单。
8. 不把模型密钥、Tool Gateway 密钥和原始敏感 Payload 发到浏览器或写入日志。
9. 确定性测试不能替代真实基础设施验收，跳过的 Live Gate 必须继续保持显式记录。
10. 新功能先补契约测试，再实现代码；任务完成前必须重新运行验证命令。

## 10. 常用命令

```bash
# Python 环境
uv sync
uv run pytest -q -rs
uv run pyright
uv run ruff check apps/runtime_api apps/runtime_worker packages \
  tests/contract/test_runtime_composition.py \
  tests/acceptance/test_three_turn_metric_session.py

# API
uv run uvicorn apps.runtime_api.main:app --host 127.0.0.1 --port 8000
curl --fail http://127.0.0.1:8000/health/live
curl --fail http://127.0.0.1:8000/health/ready

# Worker
uv run python -m apps.runtime_worker.main health

# Console
cd apps/runtime_console
npm ci
npm test
npm run check
npm run build
```

重点测试：

```bash
uv run pytest tests/acceptance/test_three_turn_metric_session.py -q -rs
uv run pytest tests/contract/test_runtime_composition.py -q -rs
uv run python tests/performance/assert_thresholds.py
```

说明：当前全仓 `uv run ruff check .` 仍会报告一个已知的历史问题：
`tests/acceptance/test_console_runtime.py:1` 的 import 顺序。它不属于本次交接文档变更，运行时范围检查保持通过。

## 11. 关键文件入口

| 文件 | 用途 |
|---|---|
| `docs/superpowers/specs/2026-08-20-deepagents-python-runtime-mvp-design.md` | 目标架构和边界 |
| `docs/superpowers/plans/2026-08-20-deepagents-python-runtime-mvp.md` | 14 个任务的实施计划 |
| `apps/runtime_api/composition.py` | 生产 Runtime 组合根 |
| `apps/runtime_api/main.py` | API 应用和健康检查 |
| `apps/runtime_worker/consumer.py` | Worker 消费和执行入口 |
| `packages/deepagents_adapter/` | Deep Agents 适配和权限 Profile |
| `packages/package_loader/` | Package 解析、校验、缓存和加载 |
| `packages/session_manager/` | Session、锁和 Checkpointer |
| `packages/execution_manager/` | Execution、取消、事件和结果 |
| `packages/event_normalizer/` | Deep Agents 事件转换 |
| `packages/model_gateway/` | 模型网关客户端 |
| `packages/tool_gateway/` | 工具契约、`query_metric` 和审计 |
| `apps/runtime_console/` | React/TypeScript 控制台 |
| `deploy/env.example` | 运行时配置契约，不含真实密钥 |
| `docs/operations/runbook.md` | 部署、健康检查和故障处理 |
| `docs/operations/cutover.md` | Node → Python 切换和回滚 |
| `legacy/` | Node 0.2 冻结基线和测试证据 |

## 12. 交接完成标准

下一位开发者接手后，至少应完成以下确认：

- 能读懂本文件、设计说明和实施计划；
- 能在本地完成 Python 测试、类型检查和 Console 构建；
- 能解释 Package、Session、Execution 的关系；
- 能解释为什么 Redis 锁不能替代 Epoch CAS；
- 能定位 `/health/ready` 为何返回 503；
- 能在不接触真实密钥的情况下运行确定性验收；
- 在真实依赖可用后，优先完成 Task 14 Live Gate，而不是继续扩展业务功能。
