# Task 13 修复轮报告

## 范围

本轮只关闭 `task-13-review-package-round-1.md` 中的三项发布门禁问题：

1. 性能 harness 改为实际走确定性的 `ExecutionManager → EventEmitter/Trace → ToolGateway → CancellationToken` 路径；事件、工具、Checkpoint、payload 和凭据计数均从运行结果计算。
2. 跨 Session 并发测试增加 event ID、message、thread ID 和 checkpoint 值的隔离断言。
3. stale worker 测试 double 校验 tenant、session、execution、worker、principal 和 execution epoch，并验证旧 worker/epoch 的写入不会改变事件或终态。

未修改生产 Runtime、控制台、迁移文件或 Task 14 文件。

本轮提交文件：

- `tests/performance/locustfile.py`
- `tests/performance/test_task13_harness.py`
- `tests/concurrency/test_same_session_serialization.py`
- `tests/chaos/test_dependency_outages.py`
- `task-13-report-fix-round-1.md`

## 验证证据

### Task 13 聚焦测试

命令：

```text
UV_CACHE_DIR=/tmp/runtime-mvp-uv-cache-task13 uv run pytest tests/recovery tests/chaos tests/concurrency tests/performance -q -rs
```

结果：`27 passed, 3 skipped`，耗时 `1.79s`。

明确跳过：

- live dependency chaos：仅在显式启用时运行；
- Redis session-lock integration：未配置 `RUNTIME_TEST_REDIS_URL`；
- PostgreSQL session-lifecycle integration：未配置 `RUNTIME_TEST_DATABASE_URL`。

### 确定性性能阈值

命令：

```text
UV_CACHE_DIR=/tmp/runtime-mvp-uv-cache-task13 uv run python tests/performance/assert_thresholds.py
```

输出：

```json
{
  "cache_hit_rate": 1.0,
  "cached_definition_p95_ms": 90.36693699999887,
  "checkpoints_recorded": 40,
  "cooperative_cancel_seconds": 0.0019325180001033004,
  "credentials_recorded": 0,
  "platform_overhead_p95_ms": 129.39890300003754,
  "raw_payloads_recorded": 0,
  "runtime_events_recorded": 440,
  "session_count": 40,
  "short_task_success_rate": 1.0,
  "tool_calls_recorded": 40,
  "tool_events_recorded": 80,
  "trace_coverage": 1.0,
  "trace_projections_recorded": 40
}
```

全部阈值通过：30–50 Session 范围、缓存命中率至少 95%、定义加载 P95 不超过 100 ms、平台开销 P95 不超过 300 ms、协作式取消不超过 2 s、Trace 覆盖率 100%、短任务成功率至少 95%，且没有原始 payload 或凭据被记录。

### 全量 Python 回归

命令：

```text
UV_CACHE_DIR=/tmp/runtime-mvp-uv-cache-task13 uv run pytest tests -q -rs
```

结果：`283 passed, 12 skipped, 1 warning`，耗时 `5.75s`。

唯一 warning 是依赖侧 `StarletteDeprecationWarning`：`httpx` 与 `starlette.testclient` 的兼容性提示，不是本轮代码错误。跳过项均为显式 live/dependency 条件，包括 Task 14 live acceptance、Redis、PostgreSQL、MinIO 和 live trace/retention 验证。

### Console 回归

使用已有本地依赖直接执行，避免 npm 启动阶段的环境网络审批：

```text
(cd apps/runtime_console && ./node_modules/.bin/vitest run)
```

结果：`3 test files, 20 tests passed`。

```text
(cd apps/runtime_console && ./node_modules/.bin/tsc --noEmit && ./node_modules/.bin/vite build)
```

结果：TypeScript 检查和 Vite production build 均通过。

### 静态检查和范围检查

```text
UV_CACHE_DIR=/tmp/runtime-mvp-uv-cache-task13 uv run ruff check tests/recovery tests/chaos tests/concurrency tests/performance
```

结果：`All checks passed!`

```text
UV_CACHE_DIR=/tmp/runtime-mvp-uv-cache-task13 uv run pyright
```

结果：`0 errors, 0 warnings, 0 informations`。

`git diff --check` 通过；显式文件名白名单检查确认差异只包含上述 Task 13 文件。

## Live / Locust 限制

- 未连接真实 PostgreSQL、Redis 或 MinIO；相应 live 测试按环境变量条件明确跳过。
- 未调用真实模型或外部 Tool Gateway。性能 harness 使用确定性的本地 Model/Metric doubles，但执行面、事件、Trace、工具和协作式取消路径是真实 Runtime 代码路径。
- 仓库未安装可用的 Locust CLI，本轮未把 `uv run locust ...` 作为通过项；性能结论只来自可重复的确定性 runner。
- 非协作式模型 provider 的硬中断能力未被证明；`cooperative_cancel_seconds` 只代表协作式取消。
- 40 Session 数据是本地确定性 release-gate 证据，不代表生产容量或真实外部依赖下的吞吐承诺。
