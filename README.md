# Enterprise Agent Runtime

当前开发主线：**Python 分布式 Harness + React/TypeScript 控制台**。

API 负责发布、Session、Execution、运维和事件读取；独立 Worker 承载 DeepAgents 0.7.7。PostgreSQL 保存任务/租约/协作关系，FencedSQLSaver 保存唯一图状态，Redis 负责通知，S3/MinIO 保存不可变产物。

## 本轮交付

- 多 Worker 能力匹配、Session 单写者、执行代次隔离、持久化子任务委派/等待/唤醒。
- 异步提交、可重连 SSE、限定时长的同步等待、取消/超时、人工恢复与 UNKNOWN 副作用门禁。
- 带业务参数和独立凭据的 query_metric，稳定调用标识及结构化回执。
- 检查点和 pending writes 完整性校验、显式离线旧库升级。
- Agent 版本激活、Session 历史、任务树/Attempt/Trace、Worker 排空、Prometheus 指标。
- 中文操作控制台；令牌仅保存在页面内存。
- 合成故障验收、发布镜像/反向代理验收、真实模型/工具的独立配置与只读验收入口。

**部署与操作：[`docs/operations/distributed-runtime-delivery.md`](docs/operations/distributed-runtime-delivery.md)**

## 工程入口

| 目录 | 用途 |
|---|---|
| `apps/distributed_runtime` | 当前 Python API、调度/协调、Worker、Harness、持久化与工具网关 |
| `apps/runtime_console` | 当前 React/TypeScript 控制台 |
| `deploy/compose.validation.yml` | 9容器确定性测试环境，故障脚本仅用于此环境 |
| `deploy/compose.runtime.yml` | 无模拟回退的真实网关部署；可组合 infrastructure 文件 |
| `scripts/validate_docker_cluster.py` | 原15组并发/故障场景 |
| `scripts/validate_release_compose.py` | 发布镜像、反向代理和实际 Worker 的合成验收 |
| `scripts/validate_console_browser.py` | Chromium 页面操作验收 |
| `scripts/check_live_configuration.py` / `validate_live_runtime.py` | 配置检查 / 真实只读联调，缺配置返回 BLOCKED |

## 开发验证

要求发布环境 Python 3.12、Node.js 24。先构建控制台，再构建测试容器。

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements-distributed.lock
(cd apps/runtime_console && npm ci && npm test && npm run build)
python -m pytest tests -m 'not real_gateway' -q
npm test
```

真实网关密钥由私有环境文件或密钥系统注入，不能写入 Agent Package、网页或 Git。

## 当前边界

可信内网单租户试点；共享试点令牌不等于企业 SSO/用户级隔离。合成模型验证不等于真实模型质量；同宿主机容器不等于跨物理机 HA。发布和真实验收必须分别判断，不以 CI 中某一条绿色流水线代替全部目标。

旧 Node Kernel 与 `apps/validation_runtime` 仅作为历史基线。其原始说明存于 [`docs/operations/kernel-baseline-readme.md`](docs/operations/kernel-baseline-readme.md)，不代表新服务的入口或安全承诺。
