# 分布式 Runtime 交付与部署手册

## 定位与边界

当前主线为 `apps/distributed_runtime`（Python API/Worker）和 `apps/runtime_console`（React/TypeScript）。原 Node Kernel 与 `apps/validation_runtime` 保留为历史回归基线，不承担新分布式执行。

执行元数据、租约、依赖和副作用账本在 PostgreSQL；LangGraph 图状态只由 FencedSQLSaver 保存；Redis 是通知/加速通道，不是唯一任务账本；MinIO 保存不可变产物。Session 固定 Package Digest，切换活跃版本只影响新 Session。

FencedSQLSaver 是本项目的 SQL Checkpointer，不是 AsyncPostgresSaver。其写入在同一 SQL 事务中核验执行代次，检查点和 pending writes 另有完整性摘要。摘要可发现覆盖范围内的损坏，不能抵御能同时改写数据库和摘要的管理员，不能证明旧记录在升级之前未被修改。

本版面向可信内网单租户试点，业务令牌与运维令牌分离，Worker 不持有业务/运维 API 令牌。不是企业 SSO、用户级 ABAC、跨物理机 HA 或任意不可信代码沙箱。Agent 定义属于受信任运维资产。

## 1. 可重复的合成环境验收

需要 Git、Docker Compose、Python 3.12、Node.js 24，命令在仓库根目录执行。

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements-distributed.lock
(cd apps/runtime_console && npm ci && npm test && npm run build)
python -m pytest tests -m 'not real_gateway' -q
npm test

docker compose -p dh-validation -f deploy/compose.validation.yml build api-a
docker compose -p dh-validation -f deploy/compose.validation.yml up -d --no-build --wait
RUN_DOCKER_ACCEPTANCE=1 python scripts/validate_docker_cluster.py
python scripts/validate_runtime_soak.py --seconds 60 --concurrency 8 --max-tasks 500
```

该环境仍是原来的 9 容器。API 分别映射本机 28080、28081，测试网关为 28090；构建后的控制台也由测试 API 提供。测试令牌仅用于这个隔离环境。验收脚本会杀进程、断网和中断存储，不能指向生产资源。结果在 `validation-results/`，必须核对 `tested_commit`。

发布镜像额外验证：

```bash
docker build -f deploy/Dockerfile.runtime --target runtime -t agent-runtime:delivery .
docker build -f deploy/Dockerfile.runtime --target console -t agent-runtime-console:delivery .
python scripts/validate_release_compose.py
pip install playwright==1.55.0
python -m playwright install --with-deps chromium
python scripts/validate_console_browser.py
```

发布拓扑验证显式叠加 `compose.release-test.yml`，外加独立测试网关，环境标签仍是 synthetic。正式配置没有自动回退到该文件的逻辑。浏览器测试采用真实 Chromium、HTTP API 和独立 Worker，但模型是确定性测试服务。

仅清理本套合成环境（`-v` 会删除测试数据卷）：

```bash
docker compose -p dh-validation -f deploy/compose.validation.yml down -v --remove-orphans
```

## 2. 真实模型与工具配置

```bash
python scripts/generate_runtime_env.py --output deploy/runtime.env
```

此文件权限为 0600，生成独立随机数据库、Redis、对象存储及 API 凭据；已有文件不会覆盖。填写以下服务端配置：`MODEL_BASE_URL`、`MODEL_NAME`、`MODEL_API_KEY`、`TOOL_BASE_URL`、`TOOL_API_TOKEN`。不要提交 env 文件，不要打印完整 Compose 配置，也不要在聊天或日志中粘贴密钥。已有基础设施可替换生成文件中的连接信息。

模型接口使用 OpenAI-compatible chat completions 协议；这是协议描述，不限定供应商。工具服务实现 `POST /query_metric`，JSON 字段为 `metric`、可选 `period`、`org`、`comparison`（yoy/mom）和 `group_by`（字符串数组）。网关接收 Bearer 工具凭据、Idempotency-Key、X-Runtime-Execution-Id、X-Runtime-Call-Id；响应必须为 JSON object（上限32KiB）。大结果由工具服务返回受控产物引用。模型不能选择工具地址或密钥。

```bash
set -a
. deploy/runtime.env
set +a
python scripts/check_live_configuration.py
```

缺配置返回 BLOCKED、退出码2；配置齐全只返回 CONFIGURED，不等于外部联调通过。

## 3. 启动真实网关部署

```bash
docker compose --env-file deploy/runtime.env -p dh-live-pilot \
  -f deploy/compose.infrastructure.yml -f deploy/compose.runtime.yml \
  up -d --build --wait
```

使用已有外部 PostgreSQL/Redis/S3 时省略 infrastructure 文件。运行单元是2个 API、3个 Worker、1个 Console/反向代理；可选本地基础设施再加3个容器。**这里没有测试网关。** 入口默认 `http://127.0.0.1:3000`，使用 RUNTIME_OPS_TOKEN 进入控制台。令牌只驻留当前页面内存，刷新需重新输入。

对外开放前在企业反向代理/Ingress 终止 TLS，限制网络访问并接入真实身份体系；不要把明文 HTTP 与共享试点令牌直接暴露公网。本 Compose 的基础设施不是 HA 配置，镜像基础标签也不是不可变发布摘要；正式发布应记录并固定验证过的 image digest。

## 4. 真实只读验收

提供实际有效的 `LIVE_METRIC_ID`、`LIVE_PERIOD`、`LIVE_ORG`、`LIVE_DIMENSION`，从受控环境注入，不在公共日志中展开。已启动目标可执行：

```bash
python scripts/validate_live_runtime.py --url http://127.0.0.1:3000
```

验收顺序：指定指标/期间/组织 → 同比 → 维度拆分 → 根据前文找最大值。检查真实模型调用、工具成功回执、检查点、固定 Session/Thread 和 Digest。未指定重启选项时不会宣称验证了 Worker 重启。

在专用 `dh-live-*` 测试项目可执行 `--restart-worker --project dh-live-pilot --env-file deploy/runtime.env`；该选项会实际重启容器，仅在授权的测试项目使用，不要指向在用生产环境。

GitHub Actions 的 `Live runtime acceptance gate` 使用同名 Secrets 注入模型与工具地址/密钥以及4个业务参数，MODEL_NAME 使用 Repository Variable。流水线只上传脱敏的ID、摘要和结构性结论，不上传真实对话、工具结果和容器业务日志。没有这些配置时应显示 BLOCKED，而不是模拟成功。

## 5. 运维与恢复

控制台支持 Agent 发布/活跃版本切换、Session、流式对话、任务树、Attempt/检查点/模型调用事件、Worker 排空与恢复、未知写结果对账。`GET /metrics` 要求运维令牌。

SSE 的断开只中断观察，不取消任务。取消操作显式传播；中断任务需要人工决定恢复，UNKNOWN 写操作先提供可核验对账证据，不能盲目重试。query_metric 是注册的只读工具，其失败可重新调用；record_metric 的真实端到端幂等仍取决于下游系统。

发布前排空 Worker，确认 active=0，再停止旧进程；API 可逐个切换。旧 Session 继续使用其原始 Package Digest。代码回滚和 Agent 版本回滚是两件事：后者通过 activate 切换新 Session 的默认版本，不迁移旧 Session。

从400a740旧数据库升级：先停止 API/Worker、备份 PostgreSQL 与对象存储，并确认没有活跃/待恢复任务；新数据库无需迁移。对于确认可信的旧库，需要显式建立升级时的完整性基线：

```bash
python -m apps.distributed_runtime.maintenance adopt-integrity \
  --acknowledge-new-baseline --reason 'approved offline upgrade after backup verification'
```

该命令读取 RUNTIME_DATABASE_URL，拒绝运行中的任务或近期心跳的 Worker，对已有摘要不重新背书。不要让新旧版本同时写同一个库。若回滚应用到不支持摘要的旧版，必须恢复配套升级前备份，不能让旧版生成无摘要状态后又无条件切回。

## 6. 验收结论的使用

Python/Node 回归、9容器故障、发布镜像拓扑与浏览器通过，只证明对应范围。60秒负载探针不是长时间稳定性证明。真实只读验收即使 PASS，也不自动证明业务数值正确、全部业务场景、企业写接口或跨物理机 HA；这些仍需企业受控数据与部署环境验收。
