# 多 Docker 节点验收环境

本环境是带故障注入的预生产模拟，不是已完成生产认证的部署模板。使用 2 API + 3 Worker + PostgreSQL + Redis + MinIO + 测试网关，共 9 个独立容器。API/Worker 不共享可写目录；SQL 持久化状态和 Checkpointer，Redis 传递可丢失通知，MinIO 保存校验哈希的产物。

## 运行

需要 Linux Docker Engine、Docker Compose v2 和 Python 3.12。仅在测试服务器运行；脚本会终止、暂停、断网和重启本 Compose 项目中的容器。端口仅绑定 127.0.0.1，不对公网开放，配置中的凭据均为一次性测试值。

```bash
python3.12 -m venv .venv
. .venv/bin/activate
pip install -r requirements-distributed.lock
python -m pytest tests -m 'not real_gateway' -q
docker compose -p dh-validation -f deploy/compose.validation.yml build api-a
docker compose -p dh-validation -f deploy/compose.validation.yml up -d --no-build --wait --wait-timeout 180
RUN_DOCKER_ACCEPTANCE=1 python scripts/validate_docker_cluster.py
cat validation-results/acceptance.json
# 以下命令会删除此测试环境的持久化卷，仅用于测试环境收尾。
docker compose -p dh-validation -f deploy/compose.validation.yml down -v --remove-orphans
```

## 测试边界

真实执行：DeepAgents/LangGraph、HTTP API、跨容器 Worker、SQL 检查点、事务 fencing、Redis Streams、S3/MinIO、SSE、幂等 ledger。脚本测试 50 个并发提交的独立会话，Worker 总执行槽为 6；不得把“50 并发提交”写成“50 个模型并行调用”。

替身范围：独立网关容器提供确定性的 OpenAI 兼容模型响应和模拟业务工具响应。没有调用真实外部模型或企业业务系统，因此不能证明模型质量、真实 Token 成本、真实业务副作用撤销或目标系统吞吐。

SIGKILL、pause、network disconnect 都作用在真实容器，不用函数 mock 代替。数据库重启验证单实例持久化与服务恢复，不代表 PostgreSQL 主从切换或多 AZ 容灾。所有容器仍共享一个宿主机，不能证明跨物理机隔离或宿主机故障容灾。

## 判断与恢复

执行超时、用户取消有独立终态。Worker 失联或存储暂时失败进入 INTERRUPTED；不会自动重跑。运维调用 POST /api/v1/runtime/ops/executions/{id}/resume 继续检查点。工具已经执行但回执不明时，UNKNOWN 阻止恢复，必须查询业务回执并提交 reconcile 后恢复。

FencedSQLSaver 是本项目的 LangGraph Checkpointer 实现，以同一个 SQL 事务完成执行代次校验与检查点写入；并不是直接使用未加 fencing 的 AsyncPostgresSaver。平台不另建一份可变聊天历史。粗粒度 PostgreSQL advisory lock 是当前吞吐上限之一。

上线前仍需验证真实模型网关/工具、SSO 与多租户授权、TLS 和密钥轮换、数据库 HA/备份恢复、跨主机网络、24 小时以上稳定性与目标容量。当前统一 Bearer 是单信任域验证机制，不是业务用户和运维的完整权限体系。镜像标签的实际 digest 随每轮测试留档，发布前须按通过的 digest 固定。
