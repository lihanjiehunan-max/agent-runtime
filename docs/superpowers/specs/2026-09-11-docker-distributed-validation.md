# 多 Docker 节点分布式运行验收

目标：以真实 DeepAgents 0.7.7、独立 API/Worker 容器及真实 PostgreSQL、Redis、MinIO，验证跨进程/跨容器协调。不把同宿主机容器模拟称为跨物理机 HA，也不把测试模型称为真实模型。

拓扑：api-a、api-b；worker-a、worker-b、worker-c（每节点两个执行槽）；PostgreSQL、Redis、MinIO；一个仅用于测试的 OpenAI 协议/工具网关服务。任务和 Checkpointer 以 PostgreSQL 为权威，Redis 为可丢失通知，MinIO 为产物库。节点没有共享可写工作目录。

需要补齐：上一轮只有六个运行模块持久保存，本次补齐检查点适配、真实 Harness Worker、API 和测试。旧 API 入口保留。租约过期标记 INTERRUPTED，显式恢复，无自动重试。

验收：部署和会话版本固定、50 并发会话、3 Worker 实际参与、同一会话互斥和幂等、检查点多轮继承、父子 Agent 持久等待/唤醒、SIGKILL 接续、pause 旧节点 fencing、单 Worker 网络隔离、API 重启/SSE 补读、Redis 故障降级、PostgreSQL 重启、MinIO 故障恢复、取消和超时、工具成功但回执丢失 UNKNOWN 门禁、授权边界。全部结果必须包含 commit、容器身份、执行 ID、时间、断言和原始日志。

边界：共享数据库粗粒度协调锁仅适用于当前验证规模。无真实模型凭证时，报告明确列为未验证。TLS/SSO/多租户、数据库复制切换、跨宿主机故障、长期 soak 与目标生产容量不在本轮声明为通过。
