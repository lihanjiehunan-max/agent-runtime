# 多容器验收复核记录

## 首轮证据

- Commit: 26d2564d48c17c65ec104ea005ff7c53b0fdbefe
- GitHub Actions run: 34558003240
- Python: 65 passed, 1 deselected（真实模型网关未调用）
- Node: 92 passed
- Docker: 9 个独立容器，15 组场景脚本通过。
- 50 并发提交全部完成，3 Worker 分配 17 / 17 / 16，运行槽位峰值 6。

首轮脚本的取消门槛是 4 秒，实测 2.004 秒，不满足原方案“2 秒内取消”的严格要求。不能仅凭工作流绿色状态判定原要求达标。

## 修正

根因：Worker 将取消识别与 lease_seconds / 3 的心跳周期绑定，6 秒租约带来约 2 秒取消等待。新增测试稳定复现 CANCEL_REQUESTED 未及时转为 CANCELLED，随后将检查间隔上限收紧到 0.25 秒；Docker 验收断言同时改为不超过 2 秒。

本地全量回归修正后 66 passed, 1 deselected。最终结果必须以包含此修正的提交所产生的 Docker Actions 工件为准，不能沿用首轮的延迟数据。

## 证据和上线边界

自动生成的 validation-results/acceptance.json 包含精确测试提交、场景 PASS/FAIL、Worker 分布、任务和 Attempt、故障耗时、容器身份和镜像 ID。原始容器日志允许包含预期故障注入异常，不能简单把异常日志数量当成验收失败数量。

这是同宿主机多容器预生产模拟。模型响应和业务数据为确定性 HTTP 测试服务；实际 DeepAgents 图、SQL Checkpointer、Redis、MinIO、HTTP/SSE 和容器故障操作是真实执行。没有验证真实模型质量、企业真实工具、跨物理机故障、数据库 HA、SSO/多租户、TLS 或长时间稳定性。不能作为无条件生产上线许可。
