# Session SSE Service 运行手册

该服务用于验证一个静态 Skill、一个静态模型网关、显式 Agent 部署、Session 多轮记忆和真实 SSE Chat 输出。它是单进程内存态 MVP；进程重启后 Agent Instance、Session 和对话上下文都会丢失。

## 配置

启动前设置两个环境变量：

- `MODEL_API_KEY`：传给 `https://token.zero-api.cc.cd/v1` 的模型凭证。
- `SERVICE_API_KEY`：外部调用 Runtime API 时使用的 Bearer Token。

不要把值写入配置文件、命令历史、日志或文档。本文中的占位符必须由部署环境的密钥管理机制替换。

## 启动

在仓库根目录执行：

```bash
MODEL_API_KEY='<configured locally>' \
SERVICE_API_KEY='<configured locally>' \
uv run uvicorn apps.validation_runtime.main:app \
  --host 0.0.0.0 --port 8000
```

主机、防火墙、容器或云安全组需要放行 TCP 8000。公网 TLS 应由 Nginx、Ingress 或负载均衡器终止；Uvicorn 本身不负责本 MVP 的证书管理。

反向代理必须关闭 SSE 缓冲并允许超过 120 秒的上游读取时间。服务响应已包含 `X-Accel-Buffering: no` 和 `Cache-Control: no-cache`。

## 健康检查

健康检查不需要鉴权，也不返回模型或运行时信息：

```bash
curl http://127.0.0.1:8000/healthz
```

## 显式部署 Agent

```bash
curl -sS -X POST \
  -H "Authorization: Bearer ${SERVICE_API_KEY}" \
  http://127.0.0.1:8000/api/v1/runtime/ops/agents/shipping-analyst/deploy
```

该接口幂等返回运维可见的 `agent_instance_id`、模型、Skill 和包摘要。业务接口从不接收 Agent Instance ID。

## 创建 Session

```bash
curl -sS -X POST \
  -H "Authorization: Bearer ${SERVICE_API_KEY}" \
  http://127.0.0.1:8000/api/v1/runtime/agents/shipping-analyst/sessions
```

保存响应中的 `session_id`。Session 在内部固定绑定到部署实例，并使用相同值作为 LangGraph thread ID。

## 流式 Chat

```bash
SESSION_ID='ses_replace_me'
curl -N -X POST \
  -H "Authorization: Bearer ${SERVICE_API_KEY}" \
  -H 'Content-Type: application/json' \
  --data '{"message":"请介绍你能提供的航运经营分析帮助。"}' \
  "http://127.0.0.1:8000/api/v1/runtime/sessions/${SESSION_ID}/chat"
```

成功流包含若干 `delta` 和一个 `done`；网关失败或超时包含一个终态 `error`。同一 Session 同时只能存在一个流，重叠请求返回 `409 SESSION_BUSY`。

## 三轮验收

服务启动后执行：

```bash
SERVICE_API_KEY='<configured locally>' \
uv run python scripts/validate_streaming_service.py \
  --base-url http://127.0.0.1:8000
```

脚本验证 Herry/散运的同 Session 记忆、`结论/依据/建议` 结构、SSE 终态和凭证不回传。

## 已知边界

- 无数据库、Redis、队列、后台 Worker、自动重试或断线续传。
- 无 WebSocket、OpenAI-compatible `/v1/chat/completions` 或前端页面。
- 一个 Bearer Token 同时保护业务和运维路径；该 MVP 不实现人员角色或 RBAC。
- Deep Agents 的文件、执行和任务工具以及默认通用子代理均被禁用。
- 不存在 Demo fallback；成功回答只能来自配置的真实网关。
