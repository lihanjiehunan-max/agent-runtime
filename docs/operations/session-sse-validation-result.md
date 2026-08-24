# Session SSE Service 验证结果

**执行时间（UTC）：** 2026-08-23T16:51:03Z  
**被测提交：** `de57bc10a9bb0d35a2edd7cf29e2442d1685e1e5`

## 运行环境

- Python：`3.12.13`
- uv：`0.11.33`
- deepagents：`0.7.7`
- Agent 包摘要：`57772ed61fb757df94a23d92f3cc329e525ab20740601bf58313e0860cb7e8eb`

## 确定性门禁

| 门禁 | 命令 | 结果 |
| --- | --- | --- |
| Python（排除外网） | `uv run pytest -m "not real_gateway" -q` | PASS — 58 passed, 1 deselected, 3.66s |
| Python（含外网标记） | `uv run pytest -q` | PASS — 58 passed, 1 skipped, 3.63s |
| Node 回归 | `node --test --test-reporter=spec` | PASS — 92 passed, 0 failed, 431.44ms |
| Python 编译 | `uv run python -m compileall -q apps scripts tests` | PASS |
| 锁文件 | `uv lock --check` | PASS — 77 packages resolved |
| Diff 格式 | `git diff --check` | PASS — 无输出 |

## API 与流式边界

- Bearer 鉴权、公开健康检查、显式幂等部署、Session 创建和 SSE Chat API 测试通过。
- SSE 测试证明 UTF-8 `delta` 按序输出，并以恰好一个 `done` 或 `error` 结束。
- 同 Session 重叠请求、不同 Session 并发、取消、超时和上游失败测试通过。
- 业务 JSON/OpenAPI/SSE forbidden-field 扫描通过；业务输出不含 Agent Instance、thread、digest、Skill、model、gateway、prompt 或 graph 字段。
- 使用哨兵值 `model-secret` 与 `service-secret` 的响应泄漏测试通过。
- 静态凭证模式扫描通过，未发现疑似提交的长 Bearer 或 `sk-...` 凭证。

## 真实网关

**NOT RUN — MODEL_API_KEY and/or SERVICE_API_KEY not configured**

本结果不把跳过记录为 PASS。配置两个环境变量并启动服务后，应执行：

```bash
uv run pytest -m real_gateway -q
uv run python scripts/validate_streaming_service.py \
  --base-url http://127.0.0.1:8000
```

只有真实 smoke 和三轮脚本均成功后，才能将外部结果更新为 PASS。
