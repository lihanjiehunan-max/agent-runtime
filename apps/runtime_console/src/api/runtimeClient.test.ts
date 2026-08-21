import { describe, expect, it, vi } from "vitest";

import { RuntimeApiError, RuntimeClient, parseSseFrames, sanitizeErrorText } from "./runtimeClient";

const session = {
  session_id: "session-1",
  thread_id: "thread-1",
  tenant_id: "tenant-1",
  user_id: "user-1",
  package: {
    tenant_id: "tenant-1",
    agent_id: "agent-metric-query",
    version: "0.1.0",
    digest: `sha256:${"1".repeat(64)}`,
    runtime_type: "deepagents",
    sdk_version: "0.7.7",
  },
  status: "open",
  revision: 0,
  execution_epoch: 0,
  active_execution_id: null,
  last_checkpoint_id: null,
  last_event_sequence: 0,
  created_at: "2026-08-20T00:00:00Z",
  updated_at: "2026-08-20T00:00:00Z",
};

function response(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

describe("RuntimeClient", () => {
  it("keeps bearer authentication out of URLs and normalized error text", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(
      response({ error: { code: "INVALID_ACCESS_TOKEN", message: "bad token=secret" } }, 401),
    );
    const client = new RuntimeClient({
      baseUrl: "https://runtime.example.test/api/v1/runtime?token=never",
      token: "secret-token-value",
      fetcher,
    });

    await expect(client.getStatus()).rejects.toMatchObject({
      code: "INVALID_ACCESS_TOKEN",
      message: expect.not.stringContaining("secret-token-value"),
    });
    const [url, init] = fetcher.mock.calls[0];
    expect(String(url)).not.toContain("secret-token-value");
    expect(String(url)).not.toContain("token=never");
    expect(new Headers(init?.headers).get("Authorization")).toBe("Bearer secret-token-value");
  });

  it("creates a Session with the typed package request", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(response(session, 201));
    const client = new RuntimeClient({ token: "dev-token", fetcher });

    await expect(client.createSession("agent-metric-query", "0.1.0")).resolves.toEqual(session);
    expect(fetcher.mock.calls[0][0]).toBe("/api/v1/runtime/sessions");
    expect(JSON.parse(String(fetcher.mock.calls[0][1]?.body))).toEqual({
      agent_id: "agent-metric-query",
      version: "0.1.0",
    });
  });

  it("parses SSE frames across chunks and ignores comments", () => {
    const first = parseSseFrames(
      ": heartbeat\n\nid: 1\nevent: execution.started\ndata: {\"sequence\":1}\n\n",
    );
    expect(first.events).toEqual([
      { id: "1", event: "execution.started", data: { sequence: 1 } },
    ]);
    const second = parseSseFrames("id: 2\nevent: execution.output\ndata: {\"text\":\"hel");
    expect(second.events).toHaveLength(0);
    const third = parseSseFrames(`${second.rest}lo\"}\n\n`);
    expect(third.events[0]).toMatchObject({ id: "2", data: { text: "hello" } });
  });

  it("sends Last-Event-ID when resuming a Session event stream", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(
        `id: 2\nevent: execution.succeeded\ndata: ${JSON.stringify({
          schema_version: "runtime.event.v1",
          event_id: "event-2",
          sequence: 2,
          occurred_at: "2026-08-20T00:00:00Z",
          tenant_id: "tenant-1",
          trace_id: "trace-1",
          span_id: "span-2",
          parent_span_id: null,
          session_id: "session-1",
          execution_id: "execution-1",
          package: session.package,
          worker_id: "worker-1",
          sdk_version: "0.7.7",
          event_type: "execution.succeeded",
          phase: "runtime",
          duration_ms: null,
          payload: {},
          payload_ref: null,
        })}\n\n`,
        {
        status: 200,
        headers: { "content-type": "text/event-stream" },
        },
      ),
    );
    const client = new RuntimeClient({ token: "dev-token", fetcher });
    const events = [];
    for await (const event of client.resumeEvents("session-1", 1)) events.push(event);

    expect(events[0]).toMatchObject({ sequence: 2, event_type: "execution.succeeded" });
    const [url, init] = fetcher.mock.calls[0];
    expect(String(url)).toBe("/api/v1/runtime/sessions/session-1/events?after_sequence=1");
    expect(new Headers(init?.headers).get("Last-Event-ID")).toBe("1");
  });

  it("supports task cancellation with the Session-scoped endpoint", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(
      response({ execution_id: "execution-1", status: "cancelled", cancellation_requested: true, already_terminal: false }),
    );
    const client = new RuntimeClient({ token: "dev-token", fetcher });

    await expect(client.cancelTask("session-1", "execution-1")).resolves.toMatchObject({ status: "cancelled" });
    expect(fetcher.mock.calls[0][0]).toBe("/api/v1/runtime/sessions/session-1/tasks/execution-1/cancel");
  });

  it("preserves AbortError so the console can report browser-only cancellation", async () => {
    const fetcher = vi.fn<typeof fetch>().mockRejectedValue(
      new DOMException("The operation was aborted", "AbortError"),
    );
    const client = new RuntimeClient({ token: "dev-token", fetcher });
    const controller = new AbortController();

    await expect(client.executeSync("session-1", "question", controller.signal)).rejects.toMatchObject({
      name: "AbortError",
    });
  });

  it("exposes normalized errors", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(
      response({ error: { code: "RUNTIME_INCOMPATIBLE", message: "upstream failed" } }, 503),
    );
    const client = new RuntimeClient({ token: "dev-token", fetcher });
    await expect(client.getStatus()).rejects.toBeInstanceOf(RuntimeApiError);
    await expect(client.getStatus()).rejects.toMatchObject({ status: 503, code: "RUNTIME_INCOMPATIBLE", message: "upstream failed" });
  });

  it("redacts full Basic and Bearer authorization values plus credential key forms", () => {
    const raw = [
      "authorization=Basic basic-secret",
      "authorization: Bearer bearer-secret",
      "token=token-secret",
      "api_key=api-secret",
      "x-api-key: header-secret",
      "accessToken=camel-secret",
    ].join(" ");

    const sanitized = sanitizeErrorText(raw);

    for (const secret of [
      "basic-secret",
      "bearer-secret",
      "token-secret",
      "api-secret",
      "header-secret",
      "camel-secret",
    ]) {
      expect(sanitized).not.toContain(secret);
    }
  });
});
