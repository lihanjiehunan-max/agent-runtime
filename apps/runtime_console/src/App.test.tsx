import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, describe, expect, it, vi } from "vitest";

import App, { extractOutputText } from "./App";
import { ChatPage } from "./pages/ChatPage";
import type { RuntimeEvent, RuntimeSession } from "./types/runtime";

const liveSession: RuntimeSession = {
  session_id: "live-session-1",
  thread_id: "live-thread-1",
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
  execution_epoch: 1,
  active_execution_id: null,
  last_checkpoint_id: null,
  last_event_sequence: 0,
  created_at: "2026-08-20T00:00:00Z",
  updated_at: "2026-08-20T00:00:00Z",
};

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function event(eventType: string, payload: Record<string, unknown>, sequence = 1): RuntimeEvent {
  return {
    schema_version: "runtime.event.v1",
    event_id: `event-${sequence}`,
    sequence,
    occurred_at: "2026-08-20T00:00:00Z",
    tenant_id: liveSession.tenant_id,
    trace_id: "trace-1",
    span_id: `span-${sequence}`,
    parent_span_id: null,
    session_id: liveSession.session_id,
    execution_id: "execution-1",
    package: liveSession.package,
    worker_id: "worker-1",
    sdk_version: liveSession.package.sdk_version,
    event_type: eventType,
    phase: "model",
    duration_ms: null,
    payload,
    payload_ref: null,
  };
}

function sseResponse(events: RuntimeEvent[]): Response {
  const body = events
    .map((item) => `id: ${item.sequence}\nevent: ${item.event_type}\ndata: ${JSON.stringify(item)}\n\n`)
    .join("");
  return new Response(body, {
    status: 200,
    headers: { "content-type": "text/event-stream" },
  });
}

function installLiveFetch(options: {
  syncOutput?: unknown;
  streamEvents?: RuntimeEvent[];
  streamError?: string;
  asyncTask?: boolean;
}) {
  const fetcher = vi.fn<typeof fetch>(async (input, init) => {
    const url = String(input);
    if (url.endsWith("/status")) return jsonResponse({ status: "ready" });
    if (url.endsWith("/sessions") && init?.method === "POST") return jsonResponse(liveSession, 201);
    if (url.includes("/events?")) {
      if (options.streamError) {
        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            controller.error(new Error(options.streamError));
          },
        });
        return new Response(body, { status: 200, headers: { "content-type": "text/event-stream" } });
      }
      return sseResponse(options.streamEvents ?? []);
    }
    if (options.asyncTask && url.endsWith("/tasks") && init?.method === "POST") {
      return jsonResponse({
        command_id: "command-1",
        execution_id: "execution-1",
        session_id: liveSession.session_id,
        tenant_id: liveSession.tenant_id,
        execution_epoch: 1,
        status: "succeeded",
        cancellation_requested: false,
        error: null,
      }, 202);
    }
    if (options.asyncTask && url.includes("/tasks/execution-1")) {
      return jsonResponse({
        command_id: "command-1",
        execution_id: "execution-1",
        session_id: liveSession.session_id,
        tenant_id: liveSession.tenant_id,
        execution_epoch: 1,
        status: "succeeded",
        cancellation_requested: false,
        error: null,
      });
    }
    if (url.endsWith("/executions") && init?.method === "POST") {
      const command = JSON.parse(String(init.body)) as { mode: string };
      if (command.mode === "sync") {
        return jsonResponse({
          execution_id: "execution-1",
          trace_id: "trace-1",
          execution_epoch: 1,
          status: "succeeded",
          output: options.syncOutput ?? null,
          error: null,
        });
      }
    }
    if (url.endsWith("/executions/stream")) {
      if (options.streamError) {
        const body = new ReadableStream<Uint8Array>({
          start(controller) {
            controller.error(new Error(options.streamError));
          },
        });
        return new Response(body, { status: 200, headers: { "content-type": "text/event-stream" } });
      }
      return sseResponse(options.streamEvents ?? []);
    }
    throw new Error(`unexpected test request ${url}`);
  });
  vi.stubGlobal("fetch", fetcher);
  return fetcher;
}

async function connectAndStartLive(user: ReturnType<typeof userEvent.setup>) {
  render(<App />);
  await user.type(screen.getByLabelText(/Bearer token/i), "live-token");
  await user.click(screen.getByRole("button", { name: "Connect LIVE" }));
  await waitFor(() => expect(screen.getByTestId("mode-badge")).toHaveTextContent("LIVE"));
  await user.click(screen.getByRole("button", { name: "Start Session" }));
  await screen.findByText(/Session live-session-1/);
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("Runtime console", () => {
  it("starts in explicit DEMO mode and keeps the same Session across turns", async () => {
    const user = userEvent.setup();
    render(<App />);

    expect(screen.getByTestId("mode-badge")).toHaveTextContent("DEMO");
    await user.click(screen.getByRole("button", { name: "Start Session" }));
    expect(await screen.findByText(/Session demo-session-1/)).toBeInTheDocument();

    const input = screen.getByLabelText("Message");
    await user.type(input, "第一轮问题");
    await user.click(screen.getByRole("button", { name: "Send message" }));
    expect(await screen.findByText(/DEMO answer for “第一轮问题”/)).toBeInTheDocument();

    await user.type(input, "第二轮问题");
    await user.click(screen.getByRole("button", { name: "Send message" }));
    expect(await screen.findByText(/DEMO answer for “第二轮问题”/)).toBeInTheDocument();
    expect(screen.getByText(/Session demo-session-1/)).toBeInTheDocument();
  });

  it("does not label a failed configured connection as a live result", async () => {
    const user = userEvent.setup();
    const originalFetch = globalThis.fetch;
    globalThis.fetch = vi.fn<typeof fetch>().mockRejectedValue(new Error("network token=should-not-render"));
    try {
      render(<App />);
      await user.type(screen.getByLabelText(/Bearer token/i), "secret-token-value");
      await user.click(screen.getByRole("button", { name: "Connect LIVE" }));
      await waitFor(() => expect(screen.getByTestId("mode-badge")).toHaveTextContent("DEMO"));
      expect(screen.getByRole("alert")).toHaveTextContent("No live result was generated");
      expect(screen.getByRole("alert")).not.toHaveTextContent("secret-token-value");
      expect(screen.getByRole("button", { name: "Use DEMO" })).toBeInTheDocument();
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  it("exposes all five operational views", async () => {
    const user = userEvent.setup();
    render(<App />);
    for (const name of ["Agents", "Sessions", "Chat", "Trace", "Dashboard"]) {
      await user.click(screen.getByRole("button", { name: new RegExp(`^${name}`) }));
      expect(screen.getByRole("heading", { name })).toBeInTheDocument();
    }
  });

  it("renders the flat sync response and assistant messages output", async () => {
    const user = userEvent.setup();
    installLiveFetch({
      syncOutput: {
        messages: [
          { role: "user", content: "问题" },
          { role: "assistant", content: "真实同步回答" },
        ],
      },
    });
    await connectAndStartLive(user);

    await user.selectOptions(screen.getByLabelText("Execution"), "sync");
    await user.type(screen.getByLabelText("Message"), "问题");
    await user.click(screen.getByRole("button", { name: "Send message" }));

    expect(await screen.findByText("真实同步回答")).toBeInTheDocument();
    expect(screen.queryByText(/Cannot read properties|undefined/)).not.toBeInTheDocument();
  });

  it("extracts the backend last_message_text execution output", () => {
    expect(extractOutputText({ last_message_text: "后端最后一条消息" })).toBe("后端最后一条消息");
  });

  it("renders model.delta payload text from the live stream", async () => {
    const user = userEvent.setup();
    installLiveFetch({
      streamEvents: [
        event("model.delta", { text: "流式回答" }),
        event("execution.succeeded", {}, 2),
      ],
    });
    await connectAndStartLive(user);

    await user.type(screen.getByLabelText("Message"), "问题");
    await user.click(screen.getByRole("button", { name: "Send message" }));

    expect(await screen.findByText("流式回答")).toBeInTheDocument();
  });

  it("preserves model.delta text after an async task succeeds", async () => {
    const user = userEvent.setup();
    installLiveFetch({
      asyncTask: true,
      streamEvents: [
        event("model.delta", { text: "异步真实回答" }),
        event("execution.succeeded", {}, 2),
      ],
    });
    await connectAndStartLive(user);

    await user.selectOptions(screen.getByLabelText("Execution"), "async");
    await user.type(screen.getByLabelText("Message"), "异步问题");
    await user.click(screen.getByRole("button", { name: "Send message" }));

    expect(await screen.findByText("异步真实回答")).toBeInTheDocument();
    expect(screen.queryByText("Execution succeeded.")).not.toBeInTheDocument();
  });

  it("caps the total streamed answer instead of only each delta", async () => {
    const user = userEvent.setup();
    const firstDelta = "a".repeat(5000);
    const secondDelta = "b".repeat(5000);
    installLiveFetch({
      streamEvents: [
        event("model.delta", { text: firstDelta }),
        event("model.delta", { text: secondDelta }, 2),
        event("execution.succeeded", {}, 3),
      ],
    });
    await connectAndStartLive(user);

    await user.type(screen.getByLabelText("Message"), "长回答问题");
    await user.click(screen.getByRole("button", { name: "Send message" }));

    const assistantParagraph = screen
      .getByTestId("chat-transcript")
      .querySelector(".message-assistant p");
    expect(assistantParagraph?.textContent).toBe(`${firstDelta}${secondDelta.slice(0, 3192)}`);
  });

  it("makes non-async cancellation explicit as browser-only", async () => {
    const user = userEvent.setup();
    render(
      <ChatPage
        mode="LIVE"
        modeReason="connected"
        session={null}
        messages={[]}
        busy
        error={null}
        executionMode="stream"
        onExecutionModeChange={vi.fn()}
        onSend={async () => undefined}
        onCancel={async () => undefined}
        onCreateSession={vi.fn()}
      />,
    );

    expect(screen.getByRole("button", { name: /Stop browser wait/i })).toBeInTheDocument();
    expect(screen.getByText(/Runtime execution may continue/i)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /Stop browser wait/i }));
  });

  it("clears the Session when switching between LIVE and DEMO", async () => {
    const user = userEvent.setup();
    installLiveFetch({ streamEvents: [] });
    render(<App />);

    await user.click(screen.getByRole("button", { name: "Start Session" }));
    await screen.findByText(/Session demo-session-/);
    await user.type(screen.getByLabelText(/Bearer token/i), "live-token");
    await user.click(screen.getByRole("button", { name: "Connect LIVE" }));
    await waitFor(() => expect(screen.getByTestId("mode-badge")).toHaveTextContent("LIVE"));
    expect(screen.queryByText(/Session demo-session-/)).not.toBeInTheDocument();
    expect(screen.getByText("No Session selected")).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "Use DEMO" }));
    expect(screen.getByTestId("mode-badge")).toHaveTextContent("DEMO");
    expect(screen.getByText("No Session selected")).toBeInTheDocument();
  });

  it("redacts generic credential forms from unexpected errors", async () => {
    const user = userEvent.setup();
    installLiveFetch({
      streamError: "upstream authorization=raw-auth api_key=raw-key token=raw-token",
    });
    await connectAndStartLive(user);

    await user.type(screen.getByLabelText("Message"), "问题");
    await user.click(screen.getByRole("button", { name: "Send message" }));
    const alert = await screen.findByRole("alert");
    expect(alert).not.toHaveTextContent("raw-auth");
    expect(alert).not.toHaveTextContent("raw-key");
    expect(alert).not.toHaveTextContent("raw-token");
  });
});
