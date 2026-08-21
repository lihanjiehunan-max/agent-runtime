import {
  DEFAULT_AGENT_ID,
  DEFAULT_AGENT_VERSION,
  DEMO_DIGEST,
  type AgentPackageRef,
  type DemoTurn,
  type RuntimeEvent,
  type RuntimeSession,
} from "../types/runtime";

export const DEMO_REASON = "No Runtime token configured; deterministic demo data is active.";

export const DEMO_PACKAGE: AgentPackageRef = {
  tenant_id: "demo-tenant",
  agent_id: DEFAULT_AGENT_ID,
  version: DEFAULT_AGENT_VERSION,
  digest: DEMO_DIGEST,
  runtime_type: "deepagents",
  sdk_version: "0.7.7",
};

let demoCounter = 0;

export function createDemoSession(): RuntimeSession {
  demoCounter += 1;
  const now = new Date().toISOString();
  return {
    session_id: `demo-session-${demoCounter}`,
    thread_id: `demo-thread-${demoCounter}`,
    tenant_id: "demo-tenant",
    user_id: "demo-user",
    package: DEMO_PACKAGE,
    status: "open",
    revision: 0,
    execution_epoch: 0,
    active_execution_id: null,
    last_checkpoint_id: null,
    last_event_sequence: 0,
    created_at: now,
    updated_at: now,
  };
}

export function demoTurn(session: RuntimeSession, input: string, turn: number): DemoTurn {
  const executionId = `demo-execution-${session.session_id}-${turn}`;
  const traceId = `demo-trace-${session.session_id}-${turn}`;
  const baseSequence = session.last_event_sequence + (turn - 1) * 4;
  const events = [
    demoEvent(session, executionId, traceId, baseSequence + 1, "execution.started", "runtime", {
      model_ref: "demo-model",
      cache_hit: turn > 1,
    }),
    demoEvent(session, executionId, traceId, baseSequence + 2, "tool.called", "tool", {
      tool_name: "query_metric",
      tool_version: "demo-1",
      metric: "营业收入",
      input_tokens: 32,
      output_tokens: 8,
    }),
    demoEvent(session, executionId, traceId, baseSequence + 3, "execution.output", "result", {
      text: `DEMO answer for “${input}”. This is deterministic fixture output, not a live model result.`,
      input_tokens: 32,
      output_tokens: 24,
    }),
    demoEvent(session, executionId, traceId, baseSequence + 4, "execution.succeeded", "runtime", {
      duration_ms: 42,
      cache_hit: turn > 1,
    }),
  ];
  return {
    reply: String(events[2].payload.text),
    events,
  };
}

function demoEvent(
  session: RuntimeSession,
  executionId: string,
  traceId: string,
  sequence: number,
  eventType: string,
  phase: string,
  payload: Record<string, unknown>,
): RuntimeEvent {
  return {
    schema_version: "runtime.event.v1",
    event_id: `demo-event-${session.session_id}-${sequence}`,
    sequence,
    occurred_at: new Date(Date.now() + sequence).toISOString(),
    tenant_id: session.tenant_id,
    trace_id: traceId,
    span_id: `demo-span-${sequence}`,
    parent_span_id: null,
    session_id: session.session_id,
    execution_id: executionId,
    package: session.package,
    worker_id: "demo-worker",
    sdk_version: session.package.sdk_version,
    event_type: eventType,
    phase,
    duration_ms: phase === "runtime" ? 42 : null,
    payload,
    payload_ref: null,
  };
}
