export type RuntimeMode = "LIVE" | "DEMO";
export type ExecutionMode = "sync" | "stream" | "async";
export type SessionStatus = "open" | "closed";
export type ExecutionStatus =
  | "accepted"
  | "loading_agent"
  | "acquiring_session_lock"
  | "running"
  | "succeeded"
  | "failed"
  | "timed_out"
  | "cancelled";

export interface AgentPackageRef {
  tenant_id: string;
  agent_id: string;
  version: string;
  digest: string;
  runtime_type: "deepagents";
  sdk_version: string;
}

export interface RuntimeSession {
  session_id: string;
  thread_id: string;
  tenant_id: string;
  user_id: string;
  package: AgentPackageRef;
  status: SessionStatus;
  revision: number;
  execution_epoch: number;
  active_execution_id: string | null;
  last_checkpoint_id: string | null;
  last_event_sequence: number;
  created_at: string;
  updated_at: string;
}

export interface RuntimeEvent {
  schema_version: "runtime.event.v1";
  event_id: string;
  sequence: number;
  occurred_at: string;
  tenant_id: string;
  trace_id: string;
  span_id: string;
  parent_span_id: string | null;
  session_id: string;
  execution_id: string;
  package: AgentPackageRef;
  worker_id: string;
  sdk_version: string;
  event_type: string;
  phase: string;
  duration_ms: number | null;
  payload: Record<string, unknown>;
  payload_ref: string | null;
}

export interface RuntimeStatus {
  schema_version: "runtime.status.v1";
  status: "ready" | "degraded" | "not_ready";
  dependencies: {
    postgres: "configured" | "not_configured";
    minio: "configured" | "not_configured";
    redis: "configured" | "not_configured";
  };
  metrics: "configured" | "not_configured";
}

export interface TaskStatus {
  command_id: string;
  execution_id: string;
  session_id: string;
  tenant_id: string;
  execution_epoch: number;
  status: ExecutionStatus;
  cancellation_requested: boolean;
  error: RuntimeErrorBody | null;
}

export interface CancelTaskResponse {
  execution_id: string;
  status: ExecutionStatus;
  cancellation_requested: boolean;
  already_terminal: boolean;
}

export interface RuntimeErrorBody {
  schema_version?: "runtime.error.v1";
  code: string;
  message: string;
  retryable?: boolean;
  details?: Record<string, unknown>;
}

export interface RuntimeExecutionResult {
  execution_id: string;
  trace_id: string;
  execution_epoch: number;
  status: ExecutionStatus;
  output: unknown | null;
  error: RuntimeErrorBody | null;
}

export interface ConsoleMessage {
  id: string;
  role: "user" | "assistant" | "system";
  text: string;
  mode: RuntimeMode | "INPUT";
  createdAt: string;
}

export interface DemoTurn {
  reply: string;
  events: RuntimeEvent[];
}

export const DEFAULT_RUNTIME_BASE_URL = "/api/v1/runtime";
export const DEFAULT_AGENT_ID = "agent-metric-query";
export const DEFAULT_AGENT_VERSION = "0.1.0";
export const DEMO_DIGEST = `sha256:${"0".repeat(64)}`;
