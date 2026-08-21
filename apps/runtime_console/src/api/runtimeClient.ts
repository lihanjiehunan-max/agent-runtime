import type {
  AgentPackageRef,
  CancelTaskResponse,
  ExecutionMode,
  RuntimeEvent,
  RuntimeExecutionResult,
  RuntimeSession,
  RuntimeStatus,
  RuntimeErrorBody,
  TaskStatus,
} from "../types/runtime";
import { DEFAULT_RUNTIME_BASE_URL } from "../types/runtime";

type Fetcher = typeof fetch;

interface RuntimeClientOptions {
  baseUrl?: string;
  token?: string;
  fetcher?: Fetcher;
}

interface SseFrame {
  id: string | null;
  event: string | null;
  data: unknown;
}

export class RuntimeApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly retryable: boolean;

  constructor(
    message: string,
    options: { status?: number; code?: string; retryable?: boolean } = {},
  ) {
    super(sanitizeErrorText(message));
    this.name = "RuntimeApiError";
    this.status = options.status ?? 0;
    this.code = options.code ?? "RUNTIME_UNAVAILABLE";
    this.retryable = options.retryable ?? false;
  }
}

export function parseSseFrames(input: string): { events: SseFrame[]; rest: string } {
  const normalized = input.replaceAll("\r\n", "\n").replaceAll("\r", "\n");
  const chunks = normalized.split("\n\n");
  const rest = chunks.pop() ?? "";
  const events: SseFrame[] = [];

  for (const chunk of chunks) {
    let id: string | null = null;
    let event: string | null = null;
    const dataLines: string[] = [];
    for (const line of chunk.split("\n")) {
      if (!line || line.startsWith(":")) continue;
      const separator = line.indexOf(":");
      const field = separator === -1 ? line : line.slice(0, separator);
      const value = separator === -1 ? "" : line.slice(separator + 1).trimStart();
      if (field === "id") id = value;
      if (field === "event") event = value;
      if (field === "data") dataLines.push(value);
    }
    if (dataLines.length === 0) continue;
    const dataText = dataLines.join("\n");
    let data: unknown = dataText;
    try {
      data = JSON.parse(dataText) as unknown;
    } catch {
      // Non-JSON SSE data is still returned as text; Runtime event endpoints use JSON.
    }
    events.push({ id, event, data });
  }
  return { events, rest };
}

export class RuntimeClient {
  private readonly baseUrl: string;
  private readonly fetcher: Fetcher;
  private token: string | null;

  constructor(options: RuntimeClientOptions = {}) {
    this.baseUrl = normalizeBaseUrl(options.baseUrl ?? DEFAULT_RUNTIME_BASE_URL);
    this.token = options.token?.trim() || null;
    this.fetcher = options.fetcher ?? fetch;
  }

  setToken(token: string | null): void {
    this.token = token?.trim() || null;
  }

  hasToken(): boolean {
    return this.token !== null;
  }

  getBaseUrl(): string {
    return this.baseUrl;
  }

  async getStatus(signal?: AbortSignal): Promise<RuntimeStatus> {
    return this.request<RuntimeStatus>("/status", { signal });
  }

  async createSession(agentId: string, version: string): Promise<RuntimeSession> {
    return this.request<RuntimeSession>("/sessions", {
      method: "POST",
      body: { agent_id: agentId, version },
    });
  }

  async getSession(sessionId: string): Promise<RuntimeSession> {
    return this.request<RuntimeSession>(`/sessions/${encodePath(sessionId)}`);
  }

  async executeSync(
    sessionId: string,
    input: string,
    signal?: AbortSignal,
  ): Promise<RuntimeExecutionResult> {
    return this.request<RuntimeExecutionResult>(
      `/sessions/${encodePath(sessionId)}/executions`,
      { method: "POST", body: { input, mode: "sync" }, signal },
    );
  }

  async *streamExecution(
    sessionId: string,
    input: string,
    signal?: AbortSignal,
  ): AsyncGenerator<RuntimeEvent> {
    const response = await this.openStream(
      `/sessions/${encodePath(sessionId)}/executions/stream`,
      { method: "POST", body: { input, mode: "stream" }, signal },
    );
    yield* this.eventsFromResponse(response, signal);
  }

  async *resumeEvents(
    sessionId: string,
    lastSequence: number,
    signal?: AbortSignal,
  ): AsyncGenerator<RuntimeEvent> {
    const query = `?after_sequence=${Math.max(0, Math.trunc(lastSequence))}`;
    const response = await this.openStream(`/sessions/${encodePath(sessionId)}/events${query}`, {
      headers: { "Last-Event-ID": String(Math.max(0, Math.trunc(lastSequence))) },
      signal,
    });
    yield* this.eventsFromResponse(response, signal);
  }

  async submitTask(sessionId: string, input: string): Promise<TaskStatus> {
    return this.request<TaskStatus>(`/sessions/${encodePath(sessionId)}/tasks`, {
      method: "POST",
      body: { input },
    });
  }

  async getTaskStatus(sessionId: string, executionId: string): Promise<TaskStatus> {
    return this.request<TaskStatus>(
      `/sessions/${encodePath(sessionId)}/tasks/${encodePath(executionId)}`,
    );
  }

  async cancelTask(sessionId: string, executionId: string): Promise<CancelTaskResponse> {
    return this.request<CancelTaskResponse>(
      `/sessions/${encodePath(sessionId)}/tasks/${encodePath(executionId)}/cancel`,
      { method: "POST" },
    );
  }

  private async *eventsFromResponse(
    response: Response,
    signal?: AbortSignal,
  ): AsyncGenerator<RuntimeEvent> {
    if (!response.body) throw new RuntimeApiError("Runtime returned an empty event stream");
    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";
    try {
      while (true) {
        if (signal?.aborted) throw new DOMException("The operation was aborted", "AbortError");
        const chunk = await reader.read();
        buffer += decoder.decode(chunk.value ?? new Uint8Array(), { stream: !chunk.done });
        const parsed = parseSseFrames(buffer);
        buffer = parsed.rest;
        for (const frame of parsed.events) {
          const event = toRuntimeEvent(frame.data);
          if (event) yield event;
        }
        if (chunk.done) break;
      }
      const final = parseSseFrames(`${buffer}\n\n`);
      for (const frame of final.events) {
        const event = toRuntimeEvent(frame.data);
        if (event) yield event;
      }
    } finally {
      reader.releaseLock();
    }
  }

  private async openStream(
    path: string,
    options: RequestOptions = {},
  ): Promise<Response> {
    if (!this.token) {
      throw new RuntimeApiError("A Runtime access token is required", {
        code: "AUTHENTICATION_REQUIRED",
        status: 401,
      });
    }
    let response: Response;
    try {
      response = await this.fetcher(this.url(path), this.init(options));
    } catch (error) {
      if (isAbortError(error)) throw error;
      throw new RuntimeApiError("Runtime is unreachable", { code: "RUNTIME_UNAVAILABLE" });
    }
    if (!response.ok) throw await this.errorFromResponse(response);
    return response;
  }

  private async request<T>(path: string, options: RequestOptions = {}): Promise<T> {
    if (!this.token && path !== "/status") {
      throw new RuntimeApiError("A Runtime access token is required", {
        code: "AUTHENTICATION_REQUIRED",
        status: 401,
      });
    }
    if (path === "/status" && !this.token) {
      throw new RuntimeApiError("A Runtime access token is required", {
        code: "AUTHENTICATION_REQUIRED",
        status: 401,
      });
    }
    let response: Response;
    try {
      response = await this.fetcher(this.url(path), this.init(options));
    } catch (error) {
      if (isAbortError(error)) throw error;
      throw new RuntimeApiError("Runtime is unreachable", { code: "RUNTIME_UNAVAILABLE" });
    }
    if (!response.ok) throw await this.errorFromResponse(response);
    if (response.status === 204) return undefined as T;
    try {
      return (await response.json()) as T;
    } catch {
      throw new RuntimeApiError("Runtime returned an invalid response", {
        status: response.status,
        code: "RUNTIME_INVALID_RESPONSE",
      });
    }
  }

  private init(options: RequestOptions): RequestInit {
    const headers = new Headers(options.headers);
    headers.set("Accept", "application/json, text/event-stream");
    if (options.body !== undefined) headers.set("Content-Type", "application/json");
    if (this.token) headers.set("Authorization", `Bearer ${this.token}`);
    return {
      method: options.method ?? "GET",
      headers,
      body: options.body === undefined ? undefined : JSON.stringify(options.body),
      signal: options.signal,
    };
  }

  private url(path: string): string {
    const separator = this.baseUrl.endsWith("/") ? "" : "/";
    return `${this.baseUrl}${separator}${path.replace(/^\//, "")}`;
  }

  private async errorFromResponse(response: Response): Promise<RuntimeApiError> {
    let body: unknown;
    try {
      body = await response.clone().json();
    } catch {
      body = undefined;
    }
    const error = asErrorBody(body);
    return new RuntimeApiError(error?.message ?? statusMessage(response.status), {
      status: response.status,
      code: error?.code ?? `HTTP_${response.status}`,
      retryable: error?.retryable ?? response.status >= 500,
    });
  }
}

interface RequestOptions {
  method?: string;
  body?: unknown;
  headers?: HeadersInit;
  signal?: AbortSignal;
}

function normalizeBaseUrl(value: string): string {
  const trimmed = (value.trim().split(/[?#]/, 1)[0] || DEFAULT_RUNTIME_BASE_URL);
  return trimmed.replace(/\/$/, "");
}

function encodePath(value: string): string {
  return encodeURIComponent(value);
}

function asErrorBody(value: unknown): RuntimeErrorBody | null {
  if (!isRecord(value) || !isRecord(value.error)) return null;
  const error = value.error;
  if (typeof error.message !== "string" || typeof error.code !== "string") return null;
  return {
    code: error.code,
    message: error.message,
    retryable: typeof error.retryable === "boolean" ? error.retryable : undefined,
  };
}

function toRuntimeEvent(value: unknown): RuntimeEvent | null {
  if (!isRecord(value) || typeof value.event_type !== "string") return null;
  if (
    typeof value.sequence !== "number" ||
    typeof value.session_id !== "string" ||
    typeof value.execution_id !== "string"
  ) {
    return null;
  }
  return value as unknown as RuntimeEvent;
}

function isRecord(value: unknown): value is Record<string, any> {
  return typeof value === "object" && value !== null;
}

function isAbortError(value: unknown): value is DOMException {
  return value instanceof DOMException && value.name === "AbortError";
}

export function sanitizeErrorText(value: string): string {
  return value
    .replace(
      /((?:proxy[-_]?authorization|authorization)\s*[:=]\s*)(?:Basic|Bearer)\s+[^\s,;}\'"]+/gi,
      "$1[redacted]",
    )
    .replace(/\b(Basic|Bearer)\s+[^\s,;}\'"]+/gi, "$1 [redacted]")
    .replace(
      /((?:x-)?(?:client[_-]?secret|refresh[_-]?token|proxy[-_]?authorization|api[_-]?key|access[_-]?token|authorization|password|secret|token)[A-Za-z0-9_-]*\s*[:=]\s*)(?:"[^"]*"|'[^']*'|[^\s,;}\'"]+)/gi,
      "$1[redacted]",
    )
    .slice(0, 1024);
}

function statusMessage(status: number): string {
  if (status === 401) return "Runtime authentication failed";
  if (status === 403) return "Runtime access was denied";
  if (status === 404) return "Runtime resource was not found";
  if (status >= 500) return "Runtime service failed";
  return "Runtime request failed";
}

export type { SseFrame };
