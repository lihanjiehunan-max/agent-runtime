import { useMemo, useRef, useState } from "react";

import { RuntimeApiError, RuntimeClient, sanitizeErrorText } from "./api/runtimeClient";
import { DEMO_REASON, createDemoSession, demoTurn } from "./demo/demoRuntime";
import { ModeBadge } from "./components/ModeBadge";
import { AgentsPage } from "./pages/AgentsPage";
import { ChatPage } from "./pages/ChatPage";
import { DashboardPage } from "./pages/DashboardPage";
import { SessionsPage } from "./pages/SessionsPage";
import { TracePage } from "./pages/TracePage";
import {
  DEFAULT_AGENT_ID,
  DEFAULT_AGENT_VERSION,
  DEFAULT_RUNTIME_BASE_URL,
  type ConsoleMessage,
  type ExecutionMode,
  type RuntimeEvent,
  type RuntimeMode,
  type RuntimeSession,
} from "./types/runtime";
import "./styles.css";

type View = "agents" | "sessions" | "chat" | "trace" | "dashboard";

const NAV_ITEMS: Array<{ id: View; label: string; hint: string }> = [
  { id: "agents", label: "Agents", hint: "package" },
  { id: "sessions", label: "Sessions", hint: "threads" },
  { id: "chat", label: "Chat", hint: "multi-turn" },
  { id: "trace", label: "Trace", hint: "events" },
  { id: "dashboard", label: "Dashboard", hint: "metrics" },
];

export default function App() {
  const [view, setView] = useState<View>("chat");
  const [mode, setMode] = useState<RuntimeMode>("DEMO");
  const [modeReason, setModeReason] = useState(DEMO_REASON);
  const [demoEnabled, setDemoEnabled] = useState(true);
  const [apiBase, setApiBase] = useState(
    import.meta.env.VITE_RUNTIME_API_BASE_URL || DEFAULT_RUNTIME_BASE_URL,
  );
  const [token, setToken] = useState("");
  const [client, setClient] = useState(() => new RuntimeClient());
  const [sessions, setSessions] = useState<RuntimeSession[]>([]);
  const [selectedSessionId, setSelectedSessionId] = useState<string | null>(null);
  const [messagesBySession, setMessagesBySession] = useState<Record<string, ConsoleMessage[]>>({});
  const [eventsBySession, setEventsBySession] = useState<Record<string, RuntimeEvent[]>>({});
  const [executionMode, setExecutionMode] = useState<ExecutionMode>("stream");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const taskRef = useRef<{ sessionId: string; executionId: string } | null>(null);
  const cancelMessageRef = useRef<string | null>(null);

  const selectedSession = sessions.find((session) => session.session_id === selectedSessionId) ?? null;
  const selectedMessages = selectedSessionId ? messagesBySession[selectedSessionId] ?? [] : [];
  const selectedEvents = selectedSessionId ? eventsBySession[selectedSessionId] ?? [] : [];
  const selectedPackage = selectedSession?.package ?? null;

  const connectionLabel = useMemo(() => {
    if (mode === "LIVE") return "Connected to the same-origin Runtime API";
    return modeReason;
  }, [mode, modeReason]);

  function resetSessionState() {
    abortRef.current?.abort();
    abortRef.current = null;
    taskRef.current = null;
    cancelMessageRef.current = null;
    setBusy(false);
    setSessions([]);
    setSelectedSessionId(null);
    setMessagesBySession({});
    setEventsBySession({});
  }

  async function connectLive() {
    const nextClient = new RuntimeClient({ baseUrl: apiBase, token });
    resetSessionState();
    setError(null);
    if (!token.trim()) {
      setClient(nextClient);
      setMode("DEMO");
      setModeReason(DEMO_REASON);
      setDemoEnabled(true);
      return;
    }
    try {
      const status = await nextClient.getStatus();
      setClient(nextClient);
      setMode("LIVE");
      setModeReason(`Runtime status: ${status.status}.`);
      setDemoEnabled(false);
    } catch (caught) {
      const message = safeErrorMessage(caught);
      setClient(nextClient);
      setMode("DEMO");
      setModeReason(`LIVE connection failed: ${message}. Choose DEMO explicitly to continue.`);
      setDemoEnabled(false);
      setError("The configured Runtime is unavailable. No live result was generated.");
    }
  }

  function switchToDemo() {
    resetSessionState();
    client.setToken(null);
    setToken("");
    setMode("DEMO");
    setModeReason("Manual DEMO mode is active; all replies are deterministic fixture output.");
    setDemoEnabled(true);
    setError(null);
  }

  async function createSession(): Promise<RuntimeSession | null> {
    setError(null);
    if (mode === "DEMO") {
      if (!demoEnabled) {
        setError("DEMO is disabled until you choose the visible Use DEMO action.");
        return null;
      }
      const session = createDemoSession();
      registerSession(session);
      return session;
    }
    try {
      const session = await client.createSession(DEFAULT_AGENT_ID, DEFAULT_AGENT_VERSION);
      registerSession(session);
      return session;
    } catch (caught) {
      setError(`Live Session creation failed: ${safeErrorMessage(caught)}`);
      return null;
    }
  }

  function registerSession(session: RuntimeSession) {
    setSessions((current) => [
      ...current.filter((item) => item.session_id !== session.session_id),
      session,
    ]);
    setSelectedSessionId(session.session_id);
  }

  function updateSession(sessionId: string, updater: (session: RuntimeSession) => RuntimeSession) {
    setSessions((current) => current.map((session) => (
      session.session_id === sessionId ? updater(session) : session
    )));
  }

  function addMessage(sessionId: string, message: ConsoleMessage) {
    setMessagesBySession((current) => ({
      ...current,
      [sessionId]: [...(current[sessionId] ?? []), message],
    }));
  }

  function addEvent(sessionId: string, event: RuntimeEvent) {
    setEventsBySession((current) => {
      const existing = current[sessionId] ?? [];
      const next = [...existing.filter((item) => item.event_id !== event.event_id), event]
        .sort((left, right) => left.sequence - right.sequence);
      return { ...current, [sessionId]: next.slice(-512) };
    });
    updateSession(sessionId, (session) => ({
      ...session,
      last_event_sequence: Math.max(session.last_event_sequence, event.sequence),
      updated_at: event.occurred_at,
    }));
  }

  async function sendTurn(input: string, selectedMode: ExecutionMode) {
    let session = selectedSession;
    if (!session) session = await createSession();
    if (!session) return;
    const isDemoSession = session.session_id.startsWith("demo-session-");
    if ((mode === "DEMO") !== isDemoSession) {
      setSelectedSessionId(null);
      setError("Selected Session does not belong to the current Runtime mode. Start a new Session.");
      return;
    }
    const messageId = `${session.session_id}-input-${Date.now()}`;
    addMessage(session.session_id, {
      id: messageId,
      role: "user",
      text: input,
      mode: "INPUT",
      createdAt: new Date().toISOString(),
    });
    setError(null);
    setBusy(true);
    const controller = new AbortController();
    abortRef.current = controller;
    taskRef.current = null;
    cancelMessageRef.current = null;
    let lastSequence = eventsBySession[session.session_id]?.at(-1)?.sequence ?? session.last_event_sequence;
    let streamedOutput = "";
    let finalOutput = "";
    let statusFallback = "";

    const consumeEvent = (event: RuntimeEvent) => {
      lastSequence = Math.max(lastSequence, event.sequence);
      addEvent(session.session_id, event);
      if (event.event_type === "model.delta") {
        streamedOutput = `${streamedOutput}${extractOutputText(event.payload)}`.slice(0, MAX_CONSOLE_OUTPUT_CHARS);
      } else if (event.event_type === "execution.output") {
        finalOutput = extractOutputText(event.payload) || finalOutput;
      }
    };

    try {
      if (mode === "DEMO") {
        const turn = (messagesBySession[session.session_id] ?? []).filter((item) => item.role === "user").length;
        const demo = demoTurn(session, input, turn);
        demo.events.forEach(consumeEvent);
        finalOutput = demo.reply;
      } else if (selectedMode === "stream") {
        for await (const event of client.streamExecution(session.session_id, input, controller.signal)) {
          consumeEvent(event);
        }
      } else if (selectedMode === "sync") {
        const result = await client.executeSync(session.session_id, input, controller.signal);
        finalOutput = extractOutputText(result.output) || `Execution ${result.status}.`;
        for await (const event of client.resumeEvents(session.session_id, lastSequence, controller.signal)) {
          consumeEvent(event);
        }
      } else {
        const task = await client.submitTask(session.session_id, input);
        taskRef.current = { sessionId: session.session_id, executionId: task.execution_id };
        let status = task;
        while (!isTerminal(status.status)) {
          await wait(250, controller.signal);
          status = await client.getTaskStatus(session.session_id, task.execution_id);
        }
        for await (const event of client.resumeEvents(session.session_id, lastSequence, controller.signal)) {
          consumeEvent(event);
        }
        finalOutput = status.error?.message || "";
        statusFallback = `Execution ${status.status}.`;
      }
      const output = finalOutput || streamedOutput || statusFallback;
      if (output) {
        addMessage(session.session_id, {
          id: `${session.session_id}-assistant-${Date.now()}`,
          role: "assistant",
          text: output,
          mode,
          createdAt: new Date().toISOString(),
        });
      }
    } catch (caught) {
      if (isAbortError(caught)) {
        setError(cancelMessageRef.current ?? "Browser request stopped; no completed answer was presented.");
        cancelMessageRef.current = null;
      } else if (selectedMode === "stream" && mode === "LIVE" && !controller.signal.aborted) {
        try {
          for await (const event of client.resumeEvents(session.session_id, lastSequence, controller.signal)) {
            consumeEvent(event);
          }
          const output = finalOutput || streamedOutput;
          if (output) {
            addMessage(session.session_id, {
              id: `${session.session_id}-assistant-resumed-${Date.now()}`,
              role: "assistant",
              text: output,
              mode: "LIVE",
              createdAt: new Date().toISOString(),
            });
          }
        } catch (resumeError) {
          setError(`Live turn failed and could not resume: ${safeErrorMessage(resumeError)}`);
        }
      } else {
        setError(`${mode} turn failed: ${safeErrorMessage(caught)}`);
      }
    } finally {
      abortRef.current = null;
      taskRef.current = null;
      setBusy(false);
    }
  }

  async function cancelTurn() {
    const task = taskRef.current;
    let message: string;
    if (task && mode === "LIVE" && executionMode === "async") {
      try {
        const response = await client.cancelTask(task.sessionId, task.executionId);
        message = response.already_terminal
          ? "Runtime task was already terminal; stopped waiting for it."
          : "Runtime cancellation requested; stopped waiting for the task.";
      } catch (caught) {
        message = `Runtime cancellation request failed; browser wait stopped: ${safeErrorMessage(caught)}`;
      }
    } else if (executionMode === "async") {
      message = "Browser request stopped before Runtime cancellation was requested; the Runtime task may continue.";
    } else {
      message = "Browser request stopped; Runtime execution may continue because sync/stream cancellation is not exposed by this API.";
    }
    cancelMessageRef.current = message;
    abortRef.current?.abort();
  }

  let content: React.ReactNode;
  if (view === "agents") {
    content = <AgentsPage mode={mode} packageRef={selectedPackage} session={selectedSession} onCreateSession={() => void createSession()} />;
  } else if (view === "sessions") {
    content = (
      <SessionsPage
        mode={mode}
        sessions={sessions}
        selectedSessionId={selectedSessionId}
        onSelect={setSelectedSessionId}
        onCreate={() => void createSession()}
      />
    );
  } else if (view === "trace") {
    content = <TracePage mode={mode} events={selectedEvents} sessionId={selectedSessionId} />;
  } else if (view === "dashboard") {
    content = <DashboardPage mode={mode} sessions={sessions} events={selectedEvents} />;
  } else {
    content = (
      <ChatPage
        mode={mode}
        modeReason={connectionLabel}
        session={selectedSession}
        messages={selectedMessages}
        busy={busy}
        error={error}
        executionMode={executionMode}
        onExecutionModeChange={setExecutionMode}
        onSend={sendTurn}
        onCancel={cancelTurn}
        onCreateSession={() => void createSession()}
      />
    );
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand-lockup">
          <span className="brand-mark">AR</span>
          <div>
            <strong>Agent Runtime</strong>
            <span>control console</span>
          </div>
        </div>
        <ModeBadge mode={mode} reason={mode === "DEMO" ? "fixture" : "connected"} />
        <nav className="main-nav" aria-label="Primary navigation">
          {NAV_ITEMS.map((item) => (
            <button
              className={`nav-item ${view === item.id ? "active" : ""}`}
              key={item.id}
              type="button"
              onClick={() => setView(item.id)}
            >
              <span>{item.label}</span>
              <small>{item.hint}</small>
            </button>
          ))}
        </nav>
        <div className="sidebar-footnote">
          <span className="mono">runtime.event.v1</span>
          <span>Evidence stays bounded and tenant-scoped.</span>
        </div>
      </aside>

      <main className="main-column">
        <header className="topbar">
          <div>
            <span className="topbar-kicker">Python Runtime MVP</span>
            <span className="topbar-title">Package → Session → Execution</span>
          </div>
          <div className="topbar-status">
            <span className={`connection-light connection-${mode.toLowerCase()}`} />
            {mode === "LIVE" ? "Runtime connected" : "Deterministic demo"}
          </div>
        </header>

        <section className="connection-card" aria-label="Runtime connection">
          <div className="connection-copy">
            <span className="eyebrow">Connection</span>
            <strong>{mode === "LIVE" ? "Live Runtime" : "Demo fallback"}</strong>
            <span>{modeReason}</span>
          </div>
          <div className="connection-controls">
            <label>
              API base
              <input value={apiBase} onChange={(event) => setApiBase(event.target.value)} />
            </label>
            <label htmlFor="runtime-token">
              Bearer token <span className="muted">(memory only)</span>
              <input
                id="runtime-token"
                type="password"
                value={token}
                onChange={(event) => setToken(event.target.value)}
                placeholder="Not stored"
                autoComplete="off"
              />
            </label>
            <div className="connection-buttons">
              <button className="secondary-button" type="button" onClick={() => void connectLive()}>
                Connect LIVE
              </button>
              {mode !== "DEMO" || !demoEnabled ? (
                <button className="primary-button" type="button" onClick={switchToDemo}>
                  Use DEMO
                </button>
              ) : null}
            </div>
          </div>
        </section>

        <div className="content-wrap">{content}</div>
      </main>
    </div>
  );
}

const MAX_CONSOLE_OUTPUT_CHARS = 8192;

export function extractOutputText(value: unknown): string {
  return extractOutputTextAtDepth(value, 0).slice(0, MAX_CONSOLE_OUTPUT_CHARS);
}

function extractOutputTextAtDepth(value: unknown, depth: number): string {
  if (depth > 6) return "";
  if (typeof value === "string") return value;
  if (Array.isArray(value)) {
    return value
      .map((item) => extractOutputTextAtDepth(item, depth + 1))
      .filter(Boolean)
      .join("")
      .slice(0, MAX_CONSOLE_OUTPUT_CHARS);
  }
  if (!isRecord(value)) return "";

  if (Array.isArray(value.messages)) {
    const messages = value.messages;
    const assistantMessages = messages.filter(isAssistantMessage);
    return extractOutputTextAtDepth(
      assistantMessages.at(-1) ?? messages.at(-1),
      depth + 1,
    );
  }
  for (const key of ["text", "content", "answer", "output", "last_message_text"]) {
    if (key in value) {
      const text = extractOutputTextAtDepth(value[key], depth + 1);
      if (text) return text;
    }
  }
  return "";
}

function isAssistantMessage(value: unknown): boolean {
  if (!isRecord(value)) return false;
  const role = value.role ?? value.type;
  return role === "assistant" || role === "ai" || role === "model";
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

export function safeErrorMessage(value: unknown): string {
  if (value instanceof RuntimeApiError) return sanitizeErrorText(value.message);
  if (value instanceof Error) return sanitizeErrorText(value.message);
  return "unexpected Runtime error";
}

function isAbortError(value: unknown): boolean {
  return value instanceof DOMException && value.name === "AbortError";
}

function isTerminal(status: string): boolean {
  return ["succeeded", "failed", "timed_out", "cancelled"].includes(status);
}

async function wait(milliseconds: number, signal: AbortSignal): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const timer = window.setTimeout(resolve, milliseconds);
    signal.addEventListener("abort", () => {
      window.clearTimeout(timer);
      reject(new DOMException("The operation was aborted", "AbortError"));
    }, { once: true });
  });
}
