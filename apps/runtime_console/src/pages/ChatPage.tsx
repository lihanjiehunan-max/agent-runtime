import { useState } from "react";

import type {
  ConsoleMessage,
  ExecutionMode,
  RuntimeMode,
  RuntimeSession,
} from "../types/runtime";

interface ChatPageProps {
  mode: RuntimeMode;
  modeReason: string;
  session: RuntimeSession | null;
  messages: ConsoleMessage[];
  busy: boolean;
  error: string | null;
  executionMode: ExecutionMode;
  onExecutionModeChange: (mode: ExecutionMode) => void;
  onSend: (input: string, mode: ExecutionMode) => Promise<void>;
  onCancel: () => Promise<void>;
  onCreateSession: () => void;
}

export function ChatPage({
  mode,
  modeReason,
  session,
  messages,
  busy,
  error,
  executionMode,
  onExecutionModeChange,
  onSend,
  onCancel,
  onCreateSession,
}: ChatPageProps) {
  const [input, setInput] = useState("");

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const value = input.trim();
    if (!value || busy) return;
    setInput("");
    await onSend(value, executionMode);
  }

  return (
    <section className="chat-layout" aria-labelledby="chat-title">
      <div className="chat-header">
        <div>
          <p className="eyebrow">Multi-turn test bench</p>
          <h2 id="chat-title">Chat</h2>
          <p className="muted">
            {session ? `Session ${session.session_id} · thread ${session.thread_id}` : "No Session selected"}
          </p>
        </div>
        <div className="chat-actions">
          <span className={`status-pill ${session ? "status-success" : "status-muted"}`}>
            {session ? session.status : "not started"}
          </span>
          <button className="secondary-button" type="button" onClick={onCreateSession}>
            {session ? "New Session" : "Start Session"}
          </button>
        </div>
      </div>

      <div className="provenance-panel">
        <span className="provenance-label">Output provenance</span>
        <span className={`provenance-value provenance-${mode.toLowerCase()}`}>{mode}</span>
        <span className="provenance-reason">{modeReason}</span>
      </div>

      <div className="chat-transcript" aria-live="polite" data-testid="chat-transcript">
        {messages.length === 0 ? (
          <div className="empty-state chat-empty">
            <strong>Ask a question about the pinned package.</strong>
            <span>The same Session is reused for every turn.</span>
          </div>
        ) : (
          messages.map((message) => (
            <article className={`message message-${message.role}`} key={message.id}>
              <div className="message-meta">
                <span>{message.role === "assistant" ? "Runtime" : message.role === "user" ? "You" : "Console"}</span>
                <span className={`message-mode message-mode-${message.mode.toLowerCase()}`}>{message.mode}</span>
              </div>
              <p>{message.text}</p>
            </article>
          ))
        )}
        {busy ? <div className="typing-indicator">Runtime is processing the turn…</div> : null}
      </div>

      {error ? <div className="error-banner" role="alert">{error}</div> : null}

      <form className="composer" onSubmit={submit}>
        <label className="sr-only" htmlFor="chat-input">Message</label>
        <textarea
          id="chat-input"
          value={input}
          onChange={(event) => setInput(event.target.value)}
          placeholder="Try: 本月散运营业收入是多少？"
          rows={3}
          disabled={busy}
        />
        <div className="composer-footer">
          <label className="mode-select-label" htmlFor="execution-mode">
            Execution
            <select
              id="execution-mode"
              value={executionMode}
              onChange={(event) => onExecutionModeChange(event.target.value as ExecutionMode)}
              disabled={busy}
            >
              <option value="stream">Stream + SSE</option>
              <option value="sync">Sync response</option>
              <option value="async">Async task</option>
            </select>
          </label>
          <div className="composer-buttons">
            {busy ? (
              <div className="cancel-control">
                <button className="danger-button" type="button" onClick={() => void onCancel()}>
                  {executionMode === "async" ? "Cancel Runtime task" : "Stop browser wait"}
                </button>
                <span className="muted cancellation-note">
                  {executionMode === "async"
                    ? "Requests server-side task cancellation when the task is known."
                    : "Browser request only; Runtime execution may continue."}
                </span>
              </div>
            ) : null}
            <button className="primary-button" type="submit" disabled={!input.trim() || busy}>
              Send message
            </button>
          </div>
        </div>
      </form>
    </section>
  );
}
