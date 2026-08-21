import type { RuntimeMode, RuntimeSession } from "../types/runtime";

interface SessionsPageProps {
  mode: RuntimeMode;
  sessions: RuntimeSession[];
  selectedSessionId: string | null;
  onSelect: (sessionId: string) => void;
  onCreate: () => void;
}

export function SessionsPage({
  mode,
  sessions,
  selectedSessionId,
  onSelect,
  onCreate,
}: SessionsPageProps) {
  return (
    <section className="page-stack" aria-labelledby="sessions-title">
      <div className="page-heading">
        <div>
          <p className="eyebrow">Conversation state</p>
          <h2 id="sessions-title">Sessions</h2>
          <p className="muted">Each chat turn stays on one pinned LangGraph thread.</p>
        </div>
        <button className="primary-button" type="button" onClick={onCreate}>
          New Session
        </button>
      </div>
      {sessions.length === 0 ? (
        <div className="empty-state">
          <strong>No Session selected</strong>
          <span>Create one to start a multi-turn conversation.</span>
        </div>
      ) : (
        <div className="session-list">
          {sessions.map((session) => (
            <button
              className={`card session-card ${selectedSessionId === session.session_id ? "selected" : ""}`}
              key={session.session_id}
              type="button"
              onClick={() => onSelect(session.session_id)}
            >
              <div className="session-card-top">
                <span className="mono">{session.session_id}</span>
                <span className={`status-pill ${session.status === "open" ? "status-success" : "status-muted"}`}>
                  {session.status}
                </span>
              </div>
              <dl className="detail-grid compact">
                <div>
                  <dt>Package pin</dt>
                  <dd>{session.package.agent_id}@{session.package.version}</dd>
                </div>
                <div>
                  <dt>TTL</dt>
                  <dd>Runtime-managed</dd>
                </div>
                <div>
                  <dt>Thread</dt>
                  <dd className="mono">{session.thread_id}</dd>
                </div>
                <div>
                  <dt>Last event</dt>
                  <dd>{session.last_event_sequence}</dd>
                </div>
              </dl>
              <span className="session-mode">{mode} · {new Date(session.updated_at).toLocaleTimeString()}</span>
            </button>
          ))}
        </div>
      )}
      <div className="notice">
        TTL is not part of the current Session v1 response, so the console does not invent a duration;
        it shows the Runtime-managed state instead.
      </div>
    </section>
  );
}
