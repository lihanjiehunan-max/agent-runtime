import type { RuntimeEvent, RuntimeMode } from "../types/runtime";

interface TracePageProps {
  mode: RuntimeMode;
  events: RuntimeEvent[];
  sessionId: string | null;
}

export function TracePage({ mode, events, sessionId }: TracePageProps) {
  return (
    <section className="page-stack" aria-labelledby="trace-title">
      <div className="page-heading">
        <div>
          <p className="eyebrow">Operational evidence</p>
          <h2 id="trace-title">Trace</h2>
          <p className="muted">Ordered events for {sessionId ?? "the selected Session"}.</p>
        </div>
        <span className="subtle-label">{mode} timeline</span>
      </div>
      {events.length === 0 ? (
        <div className="empty-state">
          <strong>No events yet</strong>
          <span>Run a turn in Chat to populate package, model, tool, checkpoint, and result evidence.</span>
        </div>
      ) : (
        <ol className="timeline">
          {[...events].sort((a, b) => a.sequence - b.sequence).map((event) => (
            <li className="timeline-item" key={`${event.event_id}-${event.sequence}`}>
              <span className="timeline-marker">{event.sequence}</span>
              <div className="timeline-content">
                <div className="timeline-header">
                  <strong>{event.event_type}</strong>
                  <span className="status-pill status-muted">{event.phase}</span>
                </div>
                <div className="timeline-meta">
                  <span>trace {event.trace_id}</span>
                  <span>execution {event.execution_id}</span>
                  {event.duration_ms !== null ? <span>{event.duration_ms} ms</span> : null}
                </div>
                <pre className="event-payload">{JSON.stringify(event.payload, null, 2)}</pre>
              </div>
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}
