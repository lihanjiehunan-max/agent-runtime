import type { RuntimeEvent, RuntimeMode, RuntimeSession } from "../types/runtime";

interface DashboardPageProps {
  mode: RuntimeMode;
  sessions: RuntimeSession[];
  events: RuntimeEvent[];
}

export function DashboardPage({ mode, sessions, events }: DashboardPageProps) {
  const completed = events.filter((event) => event.event_type.startsWith("execution."));
  const successes = events.filter((event) => event.event_type === "execution.succeeded").length;
  const durations = events
    .map((event) => event.duration_ms)
    .filter((duration): duration is number => typeof duration === "number")
    .sort((a, b) => a - b);
  const p95 = durations.length === 0 ? 0 : durations[Math.min(durations.length - 1, Math.ceil(durations.length * 0.95) - 1)];
  const inputTokens = sumPayloadNumber(events, "input_tokens");
  const outputTokens = sumPayloadNumber(events, "output_tokens");
  const cacheHits = events.filter((event) => event.payload.cache_hit === true).length;
  const cacheCandidates = events.filter((event) => "cache_hit" in event.payload).length;
  const cacheHitRate = cacheCandidates === 0 ? 0 : Math.round((cacheHits / cacheCandidates) * 100);

  const cards = [
    ["Success rate", completed.length ? `${Math.round((successes / completed.length) * 100)}%` : "—", "terminal events"],
    ["P95 latency", p95 ? `${Math.round(p95)} ms` : "—", "bounded local event sample"],
    ["Tokens", `${inputTokens + outputTokens}`, `${inputTokens} in · ${outputTokens} out`],
    ["Cache hit", cacheCandidates ? `${cacheHitRate}%` : "—", "from event payload"],
    ["Active Sessions", `${sessions.filter((session) => session.status === "open").length}`, "in this console"],
  ] as const;

  return (
    <section className="page-stack" aria-labelledby="dashboard-title">
      <div className="page-heading">
        <div>
          <p className="eyebrow">Runtime pulse</p>
          <h2 id="dashboard-title">Dashboard</h2>
          <p className="muted">Bounded metrics derived from the visible Session event stream.</p>
        </div>
        <span className="subtle-label">{mode} sample</span>
      </div>
      <div className="metric-grid">
        {cards.map(([label, value, detail]) => (
          <div className="metric-card" key={label}>
            <span className="metric-label">{label}</span>
            <strong className="metric-value">{value}</strong>
            <span className="metric-detail">{detail}</span>
          </div>
        ))}
      </div>
      <div className="card dashboard-note">
        <strong>Scope note</strong>
        <p>
          This first console intentionally computes a small, honest client-side snapshot. Prometheus and
          the Runtime status endpoint remain available through the typed client for the integrated deployment.
        </p>
      </div>
    </section>
  );
}

function sumPayloadNumber(events: RuntimeEvent[], key: string): number {
  return events.reduce((total, event) => {
    const value = event.payload[key];
    return total + (typeof value === "number" && Number.isFinite(value) ? value : 0);
  }, 0);
}
