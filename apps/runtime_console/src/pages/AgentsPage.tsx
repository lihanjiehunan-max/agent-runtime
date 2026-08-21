import type { AgentPackageRef, RuntimeMode, RuntimeSession } from "../types/runtime";

interface AgentsPageProps {
  mode: RuntimeMode;
  packageRef: AgentPackageRef | null;
  session: RuntimeSession | null;
  onCreateSession: () => void;
}

export function AgentsPage({ mode, packageRef, session, onCreateSession }: AgentsPageProps) {
  const agent = packageRef ?? session?.package;
  return (
    <section className="page-stack" aria-labelledby="agents-title">
      <div className="page-heading">
        <div>
          <p className="eyebrow">Package registry</p>
          <h2 id="agents-title">Agents</h2>
          <p className="muted">The console shows the package pinned to the current Session.</p>
        </div>
        <button className="primary-button" type="button" onClick={onCreateSession}>
          Load package
        </button>
      </div>
      <div className="card package-card">
        <div className="card-kicker">{mode === "LIVE" ? "LIVE package" : "DEMO fixture"}</div>
        <h3>{agent?.agent_id ?? "agent-metric-query"}</h3>
        <dl className="detail-grid">
          <div>
            <dt>Version</dt>
            <dd>{agent?.version ?? "0.1.0"}</dd>
          </div>
          <div>
            <dt>Digest</dt>
            <dd className="mono digest">{agent?.digest ?? "not loaded"}</dd>
          </div>
          <div>
            <dt>Runtime</dt>
            <dd>{agent?.runtime_type ?? "deepagents"}</dd>
          </div>
          <div>
            <dt>Load state</dt>
            <dd>
              <span className="status-pill status-success">{session ? "loaded / pinned" : "available"}</span>
            </dd>
          </div>
        </dl>
      </div>
      <div className="notice">
        Agent listing is intentionally small in v1 because the Runtime API exposes package identity on
        Session creation rather than a package catalog endpoint.
      </div>
    </section>
  );
}
