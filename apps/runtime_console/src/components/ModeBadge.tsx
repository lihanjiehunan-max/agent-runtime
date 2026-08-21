import type { RuntimeMode } from "../types/runtime";

interface ModeBadgeProps {
  mode: RuntimeMode;
  reason?: string;
}

export function ModeBadge({ mode, reason }: ModeBadgeProps) {
  return (
    <span className={`mode-badge mode-${mode.toLowerCase()}`} data-testid="mode-badge">
      <span aria-hidden="true" className="mode-dot" />
      <span>{mode}</span>
      {reason ? <span className="mode-reason">{reason}</span> : null}
    </span>
  );
}
