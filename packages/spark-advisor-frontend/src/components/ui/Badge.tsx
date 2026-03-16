import type { TaskStatus, Severity, AnalysisMode, DataSource } from "@/lib/types";

interface StatusBadgeProps {
  status: TaskStatus;
}

const statusLabels: Record<TaskStatus, string> = {
  completed: "Completed",
  running: "Running",
  pending: "Pending",
  failed: "Failed",
};

export function StatusBadge({ status }: StatusBadgeProps) {
  return (
    <span className={`badge badge-${status}`}>
      <span className="badge-dot" />
      {statusLabels[status]}
    </span>
  );
}

interface SeverityBadgeProps {
  severity: Severity;
}

export function SeverityBadge({ severity }: SeverityBadgeProps) {
  return <span className={`badge badge-${severity.toLowerCase()}`}>{severity}</span>;
}

interface ModeBadgeProps {
  mode: AnalysisMode;
}

export function ModeBadge({ mode }: ModeBadgeProps) {
  return <span className="badge badge-mode">{mode}</span>;
}

interface SourceBadgeProps {
  source: DataSource;
}

const sourceLabels: Record<DataSource, string> = {
  hs_manual: "HS Manual",
  hs_poller: "HS Poller",
  file: "File",
  k8s: "K8s",
};

export function SourceBadge({ source }: SourceBadgeProps) {
  return <span className="badge badge-source">{sourceLabels[source]}</span>;
}
