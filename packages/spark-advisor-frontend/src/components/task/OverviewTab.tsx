import { StatusBadge, ModeBadge, SourceBadge } from "@/components/ui/Badge";
import { formatDuration, formatDateTime } from "@/lib/format";
import type { TaskResponse, Severity } from "@/lib/types";

interface OverviewTabProps {
  task: TaskResponse;
}

export function OverviewTab({ task }: OverviewTabProps) {
  return (
    <div className="card-body" style={{ display: "flex", flexDirection: "column", gap: 24 }}>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 16 }}>
        <MetricCard label="Status" value={<StatusBadge status={task.status} />} />
        <MetricCard label="Duration" value={formatDuration(task.started_at, task.completed_at)} />
        <MetricCard
          label="Issues Found"
          value={
            task.severity_counts
              ? String(Object.values(task.severity_counts).reduce((a, b) => a + b, 0))
              : "0"
          }
        />
        <MetricCard label="Mode" value={<ModeBadge mode={task.mode} />} />
      </div>

      <div>
        <div className="section-title">Task Information</div>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(2, 1fr)", gap: 12, fontSize: 14 }}>
          <InfoRow label="Task ID" value={<span className="font-mono">{task.task_id}</span>} />
          <InfoRow label="App ID" value={<span className="font-mono">{task.app_id}</span>} />
          <InfoRow label="Source" value={<SourceBadge source={task.data_source} />} />
          <InfoRow label="Created" value={formatDateTime(task.created_at)} />
          {task.started_at && <InfoRow label="Started" value={formatDateTime(task.started_at)} />}
          {task.completed_at && <InfoRow label="Completed" value={formatDateTime(task.completed_at)} />}
        </div>
      </div>

      {task.error && (
        <div style={{
          padding: "12px 16px",
          background: "var(--severity-critical-dim, rgba(239, 68, 68, 0.1))",
          borderRadius: 8,
          border: "1px solid var(--severity-critical, #ef4444)",
          display: "flex",
          gap: 10,
          alignItems: "flex-start",
        }}>
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="var(--severity-critical, #ef4444)" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round" style={{ flexShrink: 0, marginTop: 1 }}>
            <circle cx="12" cy="12" r="10" />
            <line x1="12" y1="8" x2="12" y2="12" />
            <line x1="12" y1="16" x2="12.01" y2="16" />
          </svg>
          <span style={{ color: "var(--severity-critical, #ef4444)", fontSize: 13, lineHeight: 1.5 }}>{task.error}</span>
        </div>
      )}

      {task.severity_counts && (
        <div>
          <div className="section-title">Severity Breakdown</div>
          <div style={{ display: "flex", gap: 16 }}>
            {(["CRITICAL", "WARNING", "INFO"] as Severity[]).map((sev) => {
              const count = task.severity_counts?.[sev] ?? 0;
              if (count === 0) return null;
              return (
                <span key={sev} className={`badge badge-${sev.toLowerCase()}`}>
                  {count} {sev}
                </span>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

function MetricCard({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="stat-card">
      <div className="stat-label">{label}</div>
      <div style={{ fontSize: 20, fontWeight: 700, letterSpacing: "-0.02em" }}>{value}</div>
    </div>
  );
}

function InfoRow({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div style={{ display: "flex", gap: 8 }}>
      <span style={{ color: "var(--text-muted)", minWidth: 100 }}>{label}:</span>
      <span>{value}</span>
    </div>
  );
}
