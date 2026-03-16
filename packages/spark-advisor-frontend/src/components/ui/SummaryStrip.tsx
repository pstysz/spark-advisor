import type { TaskStatsResponse } from "@/lib/types";

interface SummaryStripProps {
  stats: TaskStatsResponse;
}

export function SummaryStrip({ stats }: SummaryStripProps) {
  const items = [
    { label: "Total", value: stats.total },
    { label: "Completed", value: stats.counts.completed ?? 0, color: "var(--status-completed)" },
    { label: "Running", value: stats.counts.running ?? 0, color: "var(--status-running)" },
    { label: "Failed", value: stats.counts.failed ?? 0, color: "var(--status-failed)" },
    { label: "Pending", value: stats.counts.pending ?? 0, color: "var(--status-pending)" },
  ];

  return (
    <div className="summary-strip">
      {items.map((item, i) => (
        <span key={item.label}>
          {i > 0 && <span className="summary-sep" />}
          <span className="summary-item">
            <span className="summary-value" style={item.color ? { color: item.color } : undefined}>
              {item.value}
            </span>
            <span className="summary-label">{item.label}</span>
          </span>
        </span>
      ))}
    </div>
  );
}
