import type { TopAppEntry } from "@/lib/types";

interface TopAppsChartProps {
  data: TopAppEntry[];
}

export function TopAppsChart({ data }: TopAppsChartProps) {
  const maxCount = Math.max(...data.map((d) => d.analysis_count), 1);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
      {data.map((item) => (
        <div key={item.app_id} className="bar-chart-item">
          <span className="bar-label font-mono" title={item.app_id}>
            {item.app_id.length > 24 ? item.app_id.slice(0, 24) + "..." : item.app_id}
          </span>
          <div className="bar-track">
            <div
              className="bar-fill"
              style={{ width: `${(item.analysis_count / maxCount) * 100}%`, background: "var(--accent-blue)" }}
            />
          </div>
          <span className="bar-value">{item.analysis_count}</span>
        </div>
      ))}
    </div>
  );
}
