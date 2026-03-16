import type { RuleFrequencyEntry } from "@/lib/types";

interface RuleFrequencyChartProps {
  data: RuleFrequencyEntry[];
}

export function RuleFrequencyChart({ data }: RuleFrequencyChartProps) {
  const maxCount = Math.max(...data.map((d) => d.count), 1);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
      {data.map((item) => (
        <div key={item.rule_id} className="bar-chart-item">
          <span className="bar-label" title={item.title}>{item.title}</span>
          <div className="bar-track">
            <div
              className={`bar-fill ${item.severity.toLowerCase()}`}
              style={{ width: `${(item.count / maxCount) * 100}%` }}
            />
          </div>
          <span className="bar-value">{item.count}</span>
        </div>
      ))}
    </div>
  );
}
