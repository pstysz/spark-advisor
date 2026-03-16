import type { ConfigComparisonEntry } from "@/lib/types";

interface ConfigTabProps {
  entries: ConfigComparisonEntry[];
}

export function ConfigTab({ entries }: ConfigTabProps) {
  if (entries.length === 0) {
    return <div className="empty-state">No configuration recommendations</div>;
  }

  return (
    <div className="table-wrapper">
      <table>
        <thead>
          <tr>
            <th>Parameter</th>
            <th>Current</th>
            <th>Recommended</th>
            <th>Source</th>
          </tr>
        </thead>
        <tbody>
          {entries.map((entry) => (
            <tr key={entry.parameter}>
              <td className="font-mono" style={{ fontSize: 13 }}>{entry.parameter}</td>
              <td className="font-mono config-current" style={{ fontSize: 13 }}>
                {entry.current_value || "—"}
              </td>
              <td className="font-mono config-recommended" style={{ fontSize: 13 }}>
                {entry.recommended_value}
              </td>
              <td>
                <span className={`badge ${entry.source === "rule" ? "badge-warning" : "badge-info"}`}>
                  {entry.source}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
