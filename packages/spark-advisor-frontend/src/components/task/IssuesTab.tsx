import { SeverityBadge } from "@/components/ui/Badge";
import type { RuleViolationResponse } from "@/lib/types";

interface IssuesTabProps {
  rules: RuleViolationResponse[];
}

export function IssuesTab({ rules }: IssuesTabProps) {
  if (rules.length === 0) {
    return <div className="empty-state">No issues found</div>;
  }

  return (
    <div className="table-wrapper">
      <table>
        <thead>
          <tr>
            <th>Severity</th>
            <th>Rule</th>
            <th>Message</th>
            <th>Stage</th>
            <th>Current</th>
            <th>Recommended</th>
            <th>Impact</th>
          </tr>
        </thead>
        <tbody>
          {rules.map((rule, i) => (
            <tr key={`${rule.rule_id}-${rule.stage_id ?? i}`}>
              <td><SeverityBadge severity={rule.severity} /></td>
              <td className="font-mono" style={{ fontSize: 12 }}>{rule.title}</td>
              <td className="td-muted" style={{ maxWidth: 300 }}>{rule.message}</td>
              <td className="td-muted">{rule.stage_id !== null ? `Stage ${rule.stage_id}` : "—"}</td>
              <td className="font-mono td-muted" style={{ fontSize: 12 }}>{rule.current_value || "—"}</td>
              <td className="font-mono config-recommended" style={{ fontSize: 12 }}>{rule.recommended_value || "—"}</td>
              <td className="td-muted" style={{ fontSize: 12 }}>{rule.estimated_impact || "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
