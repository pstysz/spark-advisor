import type { RuleViolationResponse } from "@/lib/types";
import { SeverityBadge } from "@/components/ui/Badge";

interface StagesTabProps {
  rules: RuleViolationResponse[];
}

interface StageGroup {
  stageId: number;
  issues: RuleViolationResponse[];
}

export function StagesTab({ rules }: StagesTabProps) {
  const stageMap = new Map<number, RuleViolationResponse[]>();
  for (const rule of rules) {
    if (rule.stage_id !== null) {
      const existing = stageMap.get(rule.stage_id) ?? [];
      existing.push(rule);
      stageMap.set(rule.stage_id, existing);
    }
  }

  const stages: StageGroup[] = Array.from(stageMap.entries())
    .sort(([a], [b]) => a - b)
    .map(([stageId, issues]) => ({ stageId, issues }));

  if (stages.length === 0) {
    return <div className="empty-state">No stage-specific issues found</div>;
  }

  return (
    <div className="card-body" style={{ display: "flex", flexDirection: "column", gap: 20 }}>
      {stages.map((stage) => (
        <div key={stage.stageId} style={{ borderBottom: "1px solid var(--border)", paddingBottom: 16 }}>
          <div className="section-title">Stage {stage.stageId}</div>
          {stage.issues.map((issue, i) => (
            <div key={i} className="issue-item">
              <SeverityBadge severity={issue.severity} />
              <div className="issue-content">
                <div className="issue-title">{issue.title}</div>
                <div className="issue-meta">{issue.message}</div>
              </div>
            </div>
          ))}
        </div>
      ))}
    </div>
  );
}
