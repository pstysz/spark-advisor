import { useState } from "react";
import { PageHeader } from "@/components/layout/PageHeader";
import { VolumeChart } from "@/components/charts/VolumeChart";
import { RuleFrequencyChart } from "@/components/charts/RuleFrequencyChart";
import { TopAppsChart } from "@/components/charts/TopAppsChart";
import {
  useStatsSummary,
  useRuleFrequency,
  useDailyVolume,
  useTopIssues,
  useTopApps,
  useModeBreakdown,
  useDurationByMode,
  useSourceBreakdown,
  useSeverityTrend,
  useFailureRate,
} from "@/hooks/useStats";
import { SeverityBadge } from "@/components/ui/Badge";
import { formatSeconds } from "@/lib/format";
import { SeverityTrendChart } from "@/components/charts/SeverityTrendChart";
import { FailureRateChart } from "@/components/charts/FailureRateChart";

const dayOptions = [7, 14, 30, 90];

const MODE_COLORS: Record<string, string> = {
  ai: "#a855f7",
  static: "#3b82f6",
  agent: "#f97316",
};

const SOURCE_COLORS: Record<string, string> = {
  hs_manual: "#3b82f6",
  hs_poller: "#22c55e",
  file: "#f59e0b",
  k8s: "#a855f7",
};

const SOURCE_LABELS: Record<string, string> = {
  hs_manual: "HS Manual",
  hs_poller: "HS Poller",
  file: "File",
  k8s: "K8s",
};

export function StatsPage() {
  const [days, setDays] = useState(30);

  const { data: summary } = useStatsSummary(days);
  const { data: ruleFreq } = useRuleFrequency(days);
  const { data: volume } = useDailyVolume(days);
  const { data: topIssues } = useTopIssues(days);
  const { data: topApps } = useTopApps(days);
  const { data: modeBreakdown } = useModeBreakdown(days);
  const { data: durationByMode } = useDurationByMode(days);
  const { data: sourceBreakdown } = useSourceBreakdown(days);
  const { data: severityTrend } = useSeverityTrend(days);
  const { data: failureRate } = useFailureRate(days);

  return (
    <>
      <PageHeader
        title="Statistics"
        breadcrumbs={[{ label: `Last ${days} days` }]}
        actions={
          <div style={{ display: "flex", gap: 4 }}>
            {dayOptions.map((d) => (
              <button
                key={d}
                className={`btn-page ${days === d ? "active" : ""}`}
                onClick={() => setDays(d)}
              >
                {d}d
              </button>
            ))}
          </div>
        }
      />
      <div className="page-body">
        {/* 6 KPI Cards */}
        {summary && (
          <div className="stat-grid">
            <KpiCard label="Total Analyses" value={summary.total} iconColor="blue" iconPath="M3 3v16a2 2 0 0 0 2 2h16M19 9l-5 5-4-4-3 3" />
            <KpiCard
              label="Completed"
              value={summary.completed}
              iconColor="green"
              iconPath="M20 6 9 17l-5-5"
              subtext={summary.total > 0 ? `${((summary.completed / summary.total) * 100).toFixed(1)}% success rate` : undefined}
              subtextColor="var(--status-completed)"
            />
            <KpiCard label="Failed" value={summary.failed} iconColor="red" iconPath="M12 2a10 10 0 1 0 0 20 10 10 0 0 0 0-20M15 9l-6 6M9 9l6 6" />
            <KpiCard
              label="Avg Duration"
              value={summary.avg_duration_seconds !== null ? formatSeconds(summary.avg_duration_seconds) : "—"}
              iconColor="purple"
              iconPath="M12 2a10 10 0 1 0 0 20 10 10 0 0 0 0-20M12 6v6l4 2"
            />
            <KpiCard
              label="AI Usage"
              value={summary.ai_usage_percent !== null ? `${summary.ai_usage_percent.toFixed(0)}%` : "—"}
              iconColor="purple"
              iconPath="M12 2a4 4 0 0 0-4 4v2H6a2 2 0 0 0-2 2v10a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V10a2 2 0 0 0-2-2h-2V6a4 4 0 0 0-4-4zM12 13a2 2 0 1 0 0 4 2 2 0 0 0 0-4z"
            />
            <KpiCard
              label="Avg Issues"
              value={summary.avg_issues_per_analysis !== null ? summary.avg_issues_per_analysis.toFixed(1) : "—"}
              iconColor="amber"
              iconPath="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0zM12 9v4M12 17h.01"
              subtext="per analysis"
            />
          </div>
        )}

        {/* Daily Volume - full width */}
        <div className="card">
          <div className="card-header">
            <span className="card-title">Daily Analyses</span>
            <span className="card-title" style={{ fontSize: 12 }}>Last {days} days</span>
          </div>
          <div className="card-body">
            {volume && volume.items.length > 0 ? <VolumeChart data={volume.items} /> : <div className="empty-state">No data</div>}
          </div>
        </div>

        {/* Rule Frequency + Top Issues */}
        <div className="grid-2">
          <div className="card">
            <div className="card-header">
              <span className="card-title">Rule Frequency</span>
              <span className="card-title" style={{ fontSize: 12 }}>All analyses</span>
            </div>
            <div className="card-body" style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              {ruleFreq && ruleFreq.items.length > 0 ? (
                <RuleFrequencyChart data={ruleFreq.items} />
              ) : (
                <div className="empty-state">No data</div>
              )}
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <span className="card-title">Top Issues</span>
              <span className="card-title" style={{ fontSize: 12 }}>By frequency</span>
            </div>
            <div className="card-body" style={{ padding: "12px 20px" }}>
              {topIssues && topIssues.items.length > 0 ? (
                topIssues.items.map((issue, i) => (
                  <div key={issue.rule_id} className="issue-item">
                    <span className="issue-rank">{i + 1}</span>
                    <div className="issue-content">
                      <div className="issue-title">
                        <SeverityBadge severity={issue.severity} /> {issue.title}
                      </div>
                      <div className="issue-meta">{issue.message}</div>
                    </div>
                    <span className="issue-count">{issue.count}</span>
                  </div>
                ))
              ) : (
                <div className="empty-state">No data</div>
              )}
            </div>
          </div>
        </div>

        {/* Mode Breakdown + Duration by Mode */}
        <div className="grid-2">
          <div className="card">
            <div className="card-header">
              <span className="card-title">Mode Breakdown</span>
              <span className="card-title" style={{ fontSize: 12 }}>Last {days} days</span>
            </div>
            <div className="card-body" style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              {modeBreakdown && modeBreakdown.items.length > 0 ? (
                (() => {
                  const max = Math.max(...modeBreakdown.items.map((m) => m.count));
                  return modeBreakdown.items.map((m) => (
                    <div key={m.mode} className="bar-chart-item">
                      <span className="bar-label">{m.mode.charAt(0).toUpperCase() + m.mode.slice(1)}</span>
                      <div className="bar-track">
                        <div className="bar-fill" style={{ width: `${(m.count / max) * 100}%`, background: MODE_COLORS[m.mode] || "#666" }} />
                      </div>
                      <span className="bar-value">{m.count}</span>
                    </div>
                  ));
                })()
              ) : (
                <div className="empty-state">No data</div>
              )}
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <span className="card-title">Avg Duration by Mode</span>
              <span className="card-title" style={{ fontSize: 12 }}>Completed analyses</span>
            </div>
            <div className="card-body" style={{ display: "flex", flexDirection: "column", gap: 12, padding: 20 }}>
              {durationByMode && durationByMode.items.length > 0 ? (
                (() => {
                  const max = Math.max(...durationByMode.items.map((m) => m.avg_duration_seconds));
                  return durationByMode.items.map((m) => (
                    <div key={m.mode} style={{ display: "flex", alignItems: "center", gap: 12 }}>
                      <span className="badge" style={{ background: `${MODE_COLORS[m.mode]}26`, color: MODE_COLORS[m.mode], minWidth: 56, textAlign: "center" }}>
                        {m.mode.charAt(0).toUpperCase() + m.mode.slice(1)}
                      </span>
                      <div className="bar-track" style={{ flex: 1 }}>
                        <div className="bar-fill" style={{ width: `${(m.avg_duration_seconds / max) * 100}%`, background: MODE_COLORS[m.mode] }} />
                      </div>
                      <span style={{ fontFamily: "'JetBrains Mono'", fontSize: 13, color: "var(--text-secondary)", minWidth: 60, textAlign: "right" }}>
                        {formatSeconds(m.avg_duration_seconds)}
                      </span>
                    </div>
                  ));
                })()
              ) : (
                <div className="empty-state">No data</div>
              )}
            </div>
          </div>
        </div>

        {/* Most Analyzed Apps + Source Breakdown */}
        <div className="grid-2">
          <div className="card">
            <div className="card-header">
              <span className="card-title">Most Analyzed Apps</span>
              <span className="card-title" style={{ fontSize: 12 }}>Top 5</span>
            </div>
            <div className="card-body" style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              {topApps && topApps.items.length > 0 ? (
                <TopAppsChart data={topApps.items} />
              ) : (
                <div className="empty-state">No data</div>
              )}
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <span className="card-title">Source Breakdown</span>
              <span className="card-title" style={{ fontSize: 12 }}>Last {days} days</span>
            </div>
            <div className="card-body" style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              {sourceBreakdown && sourceBreakdown.items.length > 0 ? (
                (() => {
                  const max = Math.max(...sourceBreakdown.items.map((s) => s.count));
                  return sourceBreakdown.items.map((s) => (
                    <div key={s.data_source} className="bar-chart-item">
                      <span className="bar-label">{SOURCE_LABELS[s.data_source] || s.data_source}</span>
                      <div className="bar-track">
                        <div className="bar-fill" style={{ width: `${(s.count / max) * 100}%`, background: SOURCE_COLORS[s.data_source] || "#666" }} />
                      </div>
                      <span className="bar-value">{s.count}</span>
                    </div>
                  ));
                })()
              ) : (
                <div className="empty-state">No data</div>
              )}
            </div>
          </div>
        </div>

        {/* Severity Trend + Failure Rate */}
        <div className="grid-2">
          <div className="card">
            <div className="card-header">
              <span className="card-title">Severity Trend</span>
              <span className="card-title" style={{ fontSize: 12 }}>Last {days} days</span>
            </div>
            <div className="card-body" style={{ padding: "16px 20px 12px" }}>
              <div style={{ display: "flex", gap: 16, marginBottom: 12, fontSize: 12 }}>
                <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
                  <span style={{ width: 10, height: 10, borderRadius: 2, background: "#ef4444" }} /> Critical
                </span>
                <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
                  <span style={{ width: 10, height: 10, borderRadius: 2, background: "#f59e0b" }} /> Warning
                </span>
                <span style={{ display: "flex", alignItems: "center", gap: 4 }}>
                  <span style={{ width: 10, height: 10, borderRadius: 2, background: "#3b82f6" }} /> Info
                </span>
              </div>
              {severityTrend && severityTrend.items.length > 0 ? (
                <SeverityTrendChart data={severityTrend.items} />
              ) : (
                <div className="empty-state">No data</div>
              )}
            </div>
          </div>

          <div className="card">
            <div className="card-header">
              <span className="card-title">Failure Rate</span>
              <span className="card-title" style={{ fontSize: 12 }}>Last {days} days</span>
            </div>
            <div className="card-body" style={{ padding: "16px 20px 12px" }}>
              {failureRate && failureRate.items.length > 0 ? (
                <FailureRateChart data={failureRate.items} />
              ) : (
                <div className="empty-state">No data</div>
              )}
            </div>
          </div>
        </div>
      </div>
    </>
  );
}

function KpiCard({
  label,
  value,
  iconColor,
  iconPath,
  subtext,
  subtextColor,
}: {
  label: string;
  value: string | number;
  iconColor: string;
  iconPath: string;
  subtext?: string;
  subtextColor?: string;
}) {
  const colorMap: Record<string, { bg: string; fg: string }> = {
    blue: { bg: "var(--accent-blue-dim)", fg: "var(--accent-blue)" },
    green: { bg: "var(--status-completed-dim)", fg: "var(--status-completed)" },
    red: { bg: "var(--status-failed-dim)", fg: "var(--status-failed)" },
    purple: { bg: "rgba(168, 85, 247, 0.15)", fg: "#a855f7" },
    amber: { bg: "rgba(245, 158, 11, 0.15)", fg: "#f59e0b" },
  };
  const colors = colorMap[iconColor] ?? { bg: "var(--accent-blue-dim)", fg: "var(--accent-blue)" };

  return (
    <div className="stat-card">
      <div className="stat-card-header">
        <span className="stat-label">{label}</span>
        <div className="stat-icon" style={{ background: colors.bg, color: colors.fg }}>
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round">
            {iconPath.split("M").filter(Boolean).map((seg, i) => (
              <path key={i} d={`M${seg}`} />
            ))}
          </svg>
        </div>
      </div>
      <div className="stat-value">{value}</div>
      {subtext && <div className="stat-change" style={subtextColor ? { color: subtextColor } : undefined}>{subtext}</div>}
    </div>
  );
}
