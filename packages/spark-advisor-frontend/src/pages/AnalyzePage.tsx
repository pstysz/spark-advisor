import { useState, useRef, useEffect } from "react";
import { useNavigate } from "react-router";
import { PageHeader } from "@/components/layout/PageHeader";
import { useSubmitAnalysis } from "@/hooks/useAnalyze";
import { useApplicationsList } from "@/hooks/useApplications";
import type { AnalysisMode } from "@/lib/types";

const modes: { id: AnalysisMode; title: string; description: string }[] = [
  { id: "static", title: "Static", description: "Rules engine only. Fast, no API key needed. Checks 11 rules for common Spark issues." },
  { id: "ai", title: "AI", description: "Rules + Claude AI analysis. Provides deeper insights, causal chains, and prioritized recommendations." },
  { id: "agent", title: "Agent", description: "Multi-turn Claude agent with tools. Most thorough — iteratively analyzes stages, configs, and metrics." },
];

export function AnalyzePage() {
  const navigate = useNavigate();
  const [appId, setAppId] = useState("");
  const [mode, setMode] = useState<AnalysisMode>("ai");
  const [dropdownOpen, setDropdownOpen] = useState(false);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const { mutate, isPending, error, data } = useSubmitAnalysis();
  const { data: apps, isError: appsError, isLoading: appsLoading } = useApplicationsList({ limit: 500, offset: 0 });

  const filtered = apps?.items.filter((a) =>
    a.id.toLowerCase().includes(appId.toLowerCase()) ||
    a.name.toLowerCase().includes(appId.toLowerCase()),
  ) ?? [];

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (wrapperRef.current && !wrapperRef.current.contains(e.target as Node)) {
        setDropdownOpen(false);
      }
    }
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!appId.trim()) return;
    mutate(
      { app_id: appId.trim(), mode },
      {
        onSuccess: (result) => {
          if (result.status === 202) {
            // don't navigate - show success state
          }
        },
      },
    );
  };

  const isConflict = error && "status" in error && (error as { status: number }).status === 409;
  const submitted = data && data.status === 202;

  return (
    <>
      <PageHeader title="New Analysis" breadcrumbs={[{ label: "Analyze" }]} />
      <div className="page-body">
        <div className="analyze-container">
          <div className="analyze-card">
            <h3>Analyze Spark Application</h3>
            <p className="analyze-subtitle">Submit a Spark application for performance analysis and optimization recommendations.</p>

            <form onSubmit={handleSubmit}>
              <div className="form-group" style={{ marginBottom: 20 }} ref={wrapperRef}>
                <label className="form-label">Application ID</label>
                <div className="combobox-wrapper">
                  <input
                    className="form-input"
                    type="text"
                    placeholder="Type or select from History Server..."
                    value={appId}
                    onChange={(e) => {
                      setAppId(e.target.value);
                      setDropdownOpen(true);
                    }}
                    onFocus={() => setDropdownOpen(true)}
                    autoComplete="off"
                    required
                  />
                  <button
                    type="button"
                    className="combobox-toggle"
                    onClick={() => setDropdownOpen(!dropdownOpen)}
                    tabIndex={-1}
                  >
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}>
                      <path d="m6 9 6 6 6-6" />
                    </svg>
                  </button>
                  {dropdownOpen && (
                    <div className="combobox-dropdown">
                      {appsLoading && (
                        <div className="combobox-empty">Loading applications...</div>
                      )}
                      {appsError && (
                        <div className="combobox-empty">History Server unavailable — type ID manually</div>
                      )}
                      {!appsLoading && !appsError && filtered.length === 0 && (
                        <div className="combobox-empty">No matching applications</div>
                      )}
                      {filtered.slice(0, 20).map((app) => (
                        <div
                          key={app.id}
                          className="combobox-option"
                          onClick={() => {
                            setAppId(app.id);
                            setDropdownOpen(false);
                          }}
                        >
                          <span className="combobox-option-id">{app.id}</span>
                          <span className="combobox-option-name">{app.name}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
                <span className="form-hint">Type to filter or select from available applications</span>
              </div>

              <div className="form-group" style={{ marginBottom: 20 }}>
                <label className="form-label">Analysis Mode</label>
                <div className="mode-cards">
                  {modes.map((m) => (
                    <div
                      key={m.id}
                      className={`mode-card ${mode === m.id ? "selected" : ""}`}
                      onClick={() => setMode(m.id)}
                    >
                      <div className="mode-radio" />
                      <div className="mode-info">
                        <div className="mode-name">{m.title}</div>
                        <div className="mode-desc">{m.description}</div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              {isConflict && (
                <div style={{ padding: 12, background: "var(--severity-warning-dim)", borderRadius: 8, color: "var(--severity-warning)", fontSize: 13, marginBottom: 16 }}>
                  An analysis for this application is already running.
                </div>
              )}

              {error && !isConflict && (
                <div style={{ padding: 12, background: "var(--severity-critical-dim, rgba(239, 68, 68, 0.1))", borderRadius: 8, color: "var(--severity-critical, #ef4444)", fontSize: 13, marginBottom: 16 }}>
                  {error instanceof Error ? error.message : "Failed to submit analysis. Please try again."}
                </div>
              )}

              <button type="submit" className="submit-btn" disabled={isPending || !appId.trim()}>
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round">
                  <polygon points="6 3 20 12 6 21 6 3" />
                </svg>
                {isPending ? "Submitting..." : "Start Analysis"}
              </button>
            </form>
          </div>

          {submitted && (
            <div className="success-card">
              <div className="success-icon">
                <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round">
                  <path d="M20 6 9 17l-5-5" />
                </svg>
              </div>
              <h3>Analysis Submitted</h3>
              <p style={{ color: "var(--text-secondary)", fontSize: 14 }}>Your analysis is now queued and will begin shortly.</p>
              <div className="success-task-id">{data.data.task_id}</div>
              <div>
                <a
                  href={`/tasks/${data.data.task_id}`}
                  className="success-view-link"
                  onClick={(e) => {
                    e.preventDefault();
                    navigate(`/tasks/${data.data.task_id}`);
                  }}
                >
                  View task details
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round">
                    <path d="m9 18 6-6-6-6" />
                  </svg>
                </a>
              </div>
            </div>
          )}
        </div>
      </div>
    </>
  );
}
