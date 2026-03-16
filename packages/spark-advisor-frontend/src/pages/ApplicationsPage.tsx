import { useState, useMemo } from "react";
import { Link } from "react-router";
import { PageHeader } from "@/components/layout/PageHeader";
import { Pagination } from "@/components/ui/Pagination";
import { useApplicationsList } from "@/hooks/useApplications";
import { formatDurationMs, formatDateTime } from "@/lib/format";

const PAGE_SIZE = 20;

export function ApplicationsPage() {
  const [offset, setOffset] = useState(0);
  const [search, setSearch] = useState("");

  const { data, isLoading } = useApplicationsList({ limit: PAGE_SIZE, offset });

  const filtered = useMemo(() => {
    if (!data || !search) return data?.items ?? [];
    const q = search.toLowerCase();
    return data.items.filter(
      (app) => app.id.toLowerCase().includes(q) || app.name.toLowerCase().includes(q),
    );
  }, [data, search]);

  return (
    <>
      <PageHeader title="Applications" breadcrumbs={[{ label: "Applications" }]} />
      <div className="page-body">
        <div className="filters-bar">
          <div className="filter-group">
            <label className="filter-label">Search</label>
            <input
              className="filter-input"
              type="text"
              placeholder="Search by App ID or name..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </div>
        </div>
        <div className="card">
          <div className="table-wrapper">
            <table>
              <thead>
                <tr>
                  <th>App ID</th>
                  <th>Name</th>
                  <th>Status</th>
                  <th>Duration</th>
                  <th>Analyses</th>
                  <th>Started</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {isLoading && (
                  <tr>
                    <td colSpan={7}><div className="empty-state">Loading...</div></td>
                  </tr>
                )}
                {!isLoading && filtered.length === 0 && (
                  <tr>
                    <td colSpan={7}><div className="empty-state">No applications found</div></td>
                  </tr>
                )}
                {filtered.map((app) => (
                  <tr key={app.id}>
                    <td className="font-mono" style={{ fontSize: 13 }}>{app.id}</td>
                    <td>{app.name || "—"}</td>
                    <td>
                      <span className={`badge ${app.completed ? "badge-completed" : "badge-running"}`}>
                        <span className="badge-dot" />
                        {app.completed ? "Completed" : "Running"}
                      </span>
                    </td>
                    <td className="td-muted">{formatDurationMs(app.duration_ms)}</td>
                    <td>
                      {app.analysis_count > 0 ? (
                        <span className="analysis-count">{app.analysis_count}</span>
                      ) : (
                        <span className="td-muted">0</span>
                      )}
                    </td>
                    <td className="td-muted">{app.start_time ? formatDateTime(app.start_time) : "—"}</td>
                    <td>
                      <Link to={`/analyze?app_id=${app.id}`} className="btn-analyze">
                        <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}>
                          <path d="M22 12h-4l-3 9L9 3l-3 9H2" />
                        </svg>
                        Analyze
                      </Link>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {data && !search && (
            <Pagination total={data.total} limit={PAGE_SIZE} offset={offset} onPageChange={setOffset} />
          )}
        </div>
      </div>
    </>
  );
}
