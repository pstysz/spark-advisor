import { useState } from "react";
import { Link, useNavigate } from "react-router";
import { PageHeader } from "@/components/layout/PageHeader";
import { StatusBadge, ModeBadge, SourceBadge } from "@/components/ui/Badge";
import { FilterBar, type TaskFilters } from "@/components/ui/FilterBar";
import { Pagination } from "@/components/ui/Pagination";
import { SummaryStrip } from "@/components/ui/SummaryStrip";
import { useTasksList, useTaskStats } from "@/hooks/useTasks";
import { useTaskWebSocket } from "@/hooks/useTaskWebSocket";
import { useSubmitAnalysis } from "@/hooks/useAnalyze";
import { formatDuration, formatDateTime } from "@/lib/format";
import type { TaskResponse } from "@/lib/types";

const PAGE_SIZE = 20;

export function TasksPage() {
  const [filters, setFilters] = useState<TaskFilters>({});
  const [offset, setOffset] = useState(0);

  const navigate = useNavigate();

  useTaskWebSocket();

  const { mutate: rerun, isPending: rerunPending } = useSubmitAnalysis();
  const { data: stats } = useTaskStats();

  const handleRerun = (task: TaskResponse) => {
    rerun(
      { app_id: task.app_id, mode: task.mode, rerun: true },
      { onSuccess: (result) => { if (result.status === 202) navigate(`/tasks/${result.data.task_id}`); } },
    );
  };
  const { data, isLoading } = useTasksList({
    limit: PAGE_SIZE,
    offset,
    ...filters,
  });

  const handleFilterChange = (newFilters: TaskFilters) => {
    setFilters(newFilters);
    setOffset(0);
  };

  return (
    <>
      <PageHeader title="Tasks" breadcrumbs={[{ label: "Tasks" }]} />
      <div className="page-body">
        {stats && <SummaryStrip stats={stats} />}
        <FilterBar filters={filters} onFilterChange={handleFilterChange} />
        <div className="card">
          <div className="table-wrapper">
            <table>
              <thead>
                <tr>
                  <th style={{ minWidth: 280 }}>Task ID</th>
                  <th style={{ minWidth: 260 }}>App ID</th>
                  <th>Mode</th>
                  <th>Source</th>
                  <th>Status</th>
                  <th>Issues</th>
                  <th>Duration</th>
                  <th>Created</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {isLoading && (
                  <tr>
                    <td colSpan={9}>
                      <div className="empty-state">Loading...</div>
                    </td>
                  </tr>
                )}
                {data && data.items.length === 0 && (
                  <tr>
                    <td colSpan={9}>
                      <div className="empty-state">No tasks found</div>
                    </td>
                  </tr>
                )}
                {data?.items.map((task) => (
                  <tr key={task.task_id}>
                    <td>
                      <Link to={`/tasks/${task.task_id}`} className="td-link font-mono">
                        {task.task_id}
                      </Link>
                    </td>
                    <td className="font-mono td-muted">{task.app_id}</td>
                    <td><ModeBadge mode={task.mode} /></td>
                    <td><SourceBadge source={task.data_source} /></td>
                    <td><StatusBadge status={task.status} /></td>
                    <td className="td-muted">
                      {task.severity_counts
                        ? Object.entries(task.severity_counts)
                            .map(([sev, count]) => `${count} ${sev}`)
                            .join(", ")
                        : "—"}
                    </td>
                    <td className="td-muted">{formatDuration(task.started_at, task.completed_at)}</td>
                    <td className="td-muted">{formatDateTime(task.created_at)}</td>
                    <td>
                      <button
                        className="btn-rerun"
                        disabled={task.status === "running" || task.status === "pending" || rerunPending}
                        onClick={() => handleRerun(task)}
                        title={task.status === "running" || task.status === "pending" ? "Task still in progress" : "Rerun analysis"}
                      >
                        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round">
                          <path d="M21 12a9 9 0 1 1-9-9c2.52 0 4.93 1 6.74 2.74L21 8" />
                          <path d="M21 3v5h-5" />
                        </svg>
                        Rerun
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {data && (
            <Pagination
              total={data.total}
              limit={PAGE_SIZE}
              offset={offset}
              onPageChange={setOffset}
            />
          )}
        </div>
      </div>
    </>
  );
}
