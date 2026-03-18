import { useState, useMemo } from "react";
import { Link } from "react-router";
import { PageHeader } from "@/components/layout/PageHeader";
import { StatusBadge, ModeBadge, SourceBadge } from "@/components/ui/Badge";
import { FilterBar, type TaskFilters } from "@/components/ui/FilterBar";
import { Pagination } from "@/components/ui/Pagination";
import { SummaryStrip } from "@/components/ui/SummaryStrip";
import { useTasksList, useTaskStats } from "@/hooks/useTasks";
import { useSubmitAnalysis } from "@/hooks/useAnalyze";
import { formatDuration, formatDateTime } from "@/lib/format";
import type { TaskResponse } from "@/lib/types";

const PAGE_SIZE = 20;

function groupByAppId(tasks: TaskResponse[]): Map<string, TaskResponse[]> {
  const groups = new Map<string, TaskResponse[]>();
  for (const task of tasks) {
    const list = groups.get(task.app_id);
    if (list) {
      list.push(task);
    } else {
      groups.set(task.app_id, [task]);
    }
  }
  return groups;
}

function TaskRow({ task, handleRerun, rerunPending, showAppId }: {
  task: TaskResponse;
  handleRerun: (task: TaskResponse) => void;
  rerunPending: boolean;
  showAppId: boolean;
}) {
  return (
    <tr key={task.task_id}>
      {showAppId && (
        <td className="font-mono td-muted">{task.app_id}</td>
      )}
      <td>
        <Link to={`/tasks/${task.task_id}`} className="td-link font-mono">
          {task.task_id}
        </Link>
      </td>
      <td style={{ textAlign: "center" }}><ModeBadge mode={task.mode} /></td>
      <td style={{ textAlign: "center" }}><SourceBadge source={task.data_source} /></td>
      <td style={{ textAlign: "center" }}>
        <StatusBadge status={task.status} />
        {task.status === "failed" && task.error && (
          <div style={{ fontSize: 11, color: "var(--severity-critical)", marginTop: 4, maxWidth: 200, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={task.error}>
            {task.error}
          </div>
        )}
      </td>
      <td style={{ textAlign: "center" }}>
        {task.severity_counts ? (
          <div style={{ display: "flex", gap: 4, justifyContent: "center" }}>
            {(["CRITICAL", "WARNING", "INFO"] as const).map((sev) => {
              const count = task.severity_counts?.[sev];
              if (!count) return null;
              return (
                <span key={sev} className={`badge badge-${sev.toLowerCase()}`} style={{ minWidth: 22, textAlign: "center" }}>
                  {count}
                </span>
              );
            })}
          </div>
        ) : (
          <span className="td-muted">—</span>
        )}
      </td>
      <td style={{ textAlign: "center" }} className="td-muted">{formatDuration(task.started_at, task.completed_at)}</td>
      <td style={{ textAlign: "center" }} className="td-muted">{formatDateTime(task.created_at)}</td>
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
  );
}

const COL_COUNT = 9;

export function TasksPage() {
  const [filters, setFilters] = useState<TaskFilters>({});
  const [offset, setOffset] = useState(0);
  const [grouped, setGrouped] = useState(false);

  const { mutate: rerun, isPending: rerunPending } = useSubmitAnalysis();
  const { data: stats } = useTaskStats();

  const handleRerun = (task: TaskResponse) => {
    rerun(
      { app_id: task.app_id, mode: task.mode, rerun: true },
      {},
    );
  };
  const { data, isLoading } = useTasksList({
    limit: PAGE_SIZE,
    offset,
    ...filters,
  });

  const groups = useMemo(
    () => (data ? groupByAppId(data.items) : null),
    [data],
  );

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
          <div style={{ display: "flex", alignItems: "center", gap: 10, padding: "10px 16px 0" }}>
            <button
              onClick={() => setGrouped(!grouped)}
              title={grouped ? "Show flat list" : "Group by Application ID"}
              style={{
                position: "relative",
                width: 36,
                height: 20,
                borderRadius: 10,
                border: "none",
                background: grouped ? "var(--accent, #3b82f6)" : "var(--border, #3a3a4e)",
                cursor: "pointer",
                transition: "background 0.2s ease",
                padding: 0,
                flexShrink: 0,
              }}
            >
              <span style={{
                position: "absolute",
                top: 2,
                left: grouped ? 18 : 2,
                width: 16,
                height: 16,
                borderRadius: "50%",
                background: "#fff",
                transition: "left 0.2s ease",
              }} />
            </button>
            <span style={{ fontSize: 13, color: grouped ? "var(--text-primary)" : "var(--text-muted)", userSelect: "none" }}>
              Group by App
            </span>
          </div>
          <div className="table-wrapper">
            <table>
              <thead>
                <tr>
                  {!grouped && <th style={{ minWidth: 260 }}>App ID</th>}
                  <th style={{ minWidth: 280 }}>Task ID</th>
                  <th style={{ textAlign: "center" }}>Mode</th>
                  <th style={{ textAlign: "center" }}>Source</th>
                  <th>Status</th>
                  <th style={{ textAlign: "center" }}>Issues</th>
                  <th>Duration</th>
                  <th>Created</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {isLoading && (
                  <tr>
                    <td colSpan={COL_COUNT}>
                      <div className="empty-state">Loading...</div>
                    </td>
                  </tr>
                )}
                {data && data.items.length === 0 && (
                  <tr>
                    <td colSpan={COL_COUNT}>
                      <div className="empty-state">No tasks found</div>
                    </td>
                  </tr>
                )}
                {data && !grouped && data.items.map((task) => (
                  <TaskRow key={task.task_id} task={task} handleRerun={handleRerun} rerunPending={rerunPending} showAppId />
                ))}
                {data && grouped && groups && Array.from(groups.entries()).map(([appId, tasks]) => (
                  <GroupedRows key={appId} appId={appId} tasks={tasks} handleRerun={handleRerun} rerunPending={rerunPending} />
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

function GroupedRows({ appId, tasks, handleRerun, rerunPending }: {
  appId: string;
  tasks: TaskResponse[];
  handleRerun: (task: TaskResponse) => void;
  rerunPending: boolean;
}) {
  return (
    <>
      <tr className="group-header">
        <td colSpan={8} style={{
          padding: "10px 16px",
          background: "var(--bg-secondary, #1a1a2e)",
          borderBottom: "1px solid var(--border)",
          fontWeight: 600,
          fontSize: 13,
        }}>
          <span className="font-mono" style={{ color: "var(--accent)" }}>{appId}</span>
          <span style={{ color: "var(--text-muted)", fontWeight: 400, marginLeft: 8 }}>
            ({tasks.length} {tasks.length === 1 ? "task" : "tasks"})
          </span>
        </td>
      </tr>
      {tasks.map((task) => (
        <TaskRow key={task.task_id} task={task} handleRerun={handleRerun} rerunPending={rerunPending} showAppId={false} />
      ))}
    </>
  );
}
