import { useState } from "react";
import { useParams, useNavigate } from "react-router";
import { PageHeader } from "@/components/layout/PageHeader";
import { Tabs } from "@/components/ui/Tabs";
import { OverviewTab } from "@/components/task/OverviewTab";
import { IssuesTab } from "@/components/task/IssuesTab";
import { StagesTab } from "@/components/task/StagesTab";
import { ConfigTab } from "@/components/task/ConfigTab";
import { useTask, useTaskRules, useTaskConfig } from "@/hooks/useTaskDetail";
import { useSubmitAnalysis } from "@/hooks/useAnalyze";

export function TaskDetailPage() {
  const { taskId } = useParams<{ taskId: string }>();
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState("overview");

  const { mutate: rerun, isPending: rerunPending } = useSubmitAnalysis();
  const { data: task, isLoading, error } = useTask(taskId!);
  const isTerminal = task?.status === "completed" || task?.status === "failed";
  const isCompleted = task?.status === "completed";
  const { data: rules } = useTaskRules(taskId!, isCompleted);
  const { data: config } = useTaskConfig(taskId!, isCompleted);

  if (isLoading) {
    return (
      <>
        <PageHeader title="Task Detail" breadcrumbs={[{ label: "Tasks", to: "/" }, { label: "Loading..." }]} />
        <div className="page-body">
          <div className="empty-state">Loading...</div>
        </div>
      </>
    );
  }

  if (error || !task) {
    return (
      <>
        <PageHeader title="Task Detail" breadcrumbs={[{ label: "Tasks", to: "/" }, { label: "Error" }]} />
        <div className="page-body">
          <div className="error-state">Task not found</div>
        </div>
      </>
    );
  }

  const rulesCount = rules?.length ?? 0;
  const stagesCount = rules ? new Set(rules.filter((r) => r.stage_id !== null).map((r) => r.stage_id)).size : 0;
  const configCount = config?.entries.length ?? 0;

  const tabs = [
    { id: "overview", label: "Overview", count: 1 },
    { id: "issues", label: "Issues", count: rulesCount },
    { id: "stages", label: "Stages", count: stagesCount },
    { id: "config", label: "Config", count: configCount },
  ];

  return (
    <>
      <PageHeader
        title="Task Detail"
        breadcrumbs={[
          { label: "Tasks", to: "/" },
          { label: task.task_id.slice(0, 8) + "..." },
        ]}
        actions={
          isTerminal ? (
            <button
              className="btn-rerun"
              disabled={rerunPending}
              onClick={() => {
                const isK8s = task.data_source === "k8s";
                if (isK8s) {
                  rerun(
                    { source: "k8s", request: { app_id: task.app_id, mode: task.mode } },
                    { onSuccess: (result) => { if (result.status === 202) navigate(`/tasks/${result.data.task_id}`); } },
                  );
                } else {
                  rerun(
                    { source: "hs", request: { app_id: task.app_id, mode: task.mode, rerun: true } },
                    { onSuccess: (result) => { if (result.status === 202) navigate(`/tasks/${result.data.task_id}`); } },
                  );
                }
              }}
            >
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round">
                <path d="M21 12a9 9 0 1 1-9-9c2.52 0 4.93 1 6.74 2.74L21 8" />
                <path d="M21 3v5h-5" />
              </svg>
              {rerunPending ? "Submitting..." : "Rerun Analysis"}
            </button>
          ) : undefined
        }
      />
      <div className="page-body">
        <div className="card">
          <Tabs tabs={tabs} activeTab={activeTab} onTabChange={setActiveTab} />
          {activeTab === "overview" && <OverviewTab task={task} />}
          {activeTab === "issues" && <IssuesTab rules={rules ?? []} />}
          {activeTab === "stages" && <StagesTab rules={rules ?? []} />}
          {activeTab === "config" && <ConfigTab entries={config?.entries ?? []} />}
        </div>
      </div>
    </>
  );
}
