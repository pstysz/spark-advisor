import { useState } from "react";
import { useParams } from "react-router";
import { PageHeader } from "@/components/layout/PageHeader";
import { Tabs } from "@/components/ui/Tabs";
import { OverviewTab } from "@/components/task/OverviewTab";
import { IssuesTab } from "@/components/task/IssuesTab";
import { StagesTab } from "@/components/task/StagesTab";
import { ConfigTab } from "@/components/task/ConfigTab";
import { useTask, useTaskRules, useTaskConfig } from "@/hooks/useTaskDetail";
import { useTaskWebSocket } from "@/hooks/useTaskWebSocket";
import { useMemo } from "react";

export function TaskDetailPage() {
  const { taskId } = useParams<{ taskId: string }>();
  const [activeTab, setActiveTab] = useState("overview");
  const wsTaskIds = useMemo(() => taskId ? [taskId] : [], [taskId]);
  useTaskWebSocket(wsTaskIds);

  const { data: task, isLoading, error } = useTask(taskId!);
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
