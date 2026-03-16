import { Routes, Route } from "react-router";
import { AppShell } from "@/components/layout/AppShell";
import { TasksPage } from "@/pages/TasksPage";
import { TaskDetailPage } from "@/pages/TaskDetailPage";
import { AnalyzePage } from "@/pages/AnalyzePage";
import { StatsPage } from "@/pages/StatsPage";

export function AppRouter() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route index element={<TasksPage />} />
        <Route path="tasks/:taskId" element={<TaskDetailPage />} />
        <Route path="analyze" element={<AnalyzePage />} />
        <Route path="stats" element={<StatsPage />} />
      </Route>
    </Routes>
  );
}
