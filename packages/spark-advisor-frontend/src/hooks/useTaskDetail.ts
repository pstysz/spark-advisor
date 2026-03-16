import { useQuery } from "@tanstack/react-query";
import { get } from "@/lib/api";
import type { TaskResponse, RuleViolationResponse, ConfigComparisonResponse } from "@/lib/types";

export function useTask(taskId: string) {
  return useQuery({
    queryKey: ["task", taskId],
    queryFn: () => get<TaskResponse>(`/tasks/${taskId}`),
  });
}

export function useTaskRules(taskId: string, enabled: boolean) {
  return useQuery({
    queryKey: ["task", taskId, "rules"],
    queryFn: () => get<RuleViolationResponse[]>(`/tasks/${taskId}/rules`),
    enabled,
  });
}

export function useTaskConfig(taskId: string, enabled: boolean) {
  return useQuery({
    queryKey: ["task", taskId, "config"],
    queryFn: () => get<ConfigComparisonResponse>(`/tasks/${taskId}/config`),
    enabled,
  });
}
