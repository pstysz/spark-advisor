import { useQuery, keepPreviousData } from "@tanstack/react-query";
import { get } from "@/lib/api";
import type { PaginatedTaskResponse, TaskStatsResponse, TaskStatus, DataSource } from "@/lib/types";

interface UseTasksListParams {
  limit: number;
  offset: number;
  status?: TaskStatus;
  app_id?: string;
  data_source?: DataSource;
}

export function useTasksList(params: UseTasksListParams) {
  return useQuery({
    queryKey: ["tasks", params],
    queryFn: () =>
      get<PaginatedTaskResponse>("/tasks", {
        limit: params.limit,
        offset: params.offset,
        status: params.status,
        app_id: params.app_id,
        data_source: params.data_source,
      }),
    placeholderData: keepPreviousData,
  });
}

export function useTaskStats() {
  return useQuery({
    queryKey: ["tasks", "stats"],
    queryFn: () => get<TaskStatsResponse>("/tasks/stats"),
  });
}
