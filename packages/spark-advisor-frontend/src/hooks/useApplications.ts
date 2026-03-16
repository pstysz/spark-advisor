import { useQuery, keepPreviousData } from "@tanstack/react-query";
import { get } from "@/lib/api";
import type { PaginatedApplicationResponse } from "@/lib/types";

interface UseApplicationsParams {
  limit: number;
  offset: number;
}

export function useApplicationsList(params: UseApplicationsParams) {
  return useQuery({
    queryKey: ["applications", params],
    queryFn: () =>
      get<PaginatedApplicationResponse>("/applications", {
        limit: params.limit,
        offset: params.offset,
      }),
    placeholderData: keepPreviousData,
  });
}
