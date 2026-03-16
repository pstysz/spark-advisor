import { useMutation, useQueryClient } from "@tanstack/react-query";
import { post } from "@/lib/api";
import type { AnalyzeRequest, AnalyzeResponse } from "@/lib/types";

export function useSubmitAnalysis() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (request: AnalyzeRequest) => post<AnalyzeResponse>("/analyze", request),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["tasks"] });
    },
  });
}
