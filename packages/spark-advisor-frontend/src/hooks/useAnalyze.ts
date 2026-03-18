import { useMutation, useQueryClient } from "@tanstack/react-query";
import { post, ApiError } from "@/lib/api";
import { toast } from "@/hooks/useToast";
import type { AnalyzeRequest, AnalyzeResponse } from "@/lib/types";

export function useSubmitAnalysis() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (request: AnalyzeRequest) => post<AnalyzeResponse>("/analyze", request),
    onSuccess: () => {
      toast.success("Analysis submitted successfully");
      void queryClient.invalidateQueries({ queryKey: ["tasks"] });
    },
    onError: (error) => {
      if (error instanceof ApiError && error.status === 409) {
        toast.warning("An analysis for this application is already running");
      } else {
        toast.error(error instanceof Error ? error.message : "Failed to submit analysis");
      }
    },
  });
}
