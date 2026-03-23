import { useMutation, useQueryClient } from "@tanstack/react-query";
import { post, ApiError } from "@/lib/api";
import { toast } from "@/hooks/useToast";
import type { AnalyzeRequest, AnalyzeResponse, K8sAnalyzeRequest } from "@/lib/types";

type SubmitRequest =
  | { source: "hs"; request: AnalyzeRequest }
  | { source: "k8s"; request: K8sAnalyzeRequest };

export function useSubmitAnalysis() {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (params: SubmitRequest) => {
      const path = params.source === "k8s" ? "/k8s/analyze" : "/hs/analyze";
      return post<AnalyzeResponse>(path, params.request);
    },
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
