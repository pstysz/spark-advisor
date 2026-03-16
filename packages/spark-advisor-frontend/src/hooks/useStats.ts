import { useQuery } from "@tanstack/react-query";
import { get } from "@/lib/api";
import type {
  StatsSummaryResponse,
  RuleFrequencyResponse,
  DailyVolumeResponse,
  TopIssuesResponse,
  TopAppsResponse,
  ModeBreakdownResponse,
  DurationByModeResponse,
  DataSourceBreakdownResponse,
  SeverityTrendResponse,
  FailureRateTrendResponse,
} from "@/lib/types";

export function useStatsSummary(days: number) {
  return useQuery({
    queryKey: ["stats", "summary", days],
    queryFn: () => get<StatsSummaryResponse>("/stats/summary", { days }),
  });
}

export function useRuleFrequency(days: number) {
  return useQuery({
    queryKey: ["stats", "rules", days],
    queryFn: () => get<RuleFrequencyResponse>("/stats/rules", { days }),
  });
}

export function useDailyVolume(days: number) {
  return useQuery({
    queryKey: ["stats", "daily-volume", days],
    queryFn: () => get<DailyVolumeResponse>("/stats/daily-volume", { days }),
  });
}

export function useTopIssues(days: number) {
  return useQuery({
    queryKey: ["stats", "top-issues", days],
    queryFn: () => get<TopIssuesResponse>("/stats/top-issues", { days }),
  });
}

export function useTopApps(days: number) {
  return useQuery({
    queryKey: ["stats", "top-apps", days],
    queryFn: () => get<TopAppsResponse>("/stats/top-apps", { days }),
  });
}

export function useModeBreakdown(days: number) {
  return useQuery({
    queryKey: ["stats", "mode-breakdown", days],
    queryFn: () => get<ModeBreakdownResponse>("/stats/mode-breakdown", { days }),
  });
}

export function useDurationByMode(days: number) {
  return useQuery({
    queryKey: ["stats", "duration-by-mode", days],
    queryFn: () => get<DurationByModeResponse>("/stats/duration-by-mode", { days }),
  });
}

export function useSourceBreakdown(days: number) {
  return useQuery({
    queryKey: ["stats", "source-breakdown", days],
    queryFn: () => get<DataSourceBreakdownResponse>("/stats/source-breakdown", { days }),
  });
}

export function useSeverityTrend(days: number) {
  return useQuery({
    queryKey: ["stats", "severity-trend", days],
    queryFn: () => get<SeverityTrendResponse>("/stats/severity-trend", { days }),
  });
}

export function useFailureRate(days: number) {
  return useQuery({
    queryKey: ["stats", "failure-rate", days],
    queryFn: () => get<FailureRateTrendResponse>("/stats/failure-rate", { days }),
  });
}
