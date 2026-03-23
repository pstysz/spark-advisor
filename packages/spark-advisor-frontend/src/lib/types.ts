export type TaskStatus = "pending" | "running" | "completed" | "failed";
export type AnalysisMode = "static" | "ai" | "agent";
export type DataSource = "hs_manual" | "hs_poller" | "file" | "k8s";
export type Severity = "CRITICAL" | "WARNING" | "INFO";

export interface TaskResponse {
  task_id: string;
  app_id: string;
  mode: AnalysisMode;
  data_source: DataSource;
  status: TaskStatus;
  severity_counts: Record<Severity, number> | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
  result: Record<string, unknown> | null;
}

export interface PaginatedTaskResponse {
  items: TaskResponse[];
  total: number;
  limit: number | null;
  offset: number;
}

export interface ApplicationResponse {
  id: string;
  name: string;
  start_time: string;
  end_time: string;
  duration_ms: number;
  completed: boolean;
  spark_version: string;
  user: string;
  analysis_count: number;
  data_source?: DataSource;
}

export interface PaginatedApplicationResponse {
  items: ApplicationResponse[];
  total: number;
  limit: number;
  offset: number;
}

export interface AnalyzeRequest {
  app_id: string;
  mode: AnalysisMode;
  data_source?: DataSource;
  rerun?: boolean;
}

export interface K8sAnalyzeRequest {
  namespace?: string;
  name?: string;
  app_id?: string;
  mode: AnalysisMode;
}

export interface AnalyzeResponse {
  task_id: string;
  status: TaskStatus;
}

export interface TaskStatsResponse {
  counts: Record<TaskStatus, number>;
  total: number;
}

export interface RuleViolationResponse {
  rule_id: string;
  severity: Severity;
  title: string;
  message: string;
  stage_id: number | null;
  current_value: string;
  recommended_value: string;
  estimated_impact: string;
}

export interface ConfigComparisonEntry {
  parameter: string;
  current_value: string;
  recommended_value: string;
  source: string;
}

export interface ConfigComparisonResponse {
  app_id: string;
  entries: ConfigComparisonEntry[];
}

export interface StatsSummaryResponse {
  total: number;
  completed: number;
  failed: number;
  avg_duration_seconds: number | null;
  ai_usage_percent: number | null;
  avg_issues_per_analysis: number | null;
}

export interface RuleFrequencyEntry {
  rule_id: string;
  title: string;
  count: number;
  severity: Severity;
}

export interface RuleFrequencyResponse {
  items: RuleFrequencyEntry[];
  days: number;
}

export interface DailyVolumeEntry {
  date: string;
  count: number;
}

export interface DailyVolumeResponse {
  items: DailyVolumeEntry[];
  days: number;
}

export interface ModeBreakdownEntry {
  mode: AnalysisMode;
  count: number;
}

export interface ModeBreakdownResponse {
  items: ModeBreakdownEntry[];
  days: number;
}

export interface DataSourceBreakdownEntry {
  data_source: DataSource;
  count: number;
}

export interface DataSourceBreakdownResponse {
  items: DataSourceBreakdownEntry[];
  days: number;
}

export interface SeverityTrendEntry {
  date: string;
  critical: number;
  warning: number;
  info: number;
}

export interface SeverityTrendResponse {
  items: SeverityTrendEntry[];
  days: number;
}

export interface TopAppEntry {
  app_id: string;
  analysis_count: number;
}

export interface TopAppsResponse {
  items: TopAppEntry[];
  limit: number;
  days: number;
}

export interface DurationByModeEntry {
  mode: AnalysisMode;
  avg_duration_seconds: number;
  count: number;
}

export interface DurationByModeResponse {
  items: DurationByModeEntry[];
  days: number;
}

export interface FailureRateTrendEntry {
  date: string;
  total: number;
  failed: number;
  rate: number;
}

export interface FailureRateTrendResponse {
  items: FailureRateTrendEntry[];
  days: number;
}

export interface TopIssueEntry {
  rule_id: string;
  title: string;
  message: string;
  count: number;
  severity: Severity;
  example_app_id: string;
}

export interface TopIssuesResponse {
  items: TopIssueEntry[];
  limit: number;
  days: number;
}
