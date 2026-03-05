from __future__ import annotations

from typing import TYPE_CHECKING

from spark_advisor_models.util.bytes import format_bytes

if TYPE_CHECKING:
    from spark_advisor_hs_connector.model.output import ApplicationSummary
    from spark_advisor_models.model import AnalysisResult, JobAnalysis
    from spark_advisor_models.model.output import RuleResult
    from spark_advisor_models.model.spark_config import SparkConfig


def format_job_overview(job: JobAnalysis) -> str:
    duration_s = job.duration_ms / 1000
    lines = [
        f"## Job Overview: {job.app_id}",
        "",
        f"- **App Name:** {job.app_name or 'N/A'}",
        f"- **Spark Version:** {job.spark_version or 'N/A'}",
        f"- **Duration:** {duration_s:.1f}s",
        f"- **Stages:** {len(job.stages)}",
    ]
    if job.executors:
        lines.append(f"- **Executors:** {job.executors.executor_count}")
        lines.append(f"- **Memory Utilization:** {job.executors.memory_utilization_percent:.1f}%")
    lines.append(f"- **Shuffle Partitions:** {job.config.shuffle_partitions}")
    lines.append(f"- **Executor Memory:** {job.config.executor_memory}")
    lines.append(f"- **Executor Cores:** {job.config.executor_cores}")
    return "\n".join(lines)


def format_rule_results(rule_results: list[RuleResult]) -> str:
    if not rule_results:
        return "No issues detected by rules engine."

    lines = ["## Rule Findings", ""]
    for r in rule_results:
        severity_icon = {"CRITICAL": "🔴", "WARNING": "🟡", "INFO": "🔵"}.get(r.severity.value, "⚪")
        lines.append(f"### {severity_icon} [{r.severity}] {r.title}")
        lines.append("")
        lines.append(r.message)
        if r.current_value and r.recommended_value:
            lines.append(f"- **Current:** `{r.current_value}`")
            lines.append(f"- **Recommended:** `{r.recommended_value}`")
        if r.estimated_impact:
            lines.append(f"- **Impact:** {r.estimated_impact}")
        lines.append("")
    return "\n".join(lines)


def format_analysis_result(result: AnalysisResult, *, use_ai: bool) -> str:
    parts = [
        format_job_overview(result.job),
        "",
        format_rule_results(result.rule_results),
    ]

    if use_ai and result.ai_report:
        report = result.ai_report
        parts.append("## AI Analysis")
        parts.append("")
        parts.append(f"**Overall Severity:** {report.severity}")
        parts.append("")
        parts.append(report.summary)
        parts.append("")

        if report.causal_chain:
            parts.append(f"**Causal Chain:** {report.causal_chain}")
            parts.append("")

        if report.recommendations:
            parts.append("### Recommendations")
            parts.append("")
            for rec in report.recommendations:
                parts.append(f"**{rec.priority}. {rec.title}**")
                if rec.parameter:
                    parts.append(f"- Parameter: `{rec.parameter}`")
                if rec.current_value:
                    parts.append(f"- Current: `{rec.current_value}`")
                if rec.recommended_value:
                    parts.append(f"- Recommended: `{rec.recommended_value}`")
                if rec.explanation:
                    parts.append(f"- {rec.explanation}")
                if rec.estimated_impact:
                    parts.append(f"- Impact: {rec.estimated_impact}")
                parts.append("")

        if report.suggested_config:
            parts.append("### Suggested spark-defaults.conf")
            parts.append("")
            parts.append("```properties")
            for key, value in sorted(report.suggested_config.items()):
                parts.append(f"{key}={value}")
            parts.append("```")

    return "\n".join(parts)


def format_config_table(config: SparkConfig) -> str:
    lines = ["## Spark Configuration", "", "| Key | Value |", "|-----|-------|"]
    for key in sorted(config.raw):
        lines.append(f"| `{key}` | `{config.raw[key]}` |")
    return "\n".join(lines)


def format_suggested_config(rule_results: list[RuleResult]) -> str:
    suggestions: dict[str, tuple[str, str]] = {}
    for r in rule_results:
        if r.recommended_value and r.current_value:
            param = r.rule_id
            suggestions[param] = (r.current_value, r.recommended_value)

    if not suggestions:
        return "No configuration changes suggested — all checks passed."

    lines = [
        "## Suggested Configuration Changes",
        "",
        "| Rule | Current | Recommended |",
        "|------|---------|-------------|",
    ]
    for rule_id, (current, recommended) in sorted(suggestions.items()):
        lines.append(f"| `{rule_id}` | `{current}` | `{recommended}` |")

    return "\n".join(lines)


def format_scan_results(apps: list[ApplicationSummary]) -> str:
    if not apps:
        return "No applications found."

    lines = [
        f"## Recent Spark Applications ({len(apps)})",
        "",
        "| App ID | Name | Duration | Status | Spark Version |",
        "|--------|------|----------|--------|---------------|",
    ]
    for app in apps:
        latest = app.latest_attempt
        duration = "-"
        status = ""
        spark_ver = ""
        if latest:
            if latest.duration > 0:
                duration = f"{latest.duration / 60_000:.1f} min"
            status = "✅ completed" if latest.completed else "🔄 running"
            spark_ver = latest.app_spark_version
        lines.append(f"| `{app.id}` | {app.name} | {duration} | {status} | {spark_ver} |")
    return "\n".join(lines)


METRIC_EXPLANATIONS: dict[str, str] = {
    "gc_time_percent": (
        "Percentage of total task execution time spent on JVM garbage collection. "
        "Values above 20% indicate memory pressure — the JVM is spending too much time "
        "reclaiming memory instead of doing useful work. Solution: increase `spark.executor.memory` "
        "or reduce data per partition by increasing `spark.sql.shuffle.partitions`."
    ),
    "spill_to_disk_bytes": (
        "Bytes spilled from memory to disk during shuffle or aggregation operations. "
        "Any spill > 0 means executor memory was insufficient to hold intermediate data. "
        "High spill significantly degrades performance due to disk I/O. "
        "Solution: increase `spark.executor.memory` or increase partition count."
    ),
    "data_skew_ratio": (
        "Ratio of maximum task duration to median task duration within a stage (max/median). "
        "A ratio > 5x indicates data skew — one partition has disproportionately more data. "
        "This causes stragglers that delay the entire stage. "
        "Solution: enable AQE skew join (`spark.sql.adaptive.skewJoin.enabled=true`) or salt join keys."
    ),
    "shuffle_read_bytes": (
        "Total bytes read during shuffle (data exchange between stages). "
        "Large shuffle volumes indicate heavy data movement across the network. "
        "Target partition size: ~128MB. Use this to calculate optimal `spark.sql.shuffle.partitions`."
    ),
    "shuffle_write_bytes": (
        "Total bytes written during shuffle output. This data will be read by the next stage. "
        "High shuffle write combined with many partitions may indicate over-partitioning."
    ),
    "executor_count": (
        "Number of executor JVM processes running the job. Each executor has its own memory and cores. "
        "Too many executors with low utilization wastes cluster resources. "
        "Too few may cause long task queues."
    ),
    "memory_utilization_percent": (
        "Ratio of peak memory used to total allocated memory across all executors. "
        "Low utilization (< 40%) means over-provisioned memory. "
        "High utilization (> 80%) combined with GC pressure suggests memory increase needed."
    ),
    "task_count": (
        "Number of tasks in a stage, determined by the number of partitions. "
        "Too few tasks means large partitions and potential OOM. "
        "Too many tasks means high scheduling overhead. Target: ~128MB per partition."
    ),
    "input_bytes": (
        "Total bytes read from input sources (files, tables). "
        "Small input per task (< 10MB) indicates too many small files or over-partitioning. "
        "Consider using `coalesce()` or increasing `spark.sql.files.maxPartitionBytes`."
    ),
    "duration_ms": (
        "Total wall-clock duration of the Spark application in milliseconds. "
        "This includes all stages, scheduling overhead, and idle time between stages."
    ),
}


def format_metric_explanation(metric_name: str, value: float) -> str:
    explanation = METRIC_EXPLANATIONS.get(metric_name)
    if not explanation:
        known = ", ".join(f"`{k}`" for k in sorted(METRIC_EXPLANATIONS))
        return (
            f"Unknown metric: `{metric_name}`. "
            f"Known metrics: {known}"
        )

    formatted_value = format_bytes(int(value)) if "bytes" in metric_name else f"{value:,.2f}"
    return (
        f"## Metric: `{metric_name}`\n\n"
        f"**Value:** {formatted_value}\n\n"
        f"{explanation}"
    )
