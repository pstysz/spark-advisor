from spark_advisor.analysis.config import RuleThresholds
from spark_advisor.core import JobAnalysis, RuleResult

_SYSTEM_PROMPT_TEMPLATE = """\
You are an expert Apache Spark performance engineer with 15 years of experience \
tuning Spark jobs on YARN and Kubernetes.

Your task: analyze Spark job metrics and generate concrete, actionable recommendations.

RULES:
1. Always provide SPECIFIC config values, not generalities.
   BAD: "Increase partition count"
   GOOD: "Change spark.sql.shuffle.partitions from 200 to 800"

2. Prioritize recommendations from highest to lowest impact. Return at most 7.

3. For each recommendation provide:
   - parameter: the Spark config parameter (use "code_change" for non-config recommendations)
   - current_value: what it is now
   - recommended_value: what it should be
   - explanation: brief explanation of the mechanism (2-3 sentences)
   - estimated_impact: quantified estimate (e.g., "~30% reduction in Stage 4 duration")
   - risk: potential downsides

4. If you see related problems, describe the causal chain.
   Example: "Skew in Stage 3 causes spill in Stage 4, which increases GC time"

5. If no significant issues are found, return severity "info" with a brief summary \
and an empty recommendations list. Do not invent problems.

6. Use the submit_analysis tool to return your results.

TECHNICAL CONTEXT:
- spark.sql.shuffle.partitions default=200; target ~{target_partition_mb}MB per partition
- Spill to disk > 0 means insufficient memory for shuffle/aggregation
- Task duration skew: WARNING if max/median > {skew_warning}x, CRITICAL if > {skew_critical}x
- GC time: WARNING if > {gc_warning}% of task time, CRITICAL if > {gc_critical}%
- Executor CPU utilization < {min_cpu}% = over-provisioning
- AQE (Adaptive Query Execution) can auto-handle skew if enabled (default since Spark 3.2)
"""


def build_system_prompt(thresholds: RuleThresholds | None = None) -> str:
    t = thresholds or RuleThresholds()
    return _SYSTEM_PROMPT_TEMPLATE.format(
        target_partition_mb=t.target_partition_size_bytes // (1024 * 1024),
        skew_warning=t.skew_warning_ratio,
        skew_critical=t.skew_critical_ratio,
        gc_warning=t.gc_warning_percent,
        gc_critical=t.gc_critical_percent,
        min_cpu=t.min_cpu_utilization_percent,
    )


SYSTEM_PROMPT = build_system_prompt()


def build_user_message(
    job: JobAnalysis,
    rule_results: list[RuleResult],
    thresholds: RuleThresholds | None = None,
) -> str:
    t = thresholds or RuleThresholds()
    lines: list[str] = ["Analyze this Spark job and suggest optimizations.\n", "## Configuration"]

    important_keys = [
        "spark.executor.memory",
        "spark.executor.cores",
        "spark.executor.instances",
        "spark.driver.memory",
        "spark.sql.shuffle.partitions",
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.skewJoin.enabled",
        "spark.dynamicAllocation.enabled",
        "spark.serializer",
    ]
    for key in important_keys:
        value = job.config.get(key)
        if value:
            lines.append(f"{key} = {value}")

    if job.environment:
        lines.append(f"Environment: {job.environment}")

    lines.append("\n## Job Overview")
    lines.append(f"App ID: {job.app_id}")
    if job.spark_version:
        lines.append(f"Spark version: {job.spark_version}")
    lines.append(f"Duration: {job.duration_ms / 1000:.0f}s ({job.duration_ms / 60000:.1f} min)")
    lines.append(f"Stages: {len(job.stages)}")
    total_tasks = sum(s.tasks.task_count for s in job.stages)
    total_input = sum(s.input_bytes for s in job.stages)
    total_shuffle_read = sum(s.tasks.total_shuffle_read_bytes for s in job.stages)
    total_shuffle_write = sum(s.tasks.total_shuffle_write_bytes for s in job.stages)
    total_spill = sum(s.tasks.spill_to_disk_bytes for s in job.stages)
    lines.append(f"Total tasks: {total_tasks}")
    lines.append(f"Total input: {_human_bytes(total_input)}")
    lines.append(f"Total shuffle read: {_human_bytes(total_shuffle_read)}")
    lines.append(f"Total shuffle write: {_human_bytes(total_shuffle_write)}")
    if total_spill > 0:
        lines.append(f"Total spill to disk: {_human_bytes(total_spill)}")

    lines.append("\n## Stage Metrics")
    for stage in job.stages:
        flags: list[str] = []
        if stage.tasks.skew_ratio > t.skew_warning_ratio:
            flags.append(f"SKEW({stage.tasks.skew_ratio:.1f}x)")
        if stage.tasks.spill_to_disk_bytes > 0:
            flags.append("SPILL")
        if stage.tasks.gc_time_percent > t.gc_warning_percent:
            flags.append(f"GC({stage.tasks.gc_time_percent:.0f}%)")
        if stage.tasks.failed_task_count >= t.task_failure_warning_count:
            flags.append(f"FAILURES({stage.tasks.failed_task_count})")

        header = f"\n### Stage {stage.stage_id} — {stage.stage_name}"
        if flags:
            header += f"  [{', '.join(flags)}]"
        lines.append(header)

        lines.append(f"- Tasks: {stage.tasks.task_count}")
        lines.append(
            f"- Task duration: min={stage.tasks.min_duration_ms}ms "
            f"median={stage.tasks.median_duration_ms}ms "
            f"max={stage.tasks.max_duration_ms}ms"
        )
        lines.append(f"- Input: {_human_bytes(stage.input_bytes)}")
        lines.append(f"- Output: {_human_bytes(stage.output_bytes)}")
        lines.append(f"- Shuffle read: {_human_bytes(stage.tasks.total_shuffle_read_bytes)}")
        lines.append(f"- Shuffle write: {_human_bytes(stage.tasks.total_shuffle_write_bytes)}")
        if stage.tasks.spill_to_disk_bytes > 0:
            lines.append(f"- Spill to disk: {_human_bytes(stage.tasks.spill_to_disk_bytes)}")
        lines.append(f"- GC time: {stage.tasks.gc_time_percent:.0f}% of task time")

    if job.executors:
        lines.append("\n## Executor Metrics")
        lines.append(f"- Count: {job.executors.executor_count}")
        lines.append(f"- Memory utilization: {job.executors.memory_utilization_percent:.0f}%")
        lines.append(f"- CPU utilization: {job.executors.cpu_utilization_percent:.0f}%")

    if rule_results:
        lines.append("\n## Issues Detected by Rules Engine")
        for i, rule in enumerate(rule_results, 1):
            lines.append(f"{i}. {rule.severity.upper()}: {rule.title} — {rule.message}")

    return "\n".join(lines)


def _human_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n = int(n / 1024)
    return f"{n:.1f} PB"
