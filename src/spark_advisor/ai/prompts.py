from spark_advisor.core import JobAnalysis, RuleResult

SYSTEM_PROMPT = """\
You are an expert Apache Spark performance engineer with 15 years of experience \
tuning Spark jobs on YARN and Kubernetes.

Your task: analyze Spark job metrics and generate concrete, actionable recommendations.

RULES:
1. Always provide SPECIFIC config values, not generalities.
   BAD: "Increase partition count"
   GOOD: "Change spark.sql.shuffle.partitions from 200 to 800"

2. Prioritize recommendations from highest to lowest impact.

3. For each recommendation provide:
   - parameter: the Spark config parameter (or "code change" if not a config)
   - current_value: what it is now
   - recommended_value: what it should be
   - explanation: brief explanation of the mechanism (2-3 sentences)
   - estimated_impact: quantified estimate (e.g., "~30% reduction in Stage 4 duration")
   - risk: potential downsides

4. If you see related problems, describe the causal chain.
   Example: "Skew in Stage 3 causes spill in Stage 4, which increases GC time"

5. Respond ONLY with valid JSON matching this exact schema:
{
  "summary": "1-2 sentence overview",
  "severity": "critical|warning|info",
  "recommendations": [
    {
      "priority": 1,
      "title": "short title",
      "parameter": "spark.xxx.yyy",
      "current_value": "200",
      "recommended_value": "800",
      "explanation": "why this helps",
      "estimated_impact": "expected improvement",
      "risk": "potential downsides"
    }
  ],
  "causal_chain": "description of how problems are related, if applicable"
}

TECHNICAL CONTEXT:
- spark.sql.shuffle.partitions default=200; target ~128MB per partition
- Spill to disk > 0 means insufficient memory for shuffle/aggregation
- Task duration skew (max/median > 3x) = data skew
- GC time > 20% of task time = memory pressure
- Executor idle > 30% = over-provisioning
- AQE (Adaptive Query Execution) can auto-handle skew if enabled
"""


def build_user_message(job: JobAnalysis, rule_results: list[RuleResult]) -> str:
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
    lines.append(f"Duration: {job.duration_ms / 1000:.0f}s ({job.duration_ms / 60000:.1f} min)")
    lines.append(f"Stages: {len(job.stages)}")

    lines.append("\n## Stage Metrics (problematic stages)")
    for stage in job.stages:
        has_issue = (
            stage.tasks.skew_ratio > 3
            or stage.tasks.spill_to_disk_bytes > 0
            or stage.tasks.gc_time_percent > 15
        )
        if not has_issue:
            continue

        lines.append(f"\n### Stage {stage.stage_id} — {stage.stage_name}")
        lines.append(f"- Task count: {stage.tasks.task_count}")
        lines.append(f"- Median task duration: {stage.tasks.median_duration_ms}ms")
        lines.append(f"- Max task duration: {stage.tasks.max_duration_ms}ms")
        if stage.tasks.skew_ratio > 3:
            lines.append(f"  <- SKEW ({stage.tasks.skew_ratio:.1f}x median)")
        lines.append(f"- Shuffle read: {_human_bytes(stage.tasks.total_shuffle_read_bytes)}")
        lines.append(f"- Shuffle write: {_human_bytes(stage.tasks.total_shuffle_write_bytes)}")
        if stage.tasks.spill_to_disk_bytes > 0:
            spill = _human_bytes(stage.tasks.spill_to_disk_bytes)
            lines.append(f"- Spill to disk: {spill}  <- SPILL")
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
