from spark_advisor.ai.config import Thresholds
from spark_advisor.model import RuleResult
from spark_advisor.model.metrics import JobAnalysis
from spark_advisor.util.bytes_helper import BytesHelper


def build_user_message(
    job: JobAnalysis,
    rule_results: list[RuleResult],
    thresholds: Thresholds | None = None,
) -> str:
    t = thresholds or Thresholds()
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
    lines.append(f"Total input: {BytesHelper.to_human_bytes(total_input)}")
    lines.append(f"Total shuffle read: {BytesHelper.to_human_bytes(total_shuffle_read)}")
    lines.append(f"Total shuffle write: {BytesHelper.to_human_bytes(total_shuffle_write)}")
    if total_spill > 0:
        lines.append(f"Total spill to disk: {BytesHelper.to_human_bytes(total_spill)}")

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
        lines.append(f"- Input: {BytesHelper.to_human_bytes(stage.input_bytes)}")
        lines.append(f"- Output: {BytesHelper.to_human_bytes(stage.output_bytes)}")
        lines.append(
            f"- Shuffle read: {BytesHelper.to_human_bytes(stage.tasks.total_shuffle_read_bytes)}"
        )
        lines.append(
            f"- Shuffle write: {BytesHelper.to_human_bytes(stage.tasks.total_shuffle_write_bytes)}"
        )
        if stage.tasks.spill_to_disk_bytes > 0:
            lines.append(
                f"- Spill to disk: {BytesHelper.to_human_bytes(stage.tasks.spill_to_disk_bytes)}"
            )
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
