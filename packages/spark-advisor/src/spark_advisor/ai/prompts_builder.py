from spark_advisor.ai.config import IMPORTANT_KEYS, SYSTEM_PROMPT_TEMPLATE
from spark_advisor_models.config import Thresholds
from spark_advisor_models.model import JobAnalysis, RuleResult, StageMetrics
from spark_advisor_models.util import format_bytes


def build_user_message(
    job: JobAnalysis,
    rule_results: list[RuleResult],
    thresholds: Thresholds | None = None,
) -> str:
    t = thresholds or Thresholds()
    lines: list[str] = ["Analyze this Spark job and suggest optimizations.\n", "## Configuration"]

    for key in IMPORTANT_KEYS:
        value = job.config.get(key)
        if value:
            lines.append(f"{key} = {value}")

    _append_job_overview(lines, job)
    _append_stage_metrics(lines, job, t)
    _append_executor_metrics(lines, job)
    _append_rule_results(lines, rule_results)

    return "\n".join(lines)


def _append_job_overview(lines: list[str], job: JobAnalysis) -> None:
    lines.append("\n## Job Overview")
    lines.append(f"App ID: {job.app_id}")
    if job.app_name:
        lines.append(f"App name: {job.app_name}")
    if job.spark_version:
        lines.append(f"Spark version: {job.spark_version}")
    lines.append(f"Duration: {job.duration_ms / 1000:.0f}s ({job.duration_ms / 60000:.1f} min)")
    lines.append(f"Stages: {len(job.stages)}")
    total_tasks = sum(s.tasks.task_count for s in job.stages)
    total_input = sum(s.input_bytes for s in job.stages)
    total_shuffle_read = sum(s.total_shuffle_read_bytes for s in job.stages)
    total_shuffle_write = sum(s.total_shuffle_write_bytes for s in job.stages)
    total_spill_disk = sum(s.spill_to_disk_bytes for s in job.stages)
    total_spill_memory = sum(s.spill_to_memory_bytes for s in job.stages)
    lines.append(f"Total tasks: {total_tasks}")
    lines.append(f"Total input: {format_bytes(total_input)}")
    lines.append(f"Total shuffle read: {format_bytes(total_shuffle_read)}")
    lines.append(f"Total shuffle write: {format_bytes(total_shuffle_write)}")
    if total_spill_disk > 0:
        lines.append(f"Total spill to disk: {format_bytes(total_spill_disk)}")
    if total_spill_memory > 0:
        lines.append(f"Total spill to memory: {format_bytes(total_spill_memory)}")


def _append_stage_metrics(lines: list[str], job: JobAnalysis, t: Thresholds) -> None:
    lines.append("\n## Stage Metrics")
    for stage in job.stages:
        _append_single_stage(lines, stage, t)


def _append_single_stage(lines: list[str], stage: StageMetrics, t: Thresholds) -> None:
    flags: list[str] = []
    if stage.tasks.duration_skew_ratio > t.skew_warning_ratio:
        flags.append(f"SKEW({stage.tasks.duration_skew_ratio:.1f}x)")
    if stage.spill_to_disk_bytes > 0:
        flags.append("SPILL")
    if stage.gc_time_percent > t.gc_warning_percent:
        flags.append(f"GC({stage.gc_time_percent:.0f}%)")
    if stage.failed_task_count >= t.task_failure_warning_count:
        flags.append(f"FAILURES({stage.failed_task_count})")
    if stage.killed_task_count > 0:
        flags.append(f"KILLED({stage.killed_task_count})")

    header = f"\n### Stage {stage.stage_id} — {stage.stage_name}"
    if flags:
        header += f"  [{', '.join(flags)}]"
    lines.append(header)

    dist = stage.tasks.distributions

    lines.append(f"- Tasks: {stage.tasks.task_count}")

    dur = dist.duration
    lines.append(
        f"- Task duration (wall-clock): min={dur.min:.0f}ms "
        f"median={dur.median:.0f}ms "
        f"max={dur.max:.0f}ms"
    )

    run = dist.executor_run_time
    if run.max > 0:
        lines.append(
            f"- Executor run time (compute): min={run.min:.0f}ms "
            f"median={run.median:.0f}ms "
            f"max={run.max:.0f}ms"
        )

    peak_mem = dist.peak_execution_memory
    if peak_mem.max > 0:
        lines.append(
            f"- Peak execution memory: median={format_bytes(int(peak_mem.median))} "
            f"max={format_bytes(int(peak_mem.max))}"
        )

    sched = dist.scheduler_delay
    if sched.max > t.scheduler_delay_ms:
        lines.append(
            f"- Scheduler delay: median={sched.median:.0f}ms max={sched.max:.0f}ms"
        )

    lines.append(f"- Input: {format_bytes(stage.input_bytes)}")
    if stage.input_records > 0:
        lines.append(f"- Input records: {stage.input_records:,}")
    lines.append(f"- Output: {format_bytes(stage.output_bytes)}")
    if stage.output_records > 0:
        lines.append(f"- Output records: {stage.output_records:,}")
    lines.append(f"- Shuffle read: {format_bytes(stage.total_shuffle_read_bytes)}")
    if stage.shuffle_read_records > 0:
        lines.append(f"- Shuffle read records: {stage.shuffle_read_records:,}")
    lines.append(f"- Shuffle write: {format_bytes(stage.total_shuffle_write_bytes)}")
    if stage.shuffle_write_records > 0:
        lines.append(f"- Shuffle write records: {stage.shuffle_write_records:,}")
    if stage.spill_to_disk_bytes > 0:
        lines.append(f"- Spill to disk: {format_bytes(stage.spill_to_disk_bytes)}")
    if stage.spill_to_memory_bytes > 0:
        lines.append(f"- Spill to memory: {format_bytes(stage.spill_to_memory_bytes)}")
    lines.append(f"- GC time: {stage.gc_time_percent:.0f}% of compute time")


def _append_executor_metrics(lines: list[str], job: JobAnalysis) -> None:
    if not job.executors:
        return

    ex = job.executors
    lines.append("\n## Executor Metrics")
    lines.append(f"- Count: {ex.executor_count}")
    if ex.total_cores > 0:
        lines.append(f"- Total cores: {ex.total_cores}")
    lines.append(
        f"- Memory: {format_bytes(ex.peak_memory_bytes_sum)} peak "
        f"/ {format_bytes(ex.allocated_memory_bytes_sum)} allocated "
        f"({ex.memory_utilization_percent:.0f}% utilization)"
    )

    cores = ex.total_cores if ex.total_cores > 0 else job.config.executor_cores * ex.executor_count
    total_slot_time = cores * job.duration_ms
    if total_slot_time > 0 and ex.total_task_time_ms > 0:
        slot_pct = (ex.total_task_time_ms / total_slot_time) * 100
        lines.append(f"- Slot utilization: {slot_pct:.0f}%")

    if ex.total_gc_time_ms > 0:
        lines.append(f"- Total GC time: {ex.total_gc_time_ms / 1000:.1f}s")
    if ex.failed_tasks > 0:
        lines.append(f"- Failed tasks: {ex.failed_tasks}")


def _append_rule_results(lines: list[str], rule_results: list[RuleResult]) -> None:
    if rule_results:
        lines.append("\n## Issues Detected by Rules Engine")
        for i, rule in enumerate(rule_results, 1):
            lines.append(f"{i}. {rule.severity.name}: {rule.title} — {rule.message}")


def build_system_prompt(thresholds: Thresholds | None = None) -> str:
    t = thresholds or Thresholds()
    return SYSTEM_PROMPT_TEMPLATE.format(
        target_partition_mb=t.target_partition_size_bytes // (1024 * 1024),
        skew_warning=t.skew_warning_ratio,
        skew_critical=t.skew_critical_ratio,
        gc_warning=t.gc_warning_percent,
        gc_critical=t.gc_critical_percent,
        min_slot=t.min_slot_utilization_percent,
        scheduler_delay_ms=t.scheduler_delay_ms,
        spill_warning_gb=t.spill_warning_gb,
        spill_critical_gb=t.spill_critical_gb,
    )
