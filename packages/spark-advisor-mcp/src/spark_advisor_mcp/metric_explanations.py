from __future__ import annotations

from typing import TYPE_CHECKING

from spark_advisor_models.util.bytes import format_bytes

if TYPE_CHECKING:
    from spark_advisor_models.config import Thresholds

METRIC_DESCRIPTIONS: dict[str, str] = {
    "gc_time_percent": (
        "Percentage of total task execution time spent on JVM garbage collection. "
        "Values above the warning threshold indicate memory pressure — the JVM is spending too much time "
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
        "A high ratio indicates data skew — one partition has disproportionately more data. "
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
        "High utilization combined with GC pressure suggests memory increase needed."
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
    "slot_utilization_percent": (
        "Percentage of available executor slots (cores x time) that were actively running tasks. "
        "Low utilization means the cluster is over-provisioned — paying for compute that sits idle. "
        "Consider reducing executor count, enabling dynamic allocation, or increasing parallelism."
    ),
}

_GB = 1024 * 1024 * 1024


def _get_thresholds(metric_name: str, thresholds: Thresholds) -> tuple[float | None, float | None]:
    mapping: dict[str, tuple[float | None, float | None]] = {
        "gc_time_percent": (thresholds.gc_warning_percent, thresholds.gc_critical_percent),
        "spill_to_disk_bytes": (thresholds.spill_warning_gb * _GB, thresholds.spill_critical_gb * _GB),
        "data_skew_ratio": (thresholds.skew_warning_ratio, thresholds.skew_critical_ratio),
        "memory_utilization_percent": (
            thresholds.memory_overhead_mem_utilization_percent,
            thresholds.memory_utilization_critical_percent,
        ),
    }
    return mapping.get(metric_name, (None, None))


def _assess_value(
    metric_name: str,
    value: float,
    thresholds: Thresholds,
) -> str:
    warning, critical = _get_thresholds(metric_name, thresholds)

    if critical is not None and value >= critical:
        return f"**Critical** — value exceeds the critical threshold ({critical:g})"
    if warning is not None and value >= warning:
        return f"**Warning** — value exceeds the warning threshold ({warning:g})"
    if warning is not None or critical is not None:
        return "**Healthy** — value is within normal range"
    return ""


def format_metric_explanation(metric_name: str, value: float, thresholds: Thresholds) -> str:
    description = METRIC_DESCRIPTIONS.get(metric_name)
    if not description:
        known = ", ".join(f"`{k}`" for k in sorted(METRIC_DESCRIPTIONS))
        return f"Unknown metric: `{metric_name}`. Known metrics: {known}"

    formatted_value = format_bytes(int(value)) if "bytes" in metric_name else f"{value:,.2f}"

    lines = [
        f"## Metric: `{metric_name}`",
        "",
        f"**Value:** {formatted_value}",
        "",
        description,
    ]

    assessment = _assess_value(metric_name, value, thresholds)
    if assessment:
        lines.append("")
        lines.append("### Assessment")
        lines.append("")
        lines.append(assessment)

    return "\n".join(lines)
