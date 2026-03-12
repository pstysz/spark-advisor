from __future__ import annotations

from spark_advisor_models.util.bytes import format_bytes

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
