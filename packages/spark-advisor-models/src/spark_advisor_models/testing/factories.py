from typing import Any

from spark_advisor_models.model import (
    ExecutorMetrics,
    JobAnalysis,
    Quantiles,
    RuleResult,
    Severity,
    SparkConfig,
    StageMetrics,
    TaskMetrics,
    TaskMetricsDistributions,
)


def make_quantiles(min: int = 0, p25: int = 0, median: int = 0, p75: int = 0, max: int = 0) -> Quantiles:
    return Quantiles(min=min, p25=p25, median=median, p75=p75, max=max)


def make_stage(
    stage_id: int = 0,
    *,
    task_count: int = 100,
    run_time_min: int = 500,
    run_time_median: int = 1000,
    run_time_max: int = 2000,
    sum_executor_run_time_ms: int | None = None,
    total_gc_time_ms: int = 5000,
    total_shuffle_read_bytes: int = 100 * 1024 * 1024,
    total_shuffle_write_bytes: int = 50 * 1024 * 1024,
    spill_to_disk_bytes: int = 0,
    spill_to_memory_bytes: int = 0,
    failed_task_count: int = 0,
    killed_task_count: int = 0,
    input_bytes: int = 200 * 1024 * 1024,
    input_records: int = 0,
    output_bytes: int = 50 * 1024 * 1024,
    output_records: int = 0,
    shuffle_read_records: int = 0,
    shuffle_write_records: int = 0,
    peak_memory_max: int = 0,
    scheduler_delay_max: int = 0,
) -> StageMetrics:
    run_time = make_quantiles(
        min=run_time_min,
        p25=(run_time_min + run_time_median) // 2,
        median=run_time_median,
        p75=(run_time_median + run_time_max) // 2,
        max=run_time_max,
    )
    computed_sum = sum_executor_run_time_ms if sum_executor_run_time_ms is not None else task_count * run_time_median

    peak_mem = make_quantiles(max=peak_memory_max, median=peak_memory_max // 2) if peak_memory_max else Quantiles()
    sched = (
        make_quantiles(max=scheduler_delay_max, median=scheduler_delay_max // 4) if scheduler_delay_max else Quantiles()
    )

    return StageMetrics(
        stage_id=stage_id,
        stage_name=f"Stage {stage_id}",
        sum_executor_run_time_ms=computed_sum,
        total_gc_time_ms=total_gc_time_ms,
        total_shuffle_read_bytes=total_shuffle_read_bytes,
        total_shuffle_write_bytes=total_shuffle_write_bytes,
        spill_to_disk_bytes=spill_to_disk_bytes,
        spill_to_memory_bytes=spill_to_memory_bytes,
        failed_task_count=failed_task_count,
        killed_task_count=killed_task_count,
        input_bytes=input_bytes,
        input_records=input_records,
        output_bytes=output_bytes,
        output_records=output_records,
        shuffle_read_records=shuffle_read_records,
        shuffle_write_records=shuffle_write_records,
        tasks=TaskMetrics(
            task_count=task_count,
            distributions=TaskMetricsDistributions(
                duration=run_time,
                executor_run_time=run_time,
                peak_execution_memory=peak_mem,
                scheduler_delay=sched,
            ),
        ),
    )


def make_job(**overrides: Any) -> JobAnalysis:
    defaults: dict[str, Any] = {
        "app_id": "app-test-001",
        "app_name": "TestJob",
        "duration_ms": 300_000,
        "config": SparkConfig(raw={"spark.executor.memory": "4g", "spark.executor.cores": "4"}),
        "stages": [make_stage(0)],
    }
    defaults.update(overrides)
    if isinstance(defaults.get("config"), dict):
        defaults["config"] = SparkConfig(raw=defaults["config"])
    return JobAnalysis(**defaults)


def make_executors(**overrides: Any) -> ExecutorMetrics:
    defaults: dict[str, Any] = {
        "executor_count": 10,
        "peak_memory_bytes_sum": 1024 * 1024 * 1024 * 2,
        "allocated_memory_bytes_sum": 1024 * 1024 * 1024 * 4,
        "total_task_time_ms": 500_000,
    }
    defaults.update(overrides)
    return ExecutorMetrics(**defaults)


def make_rule_result(**overrides: Any) -> RuleResult:
    defaults: dict[str, Any] = {
        "rule_id": "data_skew",
        "severity": Severity.WARNING,
        "title": "Data skew in Stage 0",
        "message": "Max task duration is 5.0x the median",
    }
    defaults.update(overrides)
    return RuleResult(**defaults)
