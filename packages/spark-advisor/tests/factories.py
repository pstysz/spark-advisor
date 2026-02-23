from spark_advisor.model import RuleResult, Severity, SparkConfig
from spark_advisor.model.metrics import (
    ExecutorMetrics,
    JobAnalysis,
    StageMetrics,
    TaskMetrics,
)


def make_tasks(**overrides: object) -> TaskMetrics:
    defaults: dict[str, object] = {
        "task_count": 100,
        "median_duration_ms": 1000,
        "max_duration_ms": 2000,
        "min_duration_ms": 500,
        "total_gc_time_ms": 5000,
        "total_shuffle_read_bytes": 1024 * 1024 * 100,
        "total_shuffle_write_bytes": 1024 * 1024 * 50,
    }
    defaults.update(overrides)
    return TaskMetrics(**defaults)  # type: ignore[arg-type]


def make_stage(stage_id: int = 0, **task_overrides: object) -> StageMetrics:
    return StageMetrics(
        stage_id=stage_id,
        stage_name=f"Stage {stage_id}",
        duration_ms=100_000,
        input_bytes=1024 * 1024 * 200,
        output_bytes=1024 * 1024 * 50,
        tasks=make_tasks(**task_overrides),
    )


def make_job(**overrides: object) -> JobAnalysis:
    defaults: dict[str, object] = {
        "app_id": "app-test-001",
        "app_name": "TestJob",
        "duration_ms": 300_000,
        "config": SparkConfig(raw={"spark.executor.memory": "4g", "spark.executor.cores": "4"}),
        "stages": [make_stage(0)],
    }
    defaults.update(overrides)
    if isinstance(defaults.get("config"), dict):
        defaults["config"] = SparkConfig(raw=defaults["config"])  # type: ignore[arg-type]
    return JobAnalysis(**defaults)  # type: ignore[arg-type]


def make_executors(**overrides: object) -> ExecutorMetrics:
    defaults: dict[str, object] = {
        "executor_count": 10,
        "peak_memory_bytes": 1024 * 1024 * 1024 * 2,
        "allocated_memory_bytes": 1024 * 1024 * 1024 * 4,
        "total_cpu_time_ms": 500_000,
        "total_run_time_ms": 1_000_000,
    }
    defaults.update(overrides)
    return ExecutorMetrics(**defaults)  # type: ignore[arg-type]


def make_rule_result(**overrides: object) -> RuleResult:
    defaults: dict[str, object] = {
        "rule_id": "data_skew",
        "severity": Severity.WARNING,
        "title": "Data skew in Stage 0",
        "message": "Max task duration is 5.0x the median",
    }
    defaults.update(overrides)
    return RuleResult(**defaults)  # type: ignore[arg-type]
