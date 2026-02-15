"""Tests for the rules engine.

Uses pytest — Python's standard test framework.
Tests are discovered by filename (test_*.py) and function name (test_*).
"""

from spark_advisor.analysis.rules import (
    DataSkewRule,
    GCPressureRule,
    ShufflePartitionsRule,
    SpillToDiskRule,
    run_rules,
)
from spark_advisor.models import (
    ExecutorMetrics,
    JobAnalysis,
    Severity,
    SparkConfig,
    StageMetrics,
    TaskMetrics,
)


def _make_job(
    stages: list[StageMetrics] | None = None,
    config: dict[str, str] | None = None,
    executors: ExecutorMetrics | None = None,
) -> JobAnalysis:
    """Helper to create a JobAnalysis with sensible defaults."""
    return JobAnalysis(
        app_id="test-app-001",
        app_name="Test Application",
        duration_ms=600_000,
        config=SparkConfig(raw=config or {"spark.sql.shuffle.partitions": "200"}),
        stages=stages or [],
        executors=executors,
    )


def _make_stage(
    stage_id: int = 0,
    median_ms: int = 100,
    max_ms: int = 100,
    gc_time_ms: int = 0,
    task_count: int = 200,
    shuffle_read: int = 0,
    spill_disk: int = 0,
) -> StageMetrics:
    return StageMetrics(
        stage_id=stage_id,
        stage_name=f"Stage {stage_id}",
        duration_ms=median_ms * task_count,
        tasks=TaskMetrics(
            task_count=task_count,
            median_duration_ms=median_ms,
            max_duration_ms=max_ms,
            min_duration_ms=median_ms // 2,
            total_gc_time_ms=gc_time_ms,
            total_shuffle_read_bytes=shuffle_read,
            spill_to_disk_bytes=spill_disk,
        ),
    )


class TestDataSkewRule:
    def test_no_skew(self):
        stage = _make_stage(median_ms=100, max_ms=200)
        job = _make_job(stages=[stage])
        results = DataSkewRule().evaluate(job)
        assert results == []

    def test_detects_moderate_skew(self):
        stage = _make_stage(median_ms=100, max_ms=800)
        job = _make_job(stages=[stage])
        results = DataSkewRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING

    def test_detects_critical_skew(self):
        stage = _make_stage(stage_id=4, median_ms=12, max_ms=340)
        job = _make_job(stages=[stage])
        results = DataSkewRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.CRITICAL
        assert "Stage 4" in results[0].title

    def test_suggests_aqe_when_disabled(self):
        stage = _make_stage(median_ms=10, max_ms=100)
        job = _make_job(
            stages=[stage],
            config={"spark.sql.adaptive.enabled": "false"},
        )
        results = DataSkewRule().evaluate(job)
        assert len(results) == 1
        assert "AQE" in results[0].recommended_value


class TestSpillToDiskRule:
    def test_no_spill(self):
        stage = _make_stage(spill_disk=0)
        job = _make_job(stages=[stage])
        assert SpillToDiskRule().evaluate(job) == []

    def test_detects_spill(self):
        stage = _make_stage(spill_disk=2 * 1024**3)
        job = _make_job(stages=[stage])
        results = SpillToDiskRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.CRITICAL


class TestGCPressureRule:
    def test_no_gc_pressure(self):
        stage = _make_stage(task_count=100, median_ms=1000, gc_time_ms=5000)
        job = _make_job(stages=[stage])
        assert GCPressureRule().evaluate(job) == []

    def test_detects_high_gc(self):
        stage = _make_stage(task_count=100, median_ms=1000, gc_time_ms=50_000)
        job = _make_job(stages=[stage])
        results = GCPressureRule().evaluate(job)
        assert len(results) == 1
        assert "GC" in results[0].title


class TestShufflePartitionsRule:
    def test_optimal_partitions(self):
        stage = _make_stage(shuffle_read=200 * 128 * 1024 * 1024)
        job = _make_job(stages=[stage], config={"spark.sql.shuffle.partitions": "200"})
        assert ShufflePartitionsRule().evaluate(job) == []

    def test_too_few_partitions(self):
        stage = _make_stage(shuffle_read=800 * 128 * 1024 * 1024)
        job = _make_job(stages=[stage], config={"spark.sql.shuffle.partitions": "200"})
        results = ShufflePartitionsRule().evaluate(job)
        assert len(results) == 1
        assert "800" in results[0].recommended_value


class TestRunRules:
    def test_returns_sorted_by_severity(self):
        stages = [
            _make_stage(stage_id=0, median_ms=100, max_ms=200, gc_time_ms=50_000, task_count=100),
            _make_stage(stage_id=1, median_ms=10, max_ms=500, spill_disk=5 * 1024**3),
        ]
        job = _make_job(stages=stages)
        results = run_rules(job)
        severities = [r.severity for r in results]
        assert severities[0] == Severity.CRITICAL

    def test_empty_job(self):
        job = _make_job(stages=[])
        results = run_rules(job)
        assert results == []
