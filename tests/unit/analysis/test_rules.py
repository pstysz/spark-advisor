from spark_advisor.analysis.rules import (
    DataSkewRule,
    GCPressureRule,
    ShufflePartitionsRule,
    SpillToDiskRule,
    TaskFailureRule,
)
from spark_advisor.analysis.static_analysis_service import StaticAnalysisService
from spark_advisor.model import Severity
from tests.factories import make_job, make_stage


class TestDataSkewRule:
    def test_no_skew(self):
        stage = make_stage(median_duration_ms=100, max_duration_ms=200)
        job = make_job(stages=[stage])
        results = DataSkewRule().evaluate(job)
        assert results == []

    def test_detects_moderate_skew(self):
        stage = make_stage(median_duration_ms=100, max_duration_ms=800)
        job = make_job(stages=[stage])
        results = DataSkewRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING

    def test_detects_critical_skew(self):
        stage = make_stage(stage_id=4, median_duration_ms=12, max_duration_ms=340)
        job = make_job(stages=[stage])
        results = DataSkewRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.CRITICAL
        assert "Stage 4" in results[0].title

    def test_suggests_aqe_when_disabled(self):
        stage = make_stage(median_duration_ms=10, max_duration_ms=100)
        job = make_job(
            stages=[stage],
            config={"spark.sql.adaptive.enabled": "false"},
        )
        results = DataSkewRule().evaluate(job)
        assert len(results) == 1
        assert "AQE" in results[0].recommended_value


class TestSpillToDiskRule:
    def test_no_spill(self):
        stage = make_stage(spill_to_disk_bytes=0)
        job = make_job(stages=[stage])
        assert SpillToDiskRule().evaluate(job) == []

    def test_detects_spill(self):
        stage = make_stage(spill_to_disk_bytes=2 * 1024**3)
        job = make_job(stages=[stage])
        results = SpillToDiskRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.CRITICAL


class TestGCPressureRule:
    def test_no_gc_pressure(self):
        stage = make_stage(task_count=100, median_duration_ms=1000, total_gc_time_ms=5000)
        job = make_job(stages=[stage])
        assert GCPressureRule().evaluate(job) == []

    def test_detects_high_gc(self):
        stage = make_stage(task_count=100, median_duration_ms=1000, total_gc_time_ms=50_000)
        job = make_job(stages=[stage])
        results = GCPressureRule().evaluate(job)
        assert len(results) == 1
        assert "GC" in results[0].title


class TestShufflePartitionsRule:
    def test_optimal_partitions(self):
        stage = make_stage(total_shuffle_read_bytes=200 * 128 * 1024 * 1024)
        job = make_job(
            stages=[stage],
            config={"spark.sql.shuffle.partitions": "200"},
        )
        assert ShufflePartitionsRule().evaluate(job) == []

    def test_too_few_partitions(self):
        stage = make_stage(total_shuffle_read_bytes=800 * 128 * 1024 * 1024)
        job = make_job(
            stages=[stage],
            config={"spark.sql.shuffle.partitions": "200"},
        )
        results = ShufflePartitionsRule().evaluate(job)
        assert len(results) == 1
        assert "800" in results[0].recommended_value


class TestSpillToDiskSeverityLevels:
    def test_small_spill_is_info(self):
        stage = make_stage(spill_to_disk_bytes=50 * 1024**2)
        job = make_job(stages=[stage])
        results = SpillToDiskRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.INFO

    def test_medium_spill_is_warning(self):
        stage = make_stage(spill_to_disk_bytes=500 * 1024**2)
        job = make_job(stages=[stage])
        results = SpillToDiskRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING

    def test_large_spill_is_critical(self):
        stage = make_stage(spill_to_disk_bytes=2 * 1024**3)
        job = make_job(stages=[stage])
        results = SpillToDiskRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.CRITICAL


class TestTaskFailureRule:
    def test_no_failures(self):
        stage = make_stage()
        job = make_job(stages=[stage])
        assert TaskFailureRule().evaluate(job) == []

    def test_detects_failures(self):
        stage = make_stage(failed_task_count=3)
        job = make_job(stages=[stage])
        results = TaskFailureRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING
        assert "3 of 100" in results[0].message


class TestRunRules:
    def test_returns_sorted_by_severity(self):
        stages = [
            make_stage(
                stage_id=0,
                median_duration_ms=100,
                max_duration_ms=200,
                total_gc_time_ms=50_000,
                task_count=100,
            ),
            make_stage(
                stage_id=1,
                median_duration_ms=10,
                max_duration_ms=500,
                spill_to_disk_bytes=5 * 1024**3,
            ),
        ]
        job = make_job(stages=stages)
        results = StaticAnalysisService().analyze(job)
        severities = [r.severity for r in results]
        assert severities[0] == Severity.CRITICAL

    def test_empty_job(self):
        job = make_job(stages=[])
        results = StaticAnalysisService().analyze(job)
        assert results == []
