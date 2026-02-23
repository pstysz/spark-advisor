from spark_advisor.analysis.rules import (
    DataSkewRule,
    ExecutorIdleRule,
    GCPressureRule,
    ShufflePartitionsRule,
    SpillToDiskRule,
    TaskFailureRule,
)
from spark_advisor.analysis.static_analysis_service import StaticAnalysisService
from spark_advisor.model import Severity
from tests.factories import make_executors, make_job, make_stage


class TestDataSkewRule:
    def test_no_skew(self):
        stage = make_stage(run_time_median=100, run_time_max=200)
        job = make_job(stages=[stage])
        results = DataSkewRule().evaluate(job)
        assert results == []

    def test_detects_moderate_skew(self):
        stage = make_stage(run_time_median=100, run_time_max=800)
        job = make_job(stages=[stage])
        results = DataSkewRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING

    def test_detects_critical_skew(self):
        stage = make_stage(stage_id=4, run_time_median=12, run_time_max=340)
        job = make_job(stages=[stage])
        results = DataSkewRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.CRITICAL
        assert "Stage 4" in results[0].title

    def test_suggests_aqe_when_disabled(self):
        stage = make_stage(run_time_median=10, run_time_max=100)
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
        stage = make_stage(sum_executor_run_time_ms=100_000, total_gc_time_ms=5000)
        job = make_job(stages=[stage])
        assert GCPressureRule().evaluate(job) == []

    def test_detects_high_gc(self):
        stage = make_stage(sum_executor_run_time_ms=100_000, total_gc_time_ms=50_000)
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


class TestExecutorIdleRule:
    def test_no_executors(self):
        job = make_job(executors=None)
        assert ExecutorIdleRule().evaluate(job) == []

    def test_high_utilization_no_warning(self):
        # 10 executors x 4 cores x 300_000ms = 12_000_000 total slot time
        # 8_000_000 task time = 66.7% utilization → above 40% threshold
        job = make_job(
            executors=make_executors(total_task_time_ms=8_000_000),
            config={"spark.executor.cores": "4"},
        )
        assert ExecutorIdleRule().evaluate(job) == []

    def test_low_utilization_triggers_warning(self):
        # 10 executors x 4 cores x 300_000ms = 12_000_000 total slot time
        # 1_000_000 task time = 8.3% utilization → below 40% threshold
        job = make_job(
            executors=make_executors(total_task_time_ms=1_000_000),
            config={"spark.executor.cores": "4"},
        )
        results = ExecutorIdleRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING
        assert "8%" in results[0].message

    def test_zero_task_time_skips(self):
        job = make_job(
            executors=make_executors(total_task_time_ms=0),
        )
        assert ExecutorIdleRule().evaluate(job) == []

    def test_default_cores_one(self):
        # No spark.executor.cores set → defaults to 1
        # 10 executors x 1 core x 300_000ms = 3_000_000 total slot time
        # 500_000 task time = 16.7% → below 40%
        job = make_job(
            executors=make_executors(total_task_time_ms=500_000),
            config={"spark.executor.memory": "4g"},
        )
        results = ExecutorIdleRule().evaluate(job)
        assert len(results) == 1


class TestRunRules:
    def test_returns_sorted_by_severity(self):
        stages = [
            make_stage(
                stage_id=0,
                run_time_median=100,
                run_time_max=200,
                sum_executor_run_time_ms=10_000,
                total_gc_time_ms=5_000,
                task_count=100,
            ),
            make_stage(
                stage_id=1,
                run_time_median=10,
                run_time_max=500,
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
