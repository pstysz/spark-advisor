from spark_advisor_models.model import Severity
from spark_advisor_models.testing import make_executors, make_job, make_stage
from spark_advisor_rules import StaticAnalysisService
from spark_advisor_rules.rules import (
    AQENotEnabledRule,
    BroadcastJoinThresholdRule,
    DataSkewRule,
    DriverMemoryRule,
    DynamicAllocationRule,
    ExcessiveStagesRule,
    ExecutorIdleRule,
    ExecutorMemoryOverheadRule,
    GCPressureRule,
    InputDataSkewRule,
    MemoryUnderutilizationRule,
    SerializerChoiceRule,
    ShuffleDataVolumeRule,
    ShufflePartitionsRule,
    SmallFileRule,
    SpillToDiskRule,
    TaskFailureRule,
)


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

    def test_detects_failures_warning(self):
        stage = make_stage(failed_task_count=3)
        job = make_job(stages=[stage])
        results = TaskFailureRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING
        assert "3 of 100" in results[0].message

    def test_detects_failures_critical(self):
        stage = make_stage(failed_task_count=15)
        job = make_job(stages=[stage])
        results = TaskFailureRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.CRITICAL
        assert "15 of 100" in results[0].message


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

    def test_low_utilization_triggers_critical(self):
        # 10 executors x 4 cores x 300_000ms = 12_000_000 total slot time
        # 1_000_000 task time = 8.3% utilization → below 20% critical threshold
        job = make_job(
            executors=make_executors(total_task_time_ms=1_000_000),
            config={"spark.executor.cores": "4"},
        )
        results = ExecutorIdleRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.CRITICAL
        assert "8%" in results[0].message

    def test_moderate_low_utilization_triggers_warning(self):
        # 10 executors x 4 cores x 300_000ms = 12_000_000 total slot time
        # 3_600_000 task time = 30% utilization → below 40% but above 20%
        job = make_job(
            executors=make_executors(total_task_time_ms=3_600_000),
            config={"spark.executor.cores": "4"},
        )
        results = ExecutorIdleRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING

    def test_zero_task_time_reports_zero_utilization(self):
        job = make_job(
            executors=make_executors(total_task_time_ms=0),
        )
        results = ExecutorIdleRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.CRITICAL
        assert "0%" in results[0].message

    def test_default_cores_one(self):
        # No spark.executor.cores set → defaults to 1
        # 10 executors x 1 core x 300_000ms = 3_000_000 total slot time
        # 500_000 task time = 16.7% → below 20% critical threshold
        job = make_job(
            executors=make_executors(total_task_time_ms=500_000),
            config={"spark.executor.memory": "4g"},
        )
        results = ExecutorIdleRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.CRITICAL


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


class TestSmallFileRule:
    def test_normal_input_size(self):
        stage = make_stage(input_bytes=200 * 1024 * 1024, task_count=10)
        job = make_job(stages=[stage])
        assert SmallFileRule().evaluate(job) == []

    def test_detects_small_files_warning(self):
        # 50MB / 100 tasks = 0.5MB per task → below 10MB threshold but above 1MB
        stage = make_stage(input_bytes=500 * 1024 * 1024, task_count=100)
        job = make_job(stages=[stage])
        results = SmallFileRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING
        assert results[0].rule_id == "small_files"

    def test_detects_tiny_files_critical(self):
        # 50MB / 100 tasks = 0.5MB per task → below 1MB critical threshold
        stage = make_stage(input_bytes=50 * 1024 * 1024, task_count=100)
        job = make_job(stages=[stage])
        results = SmallFileRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.CRITICAL

    def test_skips_zero_input(self):
        stage = make_stage(input_bytes=0, task_count=100)
        job = make_job(stages=[stage])
        assert SmallFileRule().evaluate(job) == []

    def test_skips_zero_tasks(self):
        stage = make_stage(input_bytes=100 * 1024 * 1024, task_count=0)
        job = make_job(stages=[stage])
        assert SmallFileRule().evaluate(job) == []


class TestBroadcastJoinThresholdRule:
    def test_default_threshold_no_issue(self):
        stage = make_stage(total_shuffle_read_bytes=0)
        job = make_job(stages=[stage])
        assert BroadcastJoinThresholdRule().evaluate(job) == []

    def test_disabled_with_shuffle_is_warning(self):
        stage = make_stage(total_shuffle_read_bytes=500 * 1024 * 1024)
        job = make_job(stages=[stage], config={"spark.sql.autoBroadcastJoinThreshold": "-1"})
        results = BroadcastJoinThresholdRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING

    def test_disabled_without_shuffle_is_info(self):
        stage = make_stage(total_shuffle_read_bytes=0)
        job = make_job(stages=[stage], config={"spark.sql.autoBroadcastJoinThreshold": "-1"})
        results = BroadcastJoinThresholdRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.INFO

    def test_low_threshold_with_shuffle(self):
        stage = make_stage(total_shuffle_read_bytes=500 * 1024 * 1024)
        job = make_job(stages=[stage], config={"spark.sql.autoBroadcastJoinThreshold": "1048576"})
        results = BroadcastJoinThresholdRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.INFO


class TestSerializerChoiceRule:
    def test_kryo_no_warning(self):
        stage = make_stage(total_shuffle_read_bytes=100 * 1024 * 1024)
        job = make_job(
            stages=[stage],
            config={"spark.serializer": "org.apache.spark.serializer.KryoSerializer"},
        )
        assert SerializerChoiceRule().evaluate(job) == []

    def test_java_serializer_with_shuffle(self):
        stage = make_stage(total_shuffle_read_bytes=100 * 1024 * 1024)
        job = make_job(stages=[stage], config={})
        results = SerializerChoiceRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.INFO


class TestAQENotEnabledRule:
    def test_aqe_enabled_no_finding(self) -> None:
        job = make_job(
            spark_version="3.4.1",
            config={"spark.sql.adaptive.enabled": "true"},
        )
        assert AQENotEnabledRule().evaluate(job) == []

    def test_spark2_skip(self) -> None:
        job = make_job(
            spark_version="2.4.8",
            config={"spark.sql.adaptive.enabled": "false"},
        )
        assert AQENotEnabledRule().evaluate(job) == []

    def test_spark30_aqe_off_info(self) -> None:
        job = make_job(
            spark_version="3.0.3",
            config={"spark.sql.adaptive.enabled": "false"},
        )
        results = AQENotEnabledRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.INFO
        assert results[0].rule_id == "aqe_not_enabled"

    def test_spark34_aqe_off_warning(self) -> None:
        job = make_job(
            spark_version="3.4.1",
            config={"spark.sql.adaptive.enabled": "false"},
        )
        results = AQENotEnabledRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING

    def test_spark32_aqe_absent_skip(self) -> None:
        """Spark 3.2+ with no explicit AQE config = AQE is on by default."""
        job = make_job(spark_version="3.2.0", config={})
        assert AQENotEnabledRule().evaluate(job) == []

    def test_unparseable_version_skip(self) -> None:
        job = make_job(
            spark_version="custom-build",
            config={"spark.sql.adaptive.enabled": "false"},
        )
        assert AQENotEnabledRule().evaluate(job) == []

    def test_java_serializer_without_shuffle(self):
        stage = make_stage(total_shuffle_read_bytes=0, total_shuffle_write_bytes=0)
        job = make_job(stages=[stage], config={})
        assert SerializerChoiceRule().evaluate(job) == []


class TestDynamicAllocationRule:
    def test_enabled_with_bounds(self):
        job = make_job(
            config={
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.minExecutors": "2",
                "spark.dynamicAllocation.maxExecutors": "20",
            }
        )
        assert DynamicAllocationRule().evaluate(job) == []

    def test_enabled_without_bounds(self):
        job = make_job(config={"spark.dynamicAllocation.enabled": "true"})
        results = DynamicAllocationRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING
        assert "minExecutors" in results[0].message

    def test_disabled_with_idle_executors(self):
        job = make_job(
            executors=make_executors(total_task_time_ms=1_000_000),
            config={"spark.executor.cores": "4"},
        )
        results = DynamicAllocationRule().evaluate(job)
        assert len(results) == 1
        assert "dynamicAllocation" in results[0].recommended_value

    def test_disabled_high_utilization_no_warning(self):
        job = make_job(
            executors=make_executors(total_task_time_ms=8_000_000),
            config={"spark.executor.cores": "4"},
        )
        assert DynamicAllocationRule().evaluate(job) == []

    def test_disabled_no_executors(self):
        job = make_job(executors=None, config={})
        assert DynamicAllocationRule().evaluate(job) == []


class TestExecutorMemoryOverheadRule:
    def test_no_executor_data(self):
        job = make_job(executors=None)
        assert ExecutorMemoryOverheadRule().evaluate(job) == []

    def test_gc_pressure_and_high_memory(self):
        stage = make_stage(sum_executor_run_time_ms=100_000, total_gc_time_ms=30_000)
        job = make_job(
            stages=[stage],
            executors=make_executors(
                peak_memory_bytes_sum=9 * 1024**3,
                allocated_memory_bytes_sum=10 * 1024**3,
            ),
        )
        results = ExecutorMemoryOverheadRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING

    def test_gc_pressure_low_memory_no_warning(self):
        stage = make_stage(sum_executor_run_time_ms=100_000, total_gc_time_ms=30_000)
        job = make_job(
            stages=[stage],
            executors=make_executors(
                peak_memory_bytes_sum=2 * 1024**3,
                allocated_memory_bytes_sum=10 * 1024**3,
            ),
        )
        assert ExecutorMemoryOverheadRule().evaluate(job) == []

    def test_high_memory_no_gc_no_warning(self):
        stage = make_stage(sum_executor_run_time_ms=100_000, total_gc_time_ms=5_000)
        job = make_job(
            stages=[stage],
            executors=make_executors(
                peak_memory_bytes_sum=9 * 1024**3,
                allocated_memory_bytes_sum=10 * 1024**3,
            ),
        )
        assert ExecutorMemoryOverheadRule().evaluate(job) == []


class TestDriverMemoryRule:
    def test_memory_in_range_no_finding(self) -> None:
        job = make_job(config={"spark.driver.memory": "1g"})
        assert DriverMemoryRule().evaluate(job) == []

    def test_memory_too_low_warning(self) -> None:
        job = make_job(config={"spark.driver.memory": "256m"})
        results = DriverMemoryRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING
        assert results[0].rule_id == "driver_memory"
        assert "too low" in results[0].message.lower()

    def test_memory_too_high_warning(self) -> None:
        job = make_job(config={"spark.driver.memory": "16g"})
        results = DriverMemoryRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING
        assert "too high" in results[0].message.lower()

    def test_default_memory_no_finding(self) -> None:
        job = make_job(config={})
        assert DriverMemoryRule().evaluate(job) == []


class TestMemoryUnderutilizationRule:
    def test_good_utilization_no_finding(self) -> None:
        executors = make_executors(
            peak_memory_bytes_sum=3 * 1024**3,
            allocated_memory_bytes_sum=4 * 1024**3,
        )
        job = make_job(executors=executors)
        assert MemoryUnderutilizationRule().evaluate(job) == []

    def test_low_utilization_warning(self) -> None:
        executors = make_executors(
            peak_memory_bytes_sum=1 * 1024**3,
            allocated_memory_bytes_sum=4 * 1024**3,
        )
        job = make_job(executors=executors)
        results = MemoryUnderutilizationRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING
        assert results[0].rule_id == "memory_underutilization"

    def test_no_executors_skip(self) -> None:
        job = make_job(executors=None)
        assert MemoryUnderutilizationRule().evaluate(job) == []

    def test_zero_allocated_skip(self) -> None:
        executors = make_executors(
            peak_memory_bytes_sum=0,
            allocated_memory_bytes_sum=0,
        )
        job = make_job(executors=executors)
        assert MemoryUnderutilizationRule().evaluate(job) == []


class TestExcessiveStagesRule:
    def test_few_stages_no_finding(self) -> None:
        stages = [make_stage(i) for i in range(10)]
        job = make_job(stages=stages)
        assert ExcessiveStagesRule().evaluate(job) == []

    def test_excessive_stages_warning(self) -> None:
        stages = [make_stage(i) for i in range(60)]
        job = make_job(stages=stages)
        results = ExcessiveStagesRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING
        assert results[0].rule_id == "excessive_stages"
        assert "60" in results[0].message

    def test_exactly_threshold_no_finding(self) -> None:
        stages = [make_stage(i) for i in range(50)]
        job = make_job(stages=stages)
        assert ExcessiveStagesRule().evaluate(job) == []

    def test_empty_stages_no_finding(self) -> None:
        job = make_job(stages=[])
        assert ExcessiveStagesRule().evaluate(job) == []


class TestShuffleDataVolumeRule:
    def test_small_shuffle_no_finding(self) -> None:
        stage = make_stage(0, total_shuffle_write_bytes=1 * 1024**3)
        job = make_job(stages=[stage])
        assert ShuffleDataVolumeRule().evaluate(job) == []

    def test_large_shuffle_warning(self) -> None:
        stage = make_stage(0, total_shuffle_write_bytes=15 * 1024**3)
        job = make_job(stages=[stage])
        results = ShuffleDataVolumeRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING
        assert results[0].rule_id == "shuffle_data_volume"
        assert "15.0" in results[0].message
        assert results[0].stage_id == 0

    def test_zero_shuffle_no_finding(self) -> None:
        stage = make_stage(0, total_shuffle_write_bytes=0)
        job = make_job(stages=[stage])
        assert ShuffleDataVolumeRule().evaluate(job) == []

    def test_multiple_stages_detects_each(self) -> None:
        s1 = make_stage(0, total_shuffle_write_bytes=15 * 1024**3)
        s2 = make_stage(1, total_shuffle_write_bytes=20 * 1024**3)
        job = make_job(stages=[s1, s2])
        results = ShuffleDataVolumeRule().evaluate(job)
        assert len(results) == 2


class TestInputDataSkewRule:
    def test_no_skew_no_finding(self) -> None:
        stage = make_stage(0, input_bytes_median=100 * 1024**2, input_bytes_max=150 * 1024**2)
        job = make_job(stages=[stage])
        assert InputDataSkewRule().evaluate(job) == []

    def test_warning_skew(self) -> None:
        stage = make_stage(0, input_bytes_median=100 * 1024**2, input_bytes_max=700 * 1024**2)
        job = make_job(stages=[stage])
        results = InputDataSkewRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING
        assert results[0].rule_id == "input_data_skew"

    def test_critical_skew(self) -> None:
        stage = make_stage(0, input_bytes_median=100 * 1024**2, input_bytes_max=1500 * 1024**2)
        job = make_job(stages=[stage])
        results = InputDataSkewRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.CRITICAL

    def test_zero_median_skip(self) -> None:
        stage = make_stage(0, input_bytes_median=0, input_bytes_max=0)
        job = make_job(stages=[stage])
        assert InputDataSkewRule().evaluate(job) == []

    def test_no_input_metrics_skip(self) -> None:
        stage = make_stage(0)
        job = make_job(stages=[stage])
        assert InputDataSkewRule().evaluate(job) == []
