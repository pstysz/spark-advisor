from typing import Any

from spark_advisor_hs_poller.history_server_mapper import map_job_analysis

APP_INFO: dict[str, Any] = {
    "id": "app-001",
    "name": "TestETL",
    "attempts": [
        {
            "startTime": "2026-01-01T00:00:00.000GMT",
            "endTime": "2026-01-01T00:05:00.000GMT",
            "duration": 300000,
            "sparkUser": "user",
            "completed": True,
            "appSparkVersion": "3.5.0",
        }
    ],
}

ENVIRONMENT: dict[str, Any] = {
    "sparkProperties": [
        ["spark.executor.memory", "4g"],
        ["spark.executor.cores", "4"],
        ["spark.sql.shuffle.partitions", "200"],
    ],
    "runtime": {"sparkVersion": "3.5.0"},
}

STAGES: list[dict[str, Any]] = [
    {
        "stageId": 0,
        "name": "Stage 0",
        "numTasks": 100,
        "executorRunTime": 50000,
        "jvmGcTime": 2000,
        "inputBytes": 1_000_000,
        "outputBytes": 500_000,
        "shuffleReadBytes": 200_000,
        "shuffleWriteBytes": 100_000,
        "diskBytesSpilled": 1024,
        "memoryBytesSpilled": 2048,
        "numFailedTasks": 3,
        "inputRecords": 10_000,
        "outputRecords": 5_000,
        "shuffleReadRecords": 2_000,
        "shuffleWriteRecords": 1_000,
        "numKilledTasks": 1,
    }
]

TASK_SUMMARIES: dict[int, dict[str, Any]] = {
    0: {
        "duration": [200, 400, 600, 800, 1200],
        "executorRunTime": [100, 300, 500, 700, 900],
        "executorCpuTime": [50_000_000, 150_000_000, 250_000_000, 350_000_000, 450_000_000],
        "executorDeserializeCpuTime": [10_000_000, 20_000_000, 30_000_000, 40_000_000, 50_000_000],
        "jvmGcTime": [10, 20, 30, 40, 50],
        "inputMetrics": {
            "bytesRead": [1000, 2000, 3000, 4000, 5000],
            "recordsRead": [10, 20, 30, 40, 50],
        },
        "outputMetrics": {
            "bytesWritten": [500, 1000, 1500, 2000, 2500],
            "recordsWritten": [5, 10, 15, 20, 25],
        },
        "shuffleReadMetrics": {
            "readBytes": [100, 200, 300, 400, 500],
            "readRecords": [1, 2, 3, 4, 5],
        },
        "shuffleWriteMetrics": {
            "writeBytes": [50, 100, 150, 200, 250],
            "writeRecords": [1, 2, 3, 4, 5],
            "writeTime": [10_000_000, 20_000_000, 30_000_000, 40_000_000, 50_000_000],
        },
    }
}

EXECUTORS: list[dict[str, Any]] = [
    {"id": "driver", "maxMemory": 1073741824, "totalDuration": 5000, "totalGCTime": 100},
    {
        "id": "1",
        "maxMemory": 4294967296,
        "peakMemoryMetrics": {"JVMHeapMemory": 2147483648},
        "totalDuration": 120000,
        "totalGCTime": 3000,
        "totalShuffleRead": 500_000,
        "totalShuffleWrite": 200_000,
        "failedTasks": 2,
        "totalCores": 4,
    },
    {
        "id": "2",
        "maxMemory": 4294967296,
        "peakMemoryMetrics": {"JVMHeapMemory": 1073741824},
        "totalDuration": 80000,
        "totalGCTime": 1000,
        "totalShuffleRead": 300_000,
        "totalShuffleWrite": 100_000,
        "failedTasks": 0,
        "totalCores": 4,
    },
]


def _map_full() -> Any:
    return map_job_analysis(
        app_id="app-001",
        app_info=APP_INFO,
        environment=ENVIRONMENT,
        stages_data=STAGES,
        task_summaries=TASK_SUMMARIES,
        executors_data=EXECUTORS,
    )


class TestMapJobAnalysis:
    def test_top_level_fields(self) -> None:
        job = _map_full()
        assert job.app_id == "app-001"
        assert job.app_name == "TestETL"
        assert job.spark_version == "3.5.0"
        assert job.duration_ms == 300000

    def test_config_parsing(self) -> None:
        job = _map_full()
        assert job.config.executor_memory == "4g"
        assert job.config.executor_cores == 4
        assert job.config.shuffle_partitions == 200

    def test_stage_totals(self) -> None:
        job = _map_full()
        assert len(job.stages) == 1
        stage = job.stages[0]
        assert stage.stage_id == 0
        assert stage.stage_name == "Stage 0"
        assert stage.sum_executor_run_time_ms == 50000
        assert stage.total_gc_time_ms == 2000
        assert stage.input_bytes == 1_000_000
        assert stage.output_bytes == 500_000
        assert stage.total_shuffle_read_bytes == 200_000
        assert stage.total_shuffle_write_bytes == 100_000
        assert stage.spill_to_disk_bytes == 1024
        assert stage.spill_to_memory_bytes == 2048
        assert stage.failed_task_count == 3
        assert stage.input_records == 10_000
        assert stage.output_records == 5_000
        assert stage.shuffle_read_records == 2_000
        assert stage.shuffle_write_records == 1_000
        assert stage.killed_task_count == 1

    def test_task_count(self) -> None:
        job = _map_full()
        assert job.stages[0].tasks.task_count == 100

    def test_duration_distribution(self) -> None:
        job = _map_full()
        dist = job.stages[0].tasks.distributions
        assert dist.duration.min == 200
        assert dist.duration.median == 600
        assert dist.duration.max == 1200

    def test_duration_skew_ratio_uses_executor_run_time(self) -> None:
        job = _map_full()
        assert job.stages[0].tasks.duration_skew_ratio == 900 / 500

    def test_task_distributions_executor_run_time(self) -> None:
        job = _map_full()
        dist = job.stages[0].tasks.distributions
        assert dist.executor_run_time.min == 100
        assert dist.executor_run_time.p25 == 300
        assert dist.executor_run_time.median == 500
        assert dist.executor_run_time.p75 == 700
        assert dist.executor_run_time.max == 900

    def test_nanos_to_ms_executor_cpu_time(self) -> None:
        job = _map_full()
        dist = job.stages[0].tasks.distributions
        assert dist.executor_cpu_time.min == 50
        assert dist.executor_cpu_time.median == 250
        assert dist.executor_cpu_time.max == 450

    def test_nanos_to_ms_executor_deserialize_cpu_time(self) -> None:
        job = _map_full()
        dist = job.stages[0].tasks.distributions
        assert dist.executor_deserialize_cpu_time.min == 10
        assert dist.executor_deserialize_cpu_time.median == 30
        assert dist.executor_deserialize_cpu_time.max == 50

    def test_nanos_to_ms_shuffle_write_time(self) -> None:
        job = _map_full()
        sw = job.stages[0].tasks.distributions.shuffle_write_metrics
        assert sw.write_time.min == 10
        assert sw.write_time.median == 30
        assert sw.write_time.max == 50

    def test_task_distributions_gc_time(self) -> None:
        job = _map_full()
        dist = job.stages[0].tasks.distributions
        assert dist.jvm_gc_time.min == 10
        assert dist.jvm_gc_time.median == 30
        assert dist.jvm_gc_time.max == 50

    def test_task_distributions_input_metrics(self) -> None:
        job = _map_full()
        inp = job.stages[0].tasks.distributions.input_metrics
        assert inp.bytes.min == 1000
        assert inp.bytes.median == 3000
        assert inp.bytes.max == 5000
        assert inp.records.min == 10
        assert inp.records.max == 50

    def test_task_distributions_shuffle_read(self) -> None:
        job = _map_full()
        sr = job.stages[0].tasks.distributions.shuffle_read_metrics
        assert sr.read_bytes.min == 100
        assert sr.read_bytes.max == 500
        assert sr.read_records.median == 3

    def test_task_distributions_shuffle_write_bytes(self) -> None:
        job = _map_full()
        sw = job.stages[0].tasks.distributions.shuffle_write_metrics
        assert sw.write_bytes.min == 50
        assert sw.write_bytes.max == 250

    def test_executor_metrics(self) -> None:
        job = _map_full()
        assert job.executors is not None
        assert job.executors.executor_count == 2
        assert job.executors.peak_memory_bytes_sum == 2147483648 + 1073741824
        assert job.executors.allocated_memory_bytes_sum == 4294967296 * 2
        assert job.executors.total_task_time_ms == 120000 + 80000
        assert job.executors.total_gc_time_ms == 3000 + 1000
        assert job.executors.total_shuffle_read_bytes == 500_000 + 300_000
        assert job.executors.total_shuffle_write_bytes == 200_000 + 100_000
        assert job.executors.failed_tasks == 2
        assert job.executors.total_cores == 8

    def test_executors_exclude_driver(self) -> None:
        job = _map_full()
        assert job.executors is not None
        assert job.executors.executor_count == 2

    def test_empty_task_summary_returns_defaults(self) -> None:
        job = map_job_analysis(
            app_id="app-002",
            app_info=APP_INFO,
            environment=ENVIRONMENT,
            stages_data=STAGES,
            task_summaries={},
            executors_data=EXECUTORS,
        )
        dist = job.stages[0].tasks.distributions
        assert dist.executor_run_time.min == 0
        assert dist.executor_run_time.max == 0
        assert dist.duration.min == 0

    def test_missing_attempts_uses_zero_duration(self) -> None:
        app_info: dict[str, Any] = {"name": "NoAttempts"}
        job = map_job_analysis(
            app_id="app-003",
            app_info=app_info,
            environment=ENVIRONMENT,
            stages_data=[],
            task_summaries={},
            executors_data=[],
        )
        assert job.duration_ms == 0
        assert job.app_name == "NoAttempts"

    def test_empty_environment(self) -> None:
        job = map_job_analysis(
            app_id="app-004",
            app_info=APP_INFO,
            environment={},
            stages_data=[],
            task_summaries={},
            executors_data=[],
        )
        assert job.spark_version == "3.5.0"
        assert job.config.raw == {}

    def test_spark_version_from_app_info_not_environment(self) -> None:
        env_without_runtime: dict[str, Any] = {
            "sparkProperties": [["spark.executor.memory", "4g"]],
        }
        job = map_job_analysis(
            app_id="app-005",
            app_info=APP_INFO,
            environment=env_without_runtime,
            stages_data=[],
            task_summaries={},
            executors_data=[],
        )
        assert job.spark_version == "3.5.0"

    def test_spark_version_empty_without_attempts(self) -> None:
        app_info: dict[str, Any] = {"name": "NoAttempts"}
        job = map_job_analysis(
            app_id="app-006",
            app_info=app_info,
            environment=ENVIRONMENT,
            stages_data=[],
            task_summaries={},
            executors_data=[],
        )
        assert job.spark_version == ""

    def test_gc_time_percent(self) -> None:
        job = _map_full()
        stage = job.stages[0]
        expected = (2000 / 50000) * 100
        assert stage.gc_time_percent == expected

    def test_skew_ratio(self) -> None:
        job = _map_full()
        dist = job.stages[0].tasks.distributions
        assert dist.executor_run_time.skew_ratio == 900 / 500

    def test_memory_utilization_percent(self) -> None:
        job = _map_full()
        assert job.executors is not None
        peak = 2147483648 + 1073741824
        allocated = 4294967296 * 2
        assert job.executors.memory_utilization_percent == (peak / allocated) * 100


class TestNullSafety:
    def test_peak_memory_metrics_null(self) -> None:
        executors = [
            {"id": "1", "maxMemory": 4294967296, "peakMemoryMetrics": None},
            {"id": "2", "maxMemory": 4294967296},
        ]
        job = map_job_analysis(
            app_id="app-null",
            app_info=APP_INFO,
            environment=ENVIRONMENT,
            stages_data=[],
            task_summaries={},
            executors_data=executors,
        )
        assert job.executors is not None
        assert job.executors.peak_memory_bytes_sum == 0
        assert job.executors.allocated_memory_bytes_sum == 4294967296 * 2

    def test_total_duration_missing_defaults_to_zero(self) -> None:
        executors = [
            {"id": "1", "maxMemory": 4294967296},
            {"id": "2", "maxMemory": 4294967296},
        ]
        job = map_job_analysis(
            app_id="app-no-duration",
            app_info=APP_INFO,
            environment=ENVIRONMENT,
            stages_data=[],
            task_summaries={},
            executors_data=executors,
        )
        assert job.executors is not None
        assert job.executors.total_task_time_ms == 0

    def test_runtime_null_in_environment_still_gets_version(self) -> None:
        env: dict[str, Any] = {
            "sparkProperties": [["spark.executor.memory", "4g"]],
            "runtime": None,
        }
        job = map_job_analysis(
            app_id="app-null-runtime",
            app_info=APP_INFO,
            environment=env,
            stages_data=[],
            task_summaries={},
            executors_data=[],
        )
        assert job.spark_version == "3.5.0"

    def test_null_nested_metrics_in_task_summary(self) -> None:
        task_summaries: dict[int, dict[str, Any]] = {
            0: {
                "executorRunTime": [100, 200, 300, 400, 500],
                "inputMetrics": None,
                "shuffleWriteMetrics": None,
            }
        }
        job = map_job_analysis(
            app_id="app-null-metrics",
            app_info=APP_INFO,
            environment=ENVIRONMENT,
            stages_data=STAGES,
            task_summaries=task_summaries,
            executors_data=[],
        )
        dist = job.stages[0].tasks.distributions
        assert dist.input_metrics.bytes.min == 0
        assert dist.shuffle_write_metrics.write_bytes.min == 0
