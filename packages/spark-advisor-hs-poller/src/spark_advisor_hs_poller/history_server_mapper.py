from typing import Any

from spark_advisor_shared.model.metrics import (
    ExecutorMetrics,
    IOQuantiles,
    JobAnalysis,
    Quantiles,
    ShuffleReadQuantiles,
    ShuffleWriteQuantiles,
    StageMetrics,
    TaskMetrics,
    TaskMetricsDistributions,
)
from spark_advisor_shared.model.spark_config import SparkConfig
from spark_advisor_shared.util.stat_helper import quantiles_5


def map_job_analysis(
    app_id: str,
    app_info: dict[str, Any],
    environment: dict[str, Any],
    stages_data: list[dict[str, Any]],
    task_summaries: dict[int, dict[str, Any]],
    executors_data: list[dict[str, Any]],
) -> JobAnalysis:
    attempts = app_info.get("attempts", [])
    latest = attempts[-1] if attempts else {}

    config = _map_environment(environment)
    spark_version = latest.get("appSparkVersion", "")
    stages = _map_stages(stages_data, task_summaries)
    executors = _map_executors(executors_data)

    return JobAnalysis(
        app_id=app_id,
        app_name=app_info.get("name", ""),
        spark_version=spark_version,
        duration_ms=latest.get("duration", 0),
        config=config,
        stages=stages,
        executors=executors,
    )


def _map_environment(env: dict[str, Any]) -> SparkConfig:
    spark_props: dict[str, str] = {}
    for prop in env.get("sparkProperties", []):
        if len(prop) >= 2:
            spark_props[prop[0]] = prop[1]

    return SparkConfig(raw=spark_props)


def _map_stages(
    stages_data: list[dict[str, Any]],
    task_summaries: dict[int, dict[str, Any]],
) -> list[StageMetrics]:
    stages: list[StageMetrics] = []
    for stage_data in stages_data:
        stage_id = int(stage_data["stageId"])
        task_summary = task_summaries.get(stage_id, {})

        tasks = TaskMetrics(
            task_count=stage_data.get("numTasks", 0),
            distributions=_map_task_distributions(task_summary),
        )

        stages.append(
            StageMetrics(
                stage_id=stage_id,
                stage_name=stage_data.get("name", f"Stage {stage_id}"),
                sum_executor_run_time_ms=stage_data.get("executorRunTime", 0),
                total_gc_time_ms=stage_data.get("jvmGcTime", 0),
                total_shuffle_read_bytes=stage_data.get("shuffleReadBytes", 0),
                total_shuffle_write_bytes=stage_data.get("shuffleWriteBytes", 0),
                spill_to_disk_bytes=stage_data.get("diskBytesSpilled", 0),
                spill_to_memory_bytes=stage_data.get("memoryBytesSpilled", 0),
                failed_task_count=stage_data.get("numFailedTasks", 0),
                input_bytes=stage_data.get("inputBytes", 0),
                input_records=stage_data.get("inputRecords", 0),
                output_bytes=stage_data.get("outputBytes", 0),
                output_records=stage_data.get("outputRecords", 0),
                shuffle_read_records=stage_data.get("shuffleReadRecords", 0),
                shuffle_write_records=stage_data.get("shuffleWriteRecords", 0),
                killed_task_count=stage_data.get("numKilledTasks", 0),
                tasks=tasks,
            )
        )
    return stages


def _q(obj: dict[str, Any], key: str) -> Quantiles:
    vals = obj.get(key, [])
    mn, p25, med, p75, mx = quantiles_5(vals)
    return Quantiles(min=mn, p25=p25, median=med, p75=p75, max=mx)


def _q_nanos_to_ms(obj: dict[str, Any], key: str) -> Quantiles:
    """Converts nanosecond values from Spark API to milliseconds."""
    vals = obj.get(key, [])
    converted = [v / 1_000_000 for v in vals] if vals else []
    mn, p25, med, p75, mx = quantiles_5(converted)
    return Quantiles(min=mn, p25=p25, median=med, p75=p75, max=mx)


def _map_task_distributions(task_summary: dict[str, Any]) -> TaskMetricsDistributions:
    if not task_summary:
        return TaskMetricsDistributions()

    input_metrics = task_summary.get("inputMetrics", {}) or {}
    output_metrics = task_summary.get("outputMetrics", {}) or {}
    shuffle_read = task_summary.get("shuffleReadMetrics", {}) or {}
    shuffle_write = task_summary.get("shuffleWriteMetrics", {}) or {}

    return TaskMetricsDistributions(
        duration=_q(task_summary, "duration"),
        executor_deserialize_time=_q(task_summary, "executorDeserializeTime"),
        executor_deserialize_cpu_time=_q_nanos_to_ms(task_summary, "executorDeserializeCpuTime"),
        executor_run_time=_q(task_summary, "executorRunTime"),
        executor_cpu_time=_q_nanos_to_ms(task_summary, "executorCpuTime"),
        result_size=_q(task_summary, "resultSize"),
        jvm_gc_time=_q(task_summary, "jvmGcTime"),
        result_serialization_time=_q(task_summary, "resultSerializationTime"),
        getting_result_time=_q(task_summary, "gettingResultTime"),
        scheduler_delay=_q(task_summary, "schedulerDelay"),
        peak_execution_memory=_q(task_summary, "peakExecutionMemory"),
        memory_bytes_spilled=_q(task_summary, "memoryBytesSpilled"),
        disk_bytes_spilled=_q(task_summary, "diskBytesSpilled"),
        input_metrics=IOQuantiles(
            bytes=_q(input_metrics, "bytesRead"),
            records=_q(input_metrics, "recordsRead"),
        ),
        output_metrics=IOQuantiles(
            bytes=_q(output_metrics, "bytesWritten"),
            records=_q(output_metrics, "recordsWritten"),
        ),
        shuffle_read_metrics=ShuffleReadQuantiles(
            read_bytes=_q(shuffle_read, "readBytes"),
            read_records=_q(shuffle_read, "readRecords"),
            remote_blocks_fetched=_q(shuffle_read, "remoteBlocksFetched"),
            local_blocks_fetched=_q(shuffle_read, "localBlocksFetched"),
            total_blocks_fetched=_q(shuffle_read, "totalBlocksFetched"),
            fetch_wait_time=_q(shuffle_read, "fetchWaitTime"),
            remote_bytes_read=_q(shuffle_read, "remoteBytesRead"),
            remote_bytes_read_to_disk=_q(shuffle_read, "remoteBytesReadToDisk"),
        ),
        shuffle_write_metrics=ShuffleWriteQuantiles(
            write_bytes=_q(shuffle_write, "writeBytes"),
            write_records=_q(shuffle_write, "writeRecords"),
            write_time=_q_nanos_to_ms(shuffle_write, "writeTime"),
        ),
    )


def _map_executors(executors_data: list[dict[str, Any]]) -> ExecutorMetrics:
    non_driver = [e for e in executors_data if e.get("id") != "driver"]
    return ExecutorMetrics(
        executor_count=len(non_driver),
        peak_memory_bytes_sum=sum(
            (e.get("peakMemoryMetrics") or {}).get("JVMHeapMemory", 0) for e in non_driver
        ),
        allocated_memory_bytes_sum=sum(e.get("maxMemory", 0) for e in non_driver),
        total_task_time_ms=sum(e.get("totalDuration", 0) for e in non_driver),
        total_gc_time_ms=sum(e.get("totalGCTime", 0) for e in non_driver),
        total_shuffle_read_bytes=sum(e.get("totalShuffleRead", 0) for e in non_driver),
        total_shuffle_write_bytes=sum(e.get("totalShuffleWrite", 0) for e in non_driver),
        failed_tasks=sum(e.get("failedTasks", 0) for e in non_driver),
        total_cores=sum(e.get("totalCores", 0) for e in non_driver),
    )
