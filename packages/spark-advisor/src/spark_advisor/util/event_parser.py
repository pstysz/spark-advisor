import gzip
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import orjson

from spark_advisor.model import SparkConfig
from spark_advisor.model.metrics import (
    ExecutorMetrics,
    JobAnalysis,
    Quantiles,
    StageMetrics,
    TaskMetrics,
    TaskMetricsDistributions,
)


def parse_event_log(path: Path) -> JobAnalysis:
    state = _ParserState()

    open_fn = gzip.open if path.suffix == ".gz" else open
    with open_fn(path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            event = orjson.loads(line)
            _process_event(event, state)

    return state.build()


def _quantiles_from_list(values: list[int]) -> Quantiles:
    if not values:
        return Quantiles()
    s = sorted(values)
    n = len(s)
    return Quantiles(
        min=s[0],
        p25=s[n // 4] if n >= 4 else s[0],
        median=s[n // 2],
        p75=s[(3 * n) // 4] if n >= 4 else s[-1],
        max=s[-1],
    )


@dataclass
class _StageAccumulator:
    durations: list[int] = field(default_factory=list)
    executor_run_times: list[int] = field(default_factory=list)
    gc_times: list[int] = field(default_factory=list)
    task_count: int = 0
    total_gc_time_ms: int = 0
    total_executor_run_time_ms: int = 0
    shuffle_read_bytes: int = 0
    shuffle_write_bytes: int = 0
    spill_disk_bytes: int = 0
    spill_memory_bytes: int = 0
    failed_count: int = 0
    killed_count: int = 0
    input_records: int = 0
    output_records: int = 0
    shuffle_read_records: int = 0
    shuffle_write_records: int = 0


class _ParserState:
    def __init__(self) -> None:
        self.app_id: str = ""
        self.app_name: str = ""
        self.spark_version: str = ""
        self.start_time: int = 0
        self.end_time: int = 0
        self.config: dict[str, str] = {}
        self.stage_info: dict[int, dict[str, Any]] = {}
        self.stage_tasks: dict[int, _StageAccumulator] = defaultdict(_StageAccumulator)
        self.executor_count: int = 0

    def build(self) -> JobAnalysis:
        stages = []
        for stage_id, info in sorted(self.stage_info.items()):
            acc = self.stage_tasks[stage_id]

            duration_q = _quantiles_from_list(acc.durations)
            run_time_q = _quantiles_from_list(acc.executor_run_times)
            gc_q = _quantiles_from_list(acc.gc_times)

            distributions = TaskMetricsDistributions(
                duration=duration_q,
                executor_run_time=run_time_q,
                jvm_gc_time=gc_q,
            )

            stages.append(
                StageMetrics(
                    stage_id=stage_id,
                    stage_name=info.get("name", f"Stage {stage_id}"),
                    sum_executor_run_time_ms=acc.total_executor_run_time_ms,
                    total_gc_time_ms=acc.total_gc_time_ms,
                    total_shuffle_read_bytes=acc.shuffle_read_bytes,
                    total_shuffle_write_bytes=acc.shuffle_write_bytes,
                    spill_to_disk_bytes=acc.spill_disk_bytes,
                    spill_to_memory_bytes=acc.spill_memory_bytes,
                    failed_task_count=acc.failed_count,
                    killed_task_count=acc.killed_count,
                    input_bytes=info.get("input_bytes", 0),
                    input_records=acc.input_records,
                    output_bytes=info.get("output_bytes", 0),
                    output_records=acc.output_records,
                    shuffle_read_records=acc.shuffle_read_records,
                    shuffle_write_records=acc.shuffle_write_records,
                    tasks=TaskMetrics(
                        task_count=acc.task_count,
                        distributions=distributions,
                    ),
                )
            )

        total_task_time = sum(acc.total_executor_run_time_ms for acc in self.stage_tasks.values())

        return JobAnalysis(
            app_id=self.app_id,
            app_name=self.app_name,
            spark_version=self.spark_version,
            duration_ms=self.end_time - self.start_time,
            config=SparkConfig(raw=self.config),
            stages=stages,
            executors=ExecutorMetrics(
                executor_count=self.executor_count,
                peak_memory_bytes_sum=0,
                allocated_memory_bytes_sum=0,
                total_task_time_ms=total_task_time,
            ),
        )


def _process_event(event: dict[str, Any], state: _ParserState) -> None:
    event_type = event.get("Event", "")

    match event_type:
        case "SparkListenerApplicationStart":
            state.app_id = event.get("App ID", "")
            state.app_name = event.get("App Name", "")
            state.start_time = event.get("Timestamp", 0)

        case "SparkListenerApplicationEnd":
            state.end_time = event.get("Timestamp", 0)

        case "SparkListenerLogStart":
            state.spark_version = event.get("Spark Version", "")

        case "SparkListenerEnvironmentUpdate":
            for key, value in event.get("Spark Properties", {}).items():
                state.config[key] = value

        case "SparkListenerStageCompleted":
            stage_info = event.get("Stage Info", {})
            stage_id = stage_info.get("Stage ID", -1)
            state.stage_info[stage_id] = {
                "name": stage_info.get("Stage Name", ""),
                "submission_time": stage_info.get("Submission Time", 0),
                "completion_time": stage_info.get("Completion Time", 0),
                "input_bytes": _extract_accumulator(stage_info, "internal.metrics.input.bytesRead"),
                "output_bytes": _extract_accumulator(stage_info, "internal.metrics.output.bytesWritten")
                if stage_info.get("Accumulables")
                else 0,
            }

        case "SparkListenerTaskEnd":
            stage_id = event.get("Stage ID", -1)
            task_info = event.get("Task Info", {})
            task_metrics = event.get("Task Metrics", {})

            if not task_info or not task_metrics:
                return

            acc = state.stage_tasks[stage_id]

            if task_info.get("Failed", False):
                acc.failed_count += 1

            task_end_reason = event.get("Task End Reason", {})
            if task_end_reason.get("Reason") == "TaskKilled":
                acc.killed_count += 1

            duration = task_info.get("Finish Time", 0) - task_info.get("Launch Time", 0)
            acc.durations.append(duration)
            acc.task_count += 1

            gc_time = task_metrics.get("JVM GC Time", 0)
            acc.gc_times.append(gc_time)
            acc.total_gc_time_ms += gc_time

            executor_run_time = task_metrics.get("Executor Run Time", 0)
            acc.executor_run_times.append(executor_run_time)
            acc.total_executor_run_time_ms += executor_run_time

            shuffle_read = task_metrics.get("Shuffle Read Metrics", {})
            acc.shuffle_read_bytes += shuffle_read.get("Remote Bytes Read", 0)
            acc.shuffle_read_bytes += shuffle_read.get("Local Bytes Read", 0)
            acc.shuffle_read_records += shuffle_read.get("Total Records Read", 0)

            shuffle_write = task_metrics.get("Shuffle Write Metrics", {})
            acc.shuffle_write_bytes += shuffle_write.get("Shuffle Bytes Written", 0)
            acc.shuffle_write_records += shuffle_write.get("Shuffle Records Written", 0)

            input_metrics = task_metrics.get("Input Metrics", {})
            acc.input_records += input_metrics.get("Records Read", 0)

            output_metrics = task_metrics.get("Output Metrics", {})
            acc.output_records += output_metrics.get("Records Written", 0)

            acc.spill_disk_bytes += task_metrics.get("Disk Bytes Spilled", 0)
            acc.spill_memory_bytes += task_metrics.get("Memory Bytes Spilled", 0)

        case "SparkListenerExecutorAdded":
            state.executor_count += 1


def _extract_accumulator(stage_info: dict[str, Any], name: str) -> int:
    for acc in stage_info.get("Accumulables", []):
        if acc.get("Name") == name:
            return int(acc.get("Value", 0))
    return 0
