"""Streaming parser for Spark event log files.

Handles files that can be 100MB+ by processing line-by-line
and aggregating metrics in memory. Never loads entire file.

Supports both plain .json and compressed .json.gz formats.
"""

import gzip
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Any

import orjson

from spark_advisor.core import (
    ExecutorMetrics,
    JobAnalysis,
    SparkConfig,
    StageMetrics,
    TaskMetrics,
)


class EventLogParser:
    """Parses Spark event log files into JobAnalysis model.

    Usage:
        parser = EventLogParser()
        job = parser.parse(Path("/path/to/event-log.json.gz"))
    """

    @staticmethod
    def parse(path: Path) -> JobAnalysis:
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


class _ParserState:
    """Mutable accumulator for parsing — not exposed outside this module."""

    def __init__(self) -> None:
        self.app_id: str = ""
        self.app_name: str = ""
        self.spark_version: str = ""
        self.start_time: int = 0
        self.end_time: int = 0
        self.config: dict[str, str] = {}
        self.stage_data: dict[int, dict[str, Any]] = {}
        self.task_durations: dict[int, list[int]] = defaultdict(list)
        self.task_gc_times: dict[int, int] = defaultdict(int)
        self.task_counts: dict[int, int] = defaultdict(int)
        self.task_shuffle_read: dict[int, int] = defaultdict(int)
        self.task_shuffle_write: dict[int, int] = defaultdict(int)
        self.task_spill_disk: dict[int, int] = defaultdict(int)
        self.task_spill_memory: dict[int, int] = defaultdict(int)
        self.task_failed: dict[int, int] = defaultdict(int)
        self.executor_count: int = 0
        self.executor_max_memory: int = 0

    def build(self) -> JobAnalysis:
        stages = []
        for stage_id, data in sorted(self.stage_data.items()):
            durations = self.task_durations.get(stage_id, [])
            median_dur = int(median(durations)) if durations else 0
            max_dur = max(durations) if durations else 0
            min_dur = min(durations) if durations else 0

            tasks = TaskMetrics(
                task_count=self.task_counts.get(stage_id, 0),
                median_duration_ms=median_dur,
                max_duration_ms=max_dur,
                min_duration_ms=min_dur,
                total_gc_time_ms=self.task_gc_times.get(stage_id, 0),
                total_shuffle_read_bytes=self.task_shuffle_read.get(stage_id, 0),
                total_shuffle_write_bytes=self.task_shuffle_write.get(stage_id, 0),
                spill_to_disk_bytes=self.task_spill_disk.get(stage_id, 0),
                spill_to_memory_bytes=self.task_spill_memory.get(stage_id, 0),
                failed_task_count=self.task_failed.get(stage_id, 0),
            )

            stages.append(
                StageMetrics(
                    stage_id=stage_id,
                    stage_name=data.get("name", f"Stage {stage_id}"),
                    duration_ms=data.get("completion_time", 0) - data.get("submission_time", 0),
                    input_bytes=data.get("input_bytes", 0),
                    output_bytes=data.get("output_bytes", 0),
                    tasks=tasks,
                )
            )

        executors = ExecutorMetrics(
            executor_count=self.executor_count,
            peak_memory_bytes=0,
            allocated_memory_bytes=self.executor_max_memory,
            total_cpu_time_ms=0,
            total_run_time_ms=0,
        )

        return JobAnalysis(
            app_id=self.app_id,
            app_name=self.app_name,
            spark_version=self.spark_version,
            duration_ms=self.end_time - self.start_time,
            config=SparkConfig(raw=self.config),
            stages=stages,
            executors=executors,
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
            state.stage_data[stage_id] = {
                "name": stage_info.get("Stage Name", ""),
                "submission_time": stage_info.get("Submission Time", 0),
                "completion_time": stage_info.get("Completion Time", 0),
                "input_bytes": stage_info.get("Accumulables", [{}])[0].get("Value", 0)
                if stage_info.get("Accumulables")
                else 0,
            }

        case "SparkListenerTaskEnd":
            stage_id = event.get("Stage ID", -1)
            task_info = event.get("Task Info", {})
            task_metrics = event.get("Task Metrics", {})

            if not task_info or not task_metrics:
                return

            if task_info.get("Failed", False):
                state.task_failed[stage_id] += 1

            duration = task_info.get("Finish Time", 0) - task_info.get("Launch Time", 0)
            state.task_durations[stage_id].append(duration)
            state.task_counts[stage_id] += 1
            state.task_gc_times[stage_id] += task_metrics.get("JVM GC Time", 0)

            shuffle_read = task_metrics.get("Shuffle Read Metrics", {})
            state.task_shuffle_read[stage_id] += shuffle_read.get("Remote Bytes Read", 0)

            shuffle_write = task_metrics.get("Shuffle Write Metrics", {})
            state.task_shuffle_write[stage_id] += shuffle_write.get("Shuffle Bytes Written", 0)

            state.task_spill_disk[stage_id] += task_metrics.get("Disk Bytes Spilled", 0)
            state.task_spill_memory[stage_id] += task_metrics.get("Memory Bytes Spilled", 0)

        case "SparkListenerExecutorAdded":
            state.executor_count += 1
            exec_info = event.get("Executor Info", {})
            state.executor_max_memory += exec_info.get("Total Cores", 0)
