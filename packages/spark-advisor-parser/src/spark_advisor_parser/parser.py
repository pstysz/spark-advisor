import gzip
import io
from collections import defaultdict
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Any

import lz4.frame  # type: ignore[import-untyped]
import orjson
import snappy  # type: ignore[import-untyped]
import zstandard

from spark_advisor_models.model import (
    ExecutorMetrics,
    JobAnalysis,
    Quantiles,
    SparkConfig,
    StageMetrics,
    TaskMetrics,
    TaskMetricsDistributions,
)


def parse_event_log(path: Path) -> JobAnalysis:
    state = _ParserState()

    with _open_event_log(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                event = orjson.loads(line)
            except orjson.JSONDecodeError:
                continue
            _process_event(event, state)

    return state.build()


@contextmanager
def _open_event_log(path: Path) -> Iterator[IO[str]]:
    suffix = path.suffix.lower()
    suffixes = [s.lower() for s in path.suffixes]

    if suffix == ".gz" or ".gz" in suffixes:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            yield f
    elif suffix == ".lz4":
        with lz4.frame.open(path, "rt", encoding="utf-8") as f:
            yield f
    elif suffix == ".snappy":
        raw = path.read_bytes()
        decompressed = snappy.decompress(raw)
        yield io.StringIO(decompressed.decode("utf-8"))
    elif suffix == ".zstd" or suffix == ".zst":
        dctx = zstandard.ZstdDecompressor()
        with open(path, "rb") as raw_f, dctx.stream_reader(raw_f) as reader:
            yield io.TextIOWrapper(reader, encoding="utf-8")
    else:
        with open(path, encoding="utf-8") as f:
            yield f


def _quantiles_from_list(values: list[int]) -> Quantiles:
    if not values:
        return Quantiles()
    s = sorted(values)
    n = len(s)

    def _percentile(p: float) -> int:
        idx = p * (n - 1)
        lo = int(idx)
        hi = min(lo + 1, n - 1)
        frac = idx - lo
        return int(s[lo] + frac * (s[hi] - s[lo]))

    return Quantiles(
        min=s[0],
        p25=_percentile(0.25),
        median=_percentile(0.5),
        p75=_percentile(0.75),
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

    def add_task(self, task_info: dict[str, Any], task_metrics: dict[str, Any], event: dict[str, Any]) -> None:
        if task_info.get("Failed", False):
            self.failed_count += 1

        task_end_reason = event.get("Task End Reason", {})
        if task_end_reason.get("Reason") == "TaskKilled":
            self.killed_count += 1

        duration = task_info.get("Finish Time", 0) - task_info.get("Launch Time", 0)
        self.durations.append(duration)
        self.task_count += 1

        gc_time = task_metrics.get("JVM GC Time", 0)
        self.gc_times.append(gc_time)
        self.total_gc_time_ms += gc_time

        executor_run_time = task_metrics.get("Executor Run Time", 0)
        self.executor_run_times.append(executor_run_time)
        self.total_executor_run_time_ms += executor_run_time

        shuffle_read = task_metrics.get("Shuffle Read Metrics", {})
        self.shuffle_read_bytes += shuffle_read.get("Remote Bytes Read", 0)
        self.shuffle_read_bytes += shuffle_read.get("Local Bytes Read", 0)
        self.shuffle_read_records += shuffle_read.get("Total Records Read", 0)

        shuffle_write = task_metrics.get("Shuffle Write Metrics", {})
        self.shuffle_write_bytes += shuffle_write.get("Shuffle Bytes Written", 0)
        self.shuffle_write_records += shuffle_write.get("Shuffle Records Written", 0)

        input_m = task_metrics.get("Input Metrics", {})
        self.input_records += input_m.get("Records Read", 0)

        output_m = task_metrics.get("Output Metrics", {})
        self.output_records += output_m.get("Records Written", 0)

        self.spill_disk_bytes += task_metrics.get("Disk Bytes Spilled", 0)
        self.spill_memory_bytes += task_metrics.get("Memory Bytes Spilled", 0)

    def build_stage_metrics(self, stage_id: int, stage_info: dict[str, Any]) -> StageMetrics:
        distributions = TaskMetricsDistributions(
            duration=_quantiles_from_list(self.durations),
            executor_run_time=_quantiles_from_list(self.executor_run_times),
            jvm_gc_time=_quantiles_from_list(self.gc_times),
        )
        return StageMetrics(
            stage_id=stage_id,
            stage_name=stage_info.get("name", f"Stage {stage_id}"),
            sum_executor_run_time_ms=self.total_executor_run_time_ms,
            total_gc_time_ms=self.total_gc_time_ms,
            total_shuffle_read_bytes=self.shuffle_read_bytes,
            total_shuffle_write_bytes=self.shuffle_write_bytes,
            spill_to_disk_bytes=self.spill_disk_bytes,
            spill_to_memory_bytes=self.spill_memory_bytes,
            failed_task_count=self.failed_count,
            killed_task_count=self.killed_count,
            input_bytes=stage_info.get("input_bytes", 0),
            input_records=self.input_records,
            output_bytes=stage_info.get("output_bytes", 0),
            output_records=self.output_records,
            shuffle_read_records=self.shuffle_read_records,
            shuffle_write_records=self.shuffle_write_records,
            tasks=TaskMetrics(
                task_count=self.task_count,
                distributions=distributions,
            ),
        )


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

    def _resolve_end_time(self) -> int:
        if self.end_time > self.start_time:
            return self.end_time
        if self.stage_info:
            return max(
                (info.get("completion_time", 0) for info in self.stage_info.values()),
                default=self.start_time,
            )
        return self.start_time

    def build(self) -> JobAnalysis:
        end_time = self._resolve_end_time()

        stages = []
        for stage_id, info in sorted(self.stage_info.items()):
            acc = self.stage_tasks[stage_id]
            stages.append(acc.build_stage_metrics(stage_id, info))

        total_task_time = sum(acc.total_executor_run_time_ms for acc in self.stage_tasks.values())

        return JobAnalysis(
            app_id=self.app_id,
            app_name=self.app_name,
            spark_version=self.spark_version,
            duration_ms=end_time - self.start_time,
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

            state.stage_tasks[stage_id].add_task(task_info, task_metrics, event)

        case "SparkListenerExecutorAdded":
            state.executor_count += 1


def _extract_accumulator(stage_info: dict[str, Any], name: str) -> int:
    for acc in stage_info.get("Accumulables", []):
        if acc.get("Name") == name:
            return int(acc.get("Value", 0))
    return 0
