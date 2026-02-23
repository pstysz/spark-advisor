import json
import tempfile
from pathlib import Path
from typing import Any

from spark_advisor.util.event_parser import parse_event_log

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent.parent.parent
SAMPLE_LOG = _REPO_ROOT / "sample_event_logs" / "sample_etl_job.json"


def _write_events(events: list[dict[str, Any]]) -> Path:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    return Path(f.name)


def _base_events() -> list[dict[str, Any]]:
    return [
        {"Event": "SparkListenerLogStart", "Spark Version": "3.5.0"},
        {
            "Event": "SparkListenerApplicationStart",
            "App ID": "app-1",
            "App Name": "Test",
            "Timestamp": 1000,
        },
        {
            "Event": "SparkListenerEnvironmentUpdate",
            "Spark Properties": {"spark.executor.memory": "4g"},
        },
        {
            "Event": "SparkListenerStageCompleted",
            "Stage Info": {"Stage ID": 0, "Stage Name": "read", "Accumulables": []},
        },
    ]


def _task_event(stage_id: int = 0, **metric_overrides: Any) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "Executor Run Time": 900,
        "JVM GC Time": 10,
        "Shuffle Read Metrics": {"Remote Bytes Read": 0},
        "Shuffle Write Metrics": {"Shuffle Bytes Written": 0},
        "Disk Bytes Spilled": 0,
        "Memory Bytes Spilled": 0,
    }
    metrics.update(metric_overrides)
    event: dict[str, Any] = {
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage_id,
        "Task Info": {"Launch Time": 1000, "Finish Time": 2000, "Failed": False},
        "Task Metrics": metrics,
    }
    return event


_APP_END: dict[str, Any] = {"Event": "SparkListenerApplicationEnd", "Timestamp": 5000}


class TestParseEventLog:
    def test_parses_app_metadata(self) -> None:
        job = parse_event_log(SAMPLE_LOG)
        assert job.app_id == "application_1234567890_0001"
        assert job.app_name == "SampleETLJob"

    def test_parses_config(self) -> None:
        job = parse_event_log(SAMPLE_LOG)
        assert job.config.executor_memory == "4g"
        assert job.config.shuffle_partitions == 200
        assert job.config.aqe_enabled is False

    def test_parses_stages(self) -> None:
        job = parse_event_log(SAMPLE_LOG)
        assert len(job.stages) == 2

    def test_aggregates_task_metrics(self) -> None:
        job = parse_event_log(SAMPLE_LOG)
        stage1 = next(s for s in job.stages if s.stage_id == 1)
        assert stage1.tasks.task_count == 3
        assert stage1.spill_to_disk_bytes > 0

    def test_detects_skew_in_sample(self) -> None:
        job = parse_event_log(SAMPLE_LOG)
        stage1 = next(s for s in job.stages if s.stage_id == 1)
        assert stage1.tasks.duration_skew_ratio > 10

    def test_calculates_duration(self) -> None:
        job = parse_event_log(SAMPLE_LOG)
        assert job.duration_ms > 0


class TestEventParserRecords:
    def test_collects_input_and_output_records(self) -> None:
        task = _task_event(
            **{
                "Input Metrics": {"Bytes Read": 1024, "Records Read": 500},
                "Output Metrics": {"Bytes Written": 512, "Records Written": 250},
            },
        )
        events = [*_base_events(), task, _APP_END]
        job = parse_event_log(_write_events(events))
        stage = job.stages[0]
        assert stage.input_records == 500
        assert stage.output_records == 250

    def test_collects_shuffle_records(self) -> None:
        task = _task_event(
            **{
                "Shuffle Read Metrics": {
                    "Remote Bytes Read": 100,
                    "Local Bytes Read": 200,
                    "Total Records Read": 1000,
                },
                "Shuffle Write Metrics": {
                    "Shuffle Bytes Written": 300,
                    "Shuffle Records Written": 800,
                },
            },
        )
        events = [*_base_events(), task, _APP_END]
        job = parse_event_log(_write_events(events))
        stage = job.stages[0]
        assert stage.total_shuffle_read_bytes == 300  # 100 remote + 200 local
        assert stage.shuffle_read_records == 1000
        assert stage.shuffle_write_records == 800

    def test_counts_killed_tasks(self) -> None:
        task = _task_event()
        task["Task End Reason"] = {"Reason": "TaskKilled"}
        events = [*_base_events(), task, _APP_END]
        job = parse_event_log(_write_events(events))
        stage = job.stages[0]
        assert stage.killed_task_count == 1

    def test_local_shuffle_read_included(self) -> None:
        task = _task_event(
            **{
                "Shuffle Read Metrics": {
                    "Remote Bytes Read": 500,
                    "Local Bytes Read": 300,
                },
            },
        )
        events = [*_base_events(), task, _APP_END]
        job = parse_event_log(_write_events(events))
        stage = job.stages[0]
        assert stage.total_shuffle_read_bytes == 800  # 500 + 300
