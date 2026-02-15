"""Tests for the event log file parser."""

from pathlib import Path

from spark_advisor.sources.event_log import EventLogParser

SAMPLE_LOG = (
    Path(__file__).parent.parent.parent.parent
    / "sample_event_logs"
    / "sample_etl_job.json"
)


class TestEventLogParser:
    def test_parses_app_metadata(self):
        job = EventLogParser().parse(SAMPLE_LOG)
        assert job.app_id == "application_1234567890_0001"
        assert job.app_name == "SampleETLJob"

    def test_parses_config(self):
        job = EventLogParser().parse(SAMPLE_LOG)
        assert job.config.executor_memory == "4g"
        assert job.config.shuffle_partitions == 200
        assert job.config.aqe_enabled is False

    def test_parses_stages(self):
        job = EventLogParser().parse(SAMPLE_LOG)
        assert len(job.stages) == 2

    def test_aggregates_task_metrics(self):
        job = EventLogParser().parse(SAMPLE_LOG)
        stage1 = next(s for s in job.stages if s.stage_id == 1)
        assert stage1.tasks.task_count == 3
        assert stage1.tasks.spill_to_disk_bytes > 0

    def test_detects_skew_in_sample(self):
        job = EventLogParser().parse(SAMPLE_LOG)
        stage1 = next(s for s in job.stages if s.stage_id == 1)
        assert stage1.tasks.skew_ratio > 10

    def test_calculates_duration(self):
        job = EventLogParser().parse(SAMPLE_LOG)
        assert job.duration_ms > 0
