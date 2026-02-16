from pathlib import Path

from spark_advisor.util.event_parser import parse_event_log

SAMPLE_LOG = (
    Path(__file__).parent.parent.parent.parent / "sample_event_logs" / "sample_etl_job.json"
)


class TestParseEventLog:
    def test_parses_app_metadata(self):
        job = parse_event_log(SAMPLE_LOG)
        assert job.app_id == "application_1234567890_0001"
        assert job.app_name == "SampleETLJob"

    def test_parses_config(self):
        job = parse_event_log(SAMPLE_LOG)
        assert job.config.executor_memory == "4g"
        assert job.config.shuffle_partitions == 200
        assert job.config.aqe_enabled is False

    def test_parses_stages(self):
        job = parse_event_log(SAMPLE_LOG)
        assert len(job.stages) == 2

    def test_aggregates_task_metrics(self):
        job = parse_event_log(SAMPLE_LOG)
        stage1 = next(s for s in job.stages if s.stage_id == 1)
        assert stage1.tasks.task_count == 3
        assert stage1.tasks.spill_to_disk_bytes > 0

    def test_detects_skew_in_sample(self):
        job = parse_event_log(SAMPLE_LOG)
        stage1 = next(s for s in job.stages if s.stage_id == 1)
        assert stage1.tasks.skew_ratio > 10

    def test_calculates_duration(self):
        job = parse_event_log(SAMPLE_LOG)
        assert job.duration_ms > 0
