from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from spark_advisor_models.model import JobAnalysis

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SAMPLE_LOG = _REPO_ROOT / "sample_event_logs" / "sample_etl_job.json"


class TestAnalyzeSparkJob:
    def test_analyze_from_event_log(self) -> None:
        from spark_advisor_mcp.server import analyze_spark_job

        result = analyze_spark_job(str(SAMPLE_LOG), mode="static")

        assert "## Job Overview" in result
        assert "## Rule Findings" in result
        assert "application_1234567890_0001" in result

    def test_analyze_static_mode_skips_ai(self) -> None:
        from spark_advisor_mcp.server import analyze_spark_job

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
            result = analyze_spark_job(str(SAMPLE_LOG), mode="static")

        assert "AI Analysis" not in result
        assert "## Rule Findings" in result

    def test_analyze_file_not_found(self) -> None:
        from spark_advisor_mcp.server import analyze_spark_job

        with pytest.raises(FileNotFoundError, match="not found"):
            analyze_spark_job("/nonexistent/file.json", mode="static")

    def test_ai_mode_downgrades_without_api_key(self) -> None:
        from spark_advisor_mcp.server import analyze_spark_job

        with patch.dict("os.environ", {}, clear=True):
            result = analyze_spark_job(str(SAMPLE_LOG), mode="ai")

        assert "AI Analysis" not in result
        assert "## Rule Findings" in result


class TestGetJobConfig:
    def test_returns_config_table(self) -> None:
        from spark_advisor_mcp.server import get_job_config

        result = get_job_config(str(SAMPLE_LOG))

        assert "## Job Overview" in result
        assert "## Spark Configuration" in result
        assert "| Key | Value |" in result

    def test_contains_spark_properties(self) -> None:
        from spark_advisor_mcp.server import get_job_config

        result = get_job_config(str(SAMPLE_LOG))

        assert "spark." in result


class TestSuggestConfig:
    def test_returns_suggestions(self) -> None:
        from spark_advisor_mcp.server import suggest_config

        result = suggest_config(str(SAMPLE_LOG))

        assert isinstance(result, str)
        assert len(result) > 0

    def test_runs_rules_engine(self) -> None:
        from spark_advisor_mcp.server import suggest_config

        result = suggest_config(str(SAMPLE_LOG))

        assert "Suggested" in result or "No configuration changes" in result


class TestScanRecentJobs:
    def test_scan_with_mock_hs(self) -> None:
        from spark_advisor_mcp.server import scan_recent_jobs
        from spark_advisor_models.model import ApplicationSummary, Attempt

        mock_apps = [
            ApplicationSummary(
                id="app-001",
                name="SparkPi",
                attempts=[
                    Attempt(
                        duration=60000,
                        completed=True,
                        app_spark_version="3.5.0",
                        spark_user="hdfs",
                    )
                ],
            ),
            ApplicationSummary(id="app-002", name="ETL Job"),
        ]

        with patch(
            "spark_advisor_hs_connector.history_server_client.HistoryServerClient"
        ) as mock_cls:
            mock_client = MagicMock()
            mock_client.list_applications.return_value = mock_apps
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_cls.return_value = mock_client

            result = scan_recent_jobs("http://localhost:18080", limit=10)

        assert "app-001" in result
        assert "SparkPi" in result
        assert "app-002" in result
        assert "## Recent Spark Applications" in result

    def test_scan_empty(self) -> None:
        from spark_advisor_mcp.server import scan_recent_jobs

        with patch(
            "spark_advisor_hs_connector.history_server_client.HistoryServerClient"
        ) as mock_cls:
            mock_client = MagicMock()
            mock_client.list_applications.return_value = []
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_cls.return_value = mock_client

            result = scan_recent_jobs("http://localhost:18080")

        assert "No applications found" in result


class TestExplainMetric:
    def test_known_metric(self) -> None:
        from spark_advisor_mcp.server import explain_metric

        result = explain_metric("gc_time_percent", 35.0)

        assert "## Metric: `gc_time_percent`" in result
        assert "35.00" in result
        assert "garbage collection" in result

    def test_unknown_metric(self) -> None:
        from spark_advisor_mcp.server import explain_metric

        result = explain_metric("nonexistent_metric", 0.0)

        assert "Unknown metric" in result
        assert "Known metrics:" in result

    def test_bytes_metric_formatted(self) -> None:
        from spark_advisor_mcp.server import explain_metric

        result = explain_metric("spill_to_disk_bytes", 1073741824.0)

        assert "1.0 GB" in result

    def test_all_known_metrics(self) -> None:
        from spark_advisor_mcp.formatting import METRIC_EXPLANATIONS
        from spark_advisor_mcp.server import explain_metric

        for metric_name in METRIC_EXPLANATIONS:
            result = explain_metric(metric_name, 100.0)
            assert f"## Metric: `{metric_name}`" in result


class TestFormatting:
    def test_format_job_overview(self, sample_job: JobAnalysis) -> None:
        from spark_advisor_mcp.formatting import format_job_overview

        result = format_job_overview(sample_job)

        assert "## Job Overview" in result
        assert sample_job.app_id in result
        assert "Duration" in result

    def test_format_rule_results_empty(self) -> None:
        from spark_advisor_mcp.formatting import format_rule_results

        result = format_rule_results([])

        assert "No issues detected" in result

    def test_format_rule_results_with_findings(self, sample_job: JobAnalysis) -> None:
        from spark_advisor_mcp.formatting import format_rule_results
        from spark_advisor_models.config import Thresholds
        from spark_advisor_rules import StaticAnalysisService, rules_for_threshold

        static = StaticAnalysisService(rules_for_threshold(Thresholds()))
        rule_results = static.analyze(sample_job)

        result = format_rule_results(rule_results)

        assert "## Rule Findings" in result

    def test_format_config_table(self, sample_job: JobAnalysis) -> None:
        from spark_advisor_mcp.formatting import format_config_table

        result = format_config_table(sample_job.config)

        assert "## Spark Configuration" in result
        assert "| Key | Value |" in result
