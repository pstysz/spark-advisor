from unittest.mock import MagicMock, patch

from tests.conftest import SAMPLE_LOG


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

    def test_analyze_file_not_found_returns_error(self) -> None:
        from spark_advisor_mcp.server import analyze_spark_job

        result = analyze_spark_job("/nonexistent/file.json", mode="static")

        assert "## Error" in result
        assert "File not found" in result

    def test_ai_mode_downgrades_without_api_key(self) -> None:
        from spark_advisor_mcp.server import analyze_spark_job

        with patch.dict("os.environ", {}, clear=True):
            result = analyze_spark_job(str(SAMPLE_LOG), mode="ai")

        assert "AI Analysis" not in result
        assert "## Rule Findings" in result

    def test_analyze_includes_quick_stats(self) -> None:
        from spark_advisor_mcp.server import analyze_spark_job

        result = analyze_spark_job(str(SAMPLE_LOG), mode="static")

        assert "### Quick Stats" in result
        assert "Total Shuffle Read" in result

    def test_analyze_invalid_history_server_url(self) -> None:
        from spark_advisor_mcp.server import analyze_spark_job

        result = analyze_spark_job("app-123", history_server="not-a-url", mode="static")

        assert "## Error" in result
        assert "Invalid" in result


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

    def test_config_grouped_by_category(self) -> None:
        from spark_advisor_mcp.server import get_job_config

        result = get_job_config(str(SAMPLE_LOG))

        assert "### " in result

    def test_file_not_found_returns_error(self) -> None:
        from spark_advisor_mcp.server import get_job_config

        result = get_job_config("/nonexistent/file.json")

        assert "## Error" in result
        assert "File not found" in result


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

    def test_file_not_found_returns_error(self) -> None:
        from spark_advisor_mcp.server import suggest_config

        result = suggest_config("/nonexistent/file.json")

        assert "## Error" in result


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
            "spark_advisor_hs_connector.history_server.client.HistoryServerClient"
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
            "spark_advisor_hs_connector.history_server.client.HistoryServerClient"
        ) as mock_cls:
            mock_client = MagicMock()
            mock_client.list_applications.return_value = []
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_cls.return_value = mock_client

            result = scan_recent_jobs("http://localhost:18080")

        assert "No applications found" in result

    def test_scan_invalid_url_returns_error(self) -> None:
        from spark_advisor_mcp.server import scan_recent_jobs

        result = scan_recent_jobs("not-a-url")

        assert "## Error" in result
        assert "Invalid" in result

    def test_scan_negative_limit_returns_error(self) -> None:
        from spark_advisor_mcp.server import scan_recent_jobs

        result = scan_recent_jobs("http://localhost:18080", limit=-1)

        assert "## Error" in result
        assert "limit" in result


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
        from spark_advisor_mcp.metric_explanations import METRIC_DESCRIPTIONS
        from spark_advisor_mcp.server import explain_metric

        for metric_name in METRIC_DESCRIPTIONS:
            result = explain_metric(metric_name, 100.0)
            assert f"## Metric: `{metric_name}`" in result

    def test_critical_assessment(self) -> None:
        from spark_advisor_mcp.server import explain_metric

        result = explain_metric("gc_time_percent", 50.0)

        assert "### Assessment" in result
        assert "Critical" in result

    def test_warning_assessment(self) -> None:
        from spark_advisor_mcp.server import explain_metric

        result = explain_metric("gc_time_percent", 25.0)

        assert "### Assessment" in result
        assert "Warning" in result

    def test_healthy_assessment(self) -> None:
        from spark_advisor_mcp.server import explain_metric

        result = explain_metric("gc_time_percent", 5.0)

        assert "### Assessment" in result
        assert "Healthy" in result

    def test_metric_without_thresholds_no_assessment(self) -> None:
        from spark_advisor_mcp.server import explain_metric

        result = explain_metric("shuffle_read_bytes", 1000.0)

        assert "### Assessment" not in result


class TestGetStageDetails:
    def test_returns_stage_details(self) -> None:
        from spark_advisor_mcp.server import get_stage_details

        result = get_stage_details(str(SAMPLE_LOG), stage_id=0)

        assert "## Stage 0" in result
        assert "Task Count" in result
        assert "Duration Quantiles" in result

    def test_stage_not_found(self) -> None:
        from spark_advisor_mcp.server import get_stage_details

        result = get_stage_details(str(SAMPLE_LOG), stage_id=9999)

        assert "## Error" in result
        assert "Stage not found" in result
        assert "Available stages" in result

    def test_file_not_found(self) -> None:
        from spark_advisor_mcp.server import get_stage_details

        result = get_stage_details("/nonexistent/file.json", stage_id=0)

        assert "## Error" in result
        assert "File not found" in result

    def test_includes_io_section(self) -> None:
        from spark_advisor_mcp.server import get_stage_details

        result = get_stage_details(str(SAMPLE_LOG), stage_id=0)

        assert "### I/O" in result
        assert "Shuffle Read" in result
        assert "Shuffle Write" in result

    def test_includes_gc_quantiles(self) -> None:
        from spark_advisor_mcp.server import get_stage_details

        result = get_stage_details(str(SAMPLE_LOG), stage_id=0)

        assert "### GC Time Quantiles" in result


class TestCompareJobs:
    def test_compare_same_job(self) -> None:
        from spark_advisor_mcp.server import compare_jobs

        result = compare_jobs(str(SAMPLE_LOG), str(SAMPLE_LOG))

        assert "## Job Comparison" in result
        assert "Duration" in result
        assert "Delta" in result

    def test_compare_file_not_found(self) -> None:
        from spark_advisor_mcp.server import compare_jobs

        result = compare_jobs("/nonexistent/a.json", "/nonexistent/b.json")

        assert "## Error" in result

    def test_compare_includes_metrics(self) -> None:
        from spark_advisor_mcp.server import compare_jobs

        result = compare_jobs(str(SAMPLE_LOG), str(SAMPLE_LOG))

        assert "Total Tasks" in result
        assert "Shuffle Read" in result
        assert "Spill to Disk" in result
