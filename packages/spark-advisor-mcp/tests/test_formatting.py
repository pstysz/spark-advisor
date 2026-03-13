from spark_advisor_mcp.formatting import (
    format_config_table,
    format_job_comparison,
    format_job_overview,
    format_rule_results,
    format_scan_results,
    format_stage_details,
    format_suggested_config,
)
from spark_advisor_models.config import Thresholds
from spark_advisor_models.model import JobAnalysis, RuleResult, Severity
from spark_advisor_models.model.metrics import (
    ExecutorMetrics,
    Quantiles,
    StageMetrics,
    TaskMetrics,
    TaskMetricsDistributions,
)
from spark_advisor_models.model.spark_config import SparkConfig
from spark_advisor_rules import StaticAnalysisService, rules_for_threshold


class TestFormatJobOverview:
    def test_basic_fields(self, sample_job: JobAnalysis) -> None:
        result = format_job_overview(sample_job)

        assert "## Job Overview" in result
        assert sample_job.app_id in result
        assert "Duration" in result

    def test_includes_quick_stats(self, sample_job: JobAnalysis) -> None:
        result = format_job_overview(sample_job)

        assert "### Quick Stats" in result
        assert "Total Shuffle Read" in result
        assert "Total Shuffle Write" in result
        assert "Total Spill to Disk" in result
        assert "Aggregate GC Time" in result

    def test_includes_total_tasks(self, sample_job: JobAnalysis) -> None:
        result = format_job_overview(sample_job)

        assert "Total Tasks" in result

    def test_includes_duration_in_minutes(self, sample_job: JobAnalysis) -> None:
        result = format_job_overview(sample_job)

        assert "min" in result


class TestFormatRuleResults:
    def test_empty_list(self) -> None:
        result = format_rule_results([])

        assert "No issues detected" in result

    def test_with_findings(self, sample_job: JobAnalysis) -> None:
        static = StaticAnalysisService(rules_for_threshold(Thresholds()))
        rule_results = static.analyze(sample_job)

        result = format_rule_results(rule_results)

        assert "## Rule Findings" in result

    def test_stage_id_included(self) -> None:
        rule = RuleResult(
            rule_id="test",
            severity=Severity.WARNING,
            title="Test Rule",
            message="Test message",
            stage_id=2,
            current_value="100",
            recommended_value="200",
        )

        result = format_rule_results([rule])

        assert "[Stage 2]" in result

    def test_stage_id_omitted_when_none(self) -> None:
        rule = RuleResult(
            rule_id="test",
            severity=Severity.INFO,
            title="Global Rule",
            message="Test message",
        )

        result = format_rule_results([rule])

        assert "[Stage" not in result

    def test_severity_icons(self) -> None:
        rules = [
            RuleResult(rule_id="c", severity=Severity.CRITICAL, title="Critical", message="msg"),
            RuleResult(rule_id="w", severity=Severity.WARNING, title="Warning", message="msg"),
            RuleResult(rule_id="i", severity=Severity.INFO, title="Info", message="msg"),
        ]

        result = format_rule_results(rules)

        assert "🔴" in result
        assert "🟡" in result
        assert "🔵" in result


class TestFormatConfigTable:
    def test_basic_table(self, sample_job: JobAnalysis) -> None:
        result = format_config_table(sample_job.config)

        assert "## Spark Configuration" in result
        assert "| Key | Value |" in result

    def test_grouped_by_category(self, sample_job: JobAnalysis) -> None:
        result = format_config_table(sample_job.config)

        assert "### " in result


class TestFormatSuggestedConfig:
    def test_no_suggestions(self) -> None:
        result = format_suggested_config([])

        assert "No configuration changes suggested" in result

    def test_with_suggestions(self) -> None:
        rule = RuleResult(
            rule_id="shuffle_partitions",
            severity=Severity.WARNING,
            title="Shuffle Partitions",
            message="msg",
            current_value="200",
            recommended_value="400",
        )

        result = format_suggested_config([rule])

        assert "## Suggested Configuration Changes" in result
        assert "shuffle_partitions" in result
        assert "200" in result
        assert "400" in result
        assert "| Stage |" in result

    def test_multi_stage_results_preserved(self) -> None:
        rules = [
            RuleResult(
                rule_id="data_skew",
                severity=Severity.CRITICAL,
                title="Skew Stage 1",
                message="msg",
                stage_id=1,
                current_value="skew 8x",
                recommended_value="enable AQE",
            ),
            RuleResult(
                rule_id="data_skew",
                severity=Severity.WARNING,
                title="Skew Stage 3",
                message="msg",
                stage_id=3,
                current_value="skew 6x",
                recommended_value="enable AQE",
            ),
        ]

        result = format_suggested_config(rules)

        assert "Stage 1" in result
        assert "Stage 3" in result

    def test_global_rule_shows_dash_for_stage(self) -> None:
        rule = RuleResult(
            rule_id="serializer_choice",
            severity=Severity.INFO,
            title="Serializer",
            message="msg",
            current_value="Java",
            recommended_value="Kryo",
        )

        result = format_suggested_config([rule])

        assert "| - |" in result


class TestFormatScanResults:
    def test_empty(self) -> None:
        result = format_scan_results([])

        assert "No applications found" in result

    def test_with_apps(self) -> None:
        from spark_advisor_models.model import ApplicationSummary, Attempt

        apps = [
            ApplicationSummary(
                id="app-001",
                name="Test",
                attempts=[Attempt(duration=120000, completed=True, app_spark_version="3.5.0", spark_user="user")],
            )
        ]

        result = format_scan_results(apps)

        assert "## Recent Spark Applications (1)" in result
        assert "app-001" in result
        assert "2.0 min" in result


class TestFormatStageDetails:
    def test_basic_output(self) -> None:
        stage = StageMetrics(
            stage_id=1,
            stage_name="test_stage",
            sum_executor_run_time_ms=10000,
            total_gc_time_ms=2000,
            total_shuffle_read_bytes=1024 * 1024 * 100,
            total_shuffle_write_bytes=1024 * 1024 * 50,
            spill_to_disk_bytes=1024 * 1024 * 10,
            input_bytes=1024 * 1024 * 200,
            output_bytes=1024 * 1024 * 100,
            tasks=TaskMetrics(
                task_count=50,
                distributions=TaskMetricsDistributions(
                    executor_run_time=Quantiles(min=10, p25=50, median=100, p75=200, max=500),
                    jvm_gc_time=Quantiles(min=0, p25=5, median=10, p75=20, max=50),
                ),
            ),
        )

        result = format_stage_details(stage)

        assert "## Stage 1: test_stage" in result
        assert "Task Count:** 50" in result
        assert "GC Time:** 20.0%" in result
        assert "### Duration Quantiles" in result
        assert "### GC Time Quantiles" in result
        assert "### I/O" in result

    def test_includes_skew_ratio(self) -> None:
        stage = StageMetrics(
            stage_id=0,
            stage_name="skewed",
            tasks=TaskMetrics(
                task_count=100,
                distributions=TaskMetricsDistributions(
                    executor_run_time=Quantiles(min=10, p25=50, median=100, p75=200, max=1000),
                ),
            ),
        )

        result = format_stage_details(stage)

        assert "Skew Ratio:** 10.0x" in result


class TestFormatJobComparison:
    def _make_job(self, app_id: str, duration_ms: int, shuffle_read: int = 0) -> JobAnalysis:
        return JobAnalysis(
            app_id=app_id,
            duration_ms=duration_ms,
            config=SparkConfig(raw={}),
            stages=[
                StageMetrics(
                    stage_id=0,
                    stage_name="test",
                    total_shuffle_read_bytes=shuffle_read,
                    tasks=TaskMetrics(task_count=100),
                )
            ],
            executors=ExecutorMetrics(
                executor_count=4,
                peak_memory_bytes_sum=1024 * 1024 * 1024,
                allocated_memory_bytes_sum=2 * 1024 * 1024 * 1024,
            ),
        )

    def test_basic_comparison(self) -> None:
        job_a = self._make_job("app-a", 60000)
        job_b = self._make_job("app-b", 30000)

        result = format_job_comparison(job_a, job_b)

        assert "## Job Comparison" in result
        assert "app-a" in result
        assert "app-b" in result
        assert "Duration" in result

    def test_includes_all_metrics(self) -> None:
        job_a = self._make_job("app-a", 60000, shuffle_read=1024 * 1024 * 100)
        job_b = self._make_job("app-b", 30000, shuffle_read=1024 * 1024 * 50)

        result = format_job_comparison(job_a, job_b)

        assert "Total Tasks" in result
        assert "Shuffle Read" in result
        assert "Shuffle Write" in result
        assert "Spill to Disk" in result
        assert "Memory Utilization" in result
        assert "Executors" in result

    def test_delta_and_change_columns(self) -> None:
        job_a = self._make_job("app-a", 60000)
        job_b = self._make_job("app-b", 30000)

        result = format_job_comparison(job_a, job_b)

        assert "Delta" in result
        assert "Change" in result
        assert "-50.0%" in result
