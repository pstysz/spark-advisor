from pathlib import Path

import pytest
from rich.console import Console

from spark_advisor_cli.output.console import print_analysis_result
from spark_advisor_models.config import Thresholds
from spark_advisor_models.defaults import DEFAULT_THRESHOLDS
from spark_advisor_models.model import (
    AdvisorReport,
    AnalysisMode,
    AnalysisResult,
    Recommendation,
    RuleResult,
    Severity,
)
from spark_advisor_models.testing.factories import make_executors, make_job, make_rule_result, make_stage


def _capture(
    result: AnalysisResult,
    *,
    mode: AnalysisMode = AnalysisMode.AI,
    verbose: bool = False,
    output_config: Path | None = None,
    thresholds: Thresholds = DEFAULT_THRESHOLDS,
) -> str:
    console = Console(file=None, force_terminal=True, width=200)
    with console.capture() as capture:
        print_analysis_result(
            console,
            result,
            mode=mode,
            verbose=verbose,
            output_config=output_config,
            thresholds=thresholds,
        )
    return capture.get()


def _make_result(
    *,
    rule_results: list[RuleResult] | None = None,
    ai_report: AdvisorReport | None = None,
    **job_overrides: object,
) -> AnalysisResult:
    job = make_job(**job_overrides)
    return AnalysisResult(
        app_id=job.app_id,
        job=job,
        rule_results=rule_results or [],
        ai_report=ai_report,
    )


class TestJobOverview:
    def test_shows_app_id(self) -> None:
        output = _capture(_make_result())
        assert "app-test-001" in output

    def test_shows_app_name(self) -> None:
        output = _capture(_make_result(app_name="MySparkJob"))
        assert "MySparkJob" in output

    def test_shows_duration(self) -> None:
        output = _capture(_make_result(duration_ms=120_000))
        assert "2.0 min" in output

    def test_shows_stage_count(self) -> None:
        output = _capture(_make_result(stages=[make_stage(0), make_stage(1)]))
        assert "2" in output

    def test_shows_executor_count(self) -> None:
        output = _capture(_make_result(executors=make_executors(executor_count=8)))
        assert "8" in output

    def test_shows_shuffle_partitions(self) -> None:
        output = _capture(_make_result(config={"spark.sql.shuffle.partitions": "400"}))
        assert "400" in output


class TestStageBreakdown:
    def test_hidden_by_default(self) -> None:
        output = _capture(_make_result(), verbose=False)
        assert "Stage Breakdown" not in output

    def test_shown_when_verbose(self) -> None:
        output = _capture(_make_result(), verbose=True)
        assert "Stage Breakdown" in output

    def test_gc_normal_no_color(self) -> None:
        stage = make_stage(0, total_gc_time_ms=1000, sum_executor_run_time_ms=100_000)
        output = _capture(_make_result(stages=[stage]), verbose=True)
        assert "bold red" not in output

    def test_gc_warning_yellow(self) -> None:
        stage = make_stage(0, total_gc_time_ms=25_000, sum_executor_run_time_ms=100_000)
        result = _make_result(stages=[stage])
        console = Console(file=None, force_terminal=True, width=200, color_system="truecolor")
        with console.capture() as capture:
            print_analysis_result(console, result, mode=AnalysisMode.AI, verbose=True, thresholds=DEFAULT_THRESHOLDS)
        assert "25%" in capture.get()

    def test_gc_critical_red(self) -> None:
        stage = make_stage(0, total_gc_time_ms=50_000, sum_executor_run_time_ms=100_000)
        result = _make_result(stages=[stage])
        console = Console(file=None, force_terminal=True, width=200, color_system="truecolor")
        with console.capture() as capture:
            print_analysis_result(console, result, mode=AnalysisMode.AI, verbose=True, thresholds=DEFAULT_THRESHOLDS)
        assert "50%" in capture.get()

    def test_gc_uses_custom_thresholds(self) -> None:
        stage = make_stage(0, total_gc_time_ms=15_000, sum_executor_run_time_ms=100_000)
        custom = Thresholds(gc_warning_percent=10.0, gc_critical_percent=30.0)
        output = _capture(_make_result(stages=[stage]), verbose=True, thresholds=custom)
        assert "15%" in output

    def test_spill_shown_when_nonzero(self) -> None:
        stage = make_stage(0, spill_to_disk_bytes=500 * 1024 * 1024)
        output = _capture(_make_result(stages=[stage]), verbose=True)
        assert "500.0 MB" in output

    def test_spill_dash_when_zero(self) -> None:
        stage = make_stage(0, spill_to_disk_bytes=0)
        output = _capture(_make_result(stages=[stage]), verbose=True)
        assert "-" in output


class TestRuleResults:
    def test_no_issues_message(self) -> None:
        output = _capture(_make_result(rule_results=[]))
        assert "No issues detected" in output

    def test_shows_rule_title_and_message(self) -> None:
        rule = make_rule_result(title="Data skew in Stage 0", message="Max is 10x median")
        output = _capture(_make_result(rule_results=[rule]))
        assert "Data skew in Stage 0" in output
        assert "Max is 10x median" in output

    def test_shows_recommended_value(self) -> None:
        rule = make_rule_result(recommended_value="spark.sql.shuffle.partitions=800")
        output = _capture(_make_result(rule_results=[rule]))
        assert "spark.sql.shuffle.partitions=800" in output

    def test_shows_severity_icons(self) -> None:
        critical = make_rule_result(severity=Severity.CRITICAL, title="Critical issue")
        warning = make_rule_result(severity=Severity.WARNING, title="Warning issue", rule_id="gc_pressure")
        output = _capture(_make_result(rule_results=[critical, warning]))
        assert "CRITICAL" in output
        assert "WARNING" in output


class TestAiReport:
    @pytest.fixture()
    def ai_report(self) -> AdvisorReport:
        return AdvisorReport(
            app_id="app-test-001",
            summary="Memory pressure causing spill to disk",
            severity=Severity.WARNING,
            rule_results=[],
            recommendations=[
                Recommendation(
                    priority=1,
                    title="Increase executor memory",
                    parameter="spark.executor.memory",
                    current_value="4g",
                    recommended_value="8g",
                    explanation="Current memory is insufficient",
                    estimated_impact="50% reduction in spill",
                ),
            ],
            causal_chain="Low memory → GC pressure → Spill to disk",
            suggested_config={"spark.executor.memory": "8g"},
        )

    def test_shows_summary(self, ai_report: AdvisorReport) -> None:
        output = _capture(_make_result(ai_report=ai_report))
        assert "Memory pressure causing spill to disk" in output

    def test_shows_causal_chain(self, ai_report: AdvisorReport) -> None:
        output = _capture(_make_result(ai_report=ai_report))
        assert "Low memory" in output

    def test_shows_recommendation(self, ai_report: AdvisorReport) -> None:
        output = _capture(_make_result(ai_report=ai_report))
        assert "Increase executor memory" in output
        assert "8g" in output

    def test_shows_suggested_config(self, ai_report: AdvisorReport) -> None:
        output = _capture(_make_result(ai_report=ai_report))
        assert "spark.executor.memory" in output

    def test_empty_recommendations_skips_report(self) -> None:
        report = AdvisorReport(
            app_id="app-test-001",
            summary="All good",
            severity=Severity.INFO,
            rule_results=[],
            recommendations=[],
        )
        output = _capture(_make_result(ai_report=report))
        assert "AI Analysis" not in output

    def test_saves_config_file(self, ai_report: AdvisorReport, tmp_path: Path) -> None:
        config_path = tmp_path / "spark-defaults.conf"
        _capture(_make_result(ai_report=ai_report), output_config=config_path)
        content = config_path.read_text()
        assert "spark.executor.memory = 8g" in content


class TestModeHints:
    def test_static_mode_shows_ai_disabled_hint(self) -> None:
        output = _capture(_make_result(), mode=AnalysisMode.STATIC)
        assert "AI analysis disabled" in output

    def test_ai_mode_no_disabled_hint(self) -> None:
        output = _capture(_make_result(), mode=AnalysisMode.AI)
        assert "AI analysis disabled" not in output

    def test_static_mode_output_warning(self) -> None:
        output = _capture(_make_result(), mode=AnalysisMode.STATIC, output_config=Path("out.conf"))
        assert "Warning" in output
        assert "--output requires AI" in output

    def test_ai_mode_no_output_warning(self) -> None:
        output = _capture(_make_result(), mode=AnalysisMode.AI, output_config=Path("out.conf"))
        assert "--output requires AI" not in output

    def test_no_rules_no_ai_shows_skip_message(self) -> None:
        output = _capture(_make_result(rule_results=[]), mode=AnalysisMode.AI)
        assert "No issues found" in output

    def test_no_rules_static_mode_no_skip_message(self) -> None:
        output = _capture(_make_result(rule_results=[]), mode=AnalysisMode.STATIC)
        assert "skipping AI analysis" not in output
