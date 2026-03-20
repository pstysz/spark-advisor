import json
from pathlib import Path

from typer.testing import CliRunner

from spark_advisor_cli.app import app
from spark_advisor_models.config import Thresholds
from spark_advisor_models.model import AnalysisResult
from spark_advisor_parser import parse_event_log
from spark_advisor_rules import StaticAnalysisService, rules_for_threshold

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SAMPLE_LOG = _REPO_ROOT / "sample_event_logs" / "sample_etl_job.json"

runner = CliRunner()


def _run_local_analysis(source: Path) -> AnalysisResult:
    job = parse_event_log(source)
    thresholds = Thresholds()
    static = StaticAnalysisService(rules_for_threshold(thresholds))
    rule_results = static.analyze(job)
    return AnalysisResult(app_id=job.app_id, job=job, rule_results=rule_results, ai_report=None)


class TestLocalAnalysis:
    def test_produces_analysis_result(self) -> None:
        result = _run_local_analysis(SAMPLE_LOG)
        assert isinstance(result, AnalysisResult)
        assert result.app_id == "application_1234567890_0001"

    def test_ai_report_is_none(self) -> None:
        result = _run_local_analysis(SAMPLE_LOG)
        assert result.ai_report is None

    def test_detects_issues(self) -> None:
        result = _run_local_analysis(SAMPLE_LOG)
        assert len(result.rule_results) > 0

    def test_job_data_attached(self) -> None:
        result = _run_local_analysis(SAMPLE_LOG)
        assert result.job is not None
        assert len(result.job.stages) == 2


class TestJsonOutput:
    def test_json_format_produces_valid_json(self) -> None:
        result = runner.invoke(app, ["analyze", str(SAMPLE_LOG), "--format", "json"])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["app_id"] == "application_1234567890_0001"
        assert "rule_results" in data
        assert "job" in data

    def test_json_format_contains_rules(self) -> None:
        result = runner.invoke(app, ["analyze", str(SAMPLE_LOG), "--format", "json"])
        data = json.loads(result.output)
        assert isinstance(data["rule_results"], list)
        assert len(data["rule_results"]) > 0


class TestVerboseOutput:
    def test_verbose_shows_stage_table(self) -> None:
        result = runner.invoke(app, ["analyze", str(SAMPLE_LOG), "--verbose"])
        assert result.exit_code == 0
        assert "Stage Breakdown" in result.output

    def test_verbose_not_shown_by_default(self) -> None:
        result = runner.invoke(app, ["analyze", str(SAMPLE_LOG)])
        assert result.exit_code == 0
        assert "Stage Breakdown" not in result.output
