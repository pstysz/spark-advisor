from pathlib import Path
from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from spark_advisor_cli.app import app
from spark_advisor_cli.commands.analyze import _resolve_ai_enabled
from spark_advisor_models.model import (
    AdvisorReport,
    AnalysisResult,
    Recommendation,
    Severity,
)
from spark_advisor_models.model.output import AnalysisMode

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SAMPLE_LOG = _REPO_ROOT / "sample_event_logs" / "sample_etl_job.json"

runner = CliRunner()


def _make_ai_result(app_id: str = "application_1234567890_0001") -> AnalysisResult:
    from spark_advisor_cli.event_log.parser import parse_event_log
    from spark_advisor_models.config import Thresholds
    from spark_advisor_rules import StaticAnalysisService, rules_for_threshold

    job = parse_event_log(SAMPLE_LOG)
    static = StaticAnalysisService(rules_for_threshold(Thresholds()))
    rule_results = static.analyze(job)

    return AnalysisResult(
        app_id=app_id,
        job=job,
        rule_results=rule_results,
        ai_report=AdvisorReport(
            app_id=app_id,
            summary="Test AI summary with recommendations.",
            severity=Severity.WARNING,
            rule_results=rule_results,
            recommendations=[
                Recommendation(
                    priority=1,
                    title="Increase executor memory",
                    parameter="spark.executor.memory",
                    current_value="1g",
                    recommended_value="4g",
                    explanation="Memory pressure detected.",
                    estimated_impact="~30% reduction in GC time",
                    risk="Higher resource cost",
                ),
            ],
            causal_chain="GC pressure → spill to disk",
            suggested_config={"spark.executor.memory": "4g"},
        ),
    )


class TestAnalyzeWithAI:
    @patch("spark_advisor_cli.commands.analyze._run_analysis")
    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"})
    def test_ai_enabled_when_api_key_set(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _make_ai_result()

        result = runner.invoke(app, ["analyze", str(SAMPLE_LOG)])

        assert result.exit_code == 0
        assert "AI Analysis" in result.output
        mock_run.assert_called_once()
        _, kwargs = mock_run.call_args
        assert kwargs["use_ai"] is True

    @patch("spark_advisor_cli.commands.analyze._run_analysis")
    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"})
    def test_no_ai_flag_disables_ai(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _make_ai_result()

        result = runner.invoke(app, ["analyze", str(SAMPLE_LOG), "--no-ai"])

        assert result.exit_code == 0
        _, kwargs = mock_run.call_args
        assert kwargs["use_ai"] is False

    @patch("spark_advisor_cli.commands.analyze._run_analysis")
    @patch.dict("os.environ", {}, clear=True)
    def test_ai_disabled_when_no_api_key(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _make_ai_result()

        result = runner.invoke(app, ["analyze", str(SAMPLE_LOG)])

        assert result.exit_code == 0
        _, kwargs = mock_run.call_args
        assert kwargs["use_ai"] is False

    @patch("spark_advisor_cli.commands.analyze._run_analysis")
    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"})
    def test_model_option_passed(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _make_ai_result()

        runner.invoke(app, ["analyze", str(SAMPLE_LOG), "--model", "claude-opus-4"])

        _, kwargs = mock_run.call_args
        assert kwargs["model"] == "claude-opus-4"

    @patch("spark_advisor_cli.commands.analyze._run_analysis")
    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"})
    def test_ai_error_shows_message(self, mock_run: MagicMock) -> None:
        mock_run.side_effect = Exception("Model did not call submit_analysis tool")

        result = runner.invoke(app, ["analyze", str(SAMPLE_LOG)])

        assert result.exit_code == 1
        assert "Analysis error" in result.output

    @patch("spark_advisor_cli.commands.analyze._run_analysis")
    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"})
    def test_ai_report_shows_recommendations(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _make_ai_result()

        result = runner.invoke(app, ["analyze", str(SAMPLE_LOG)])

        assert result.exit_code == 0
        assert "Increase executor memory" in result.output
        assert "spark.executor.memory" in result.output


class TestAgentMode:
    @patch.dict("os.environ", {}, clear=True)
    def test_agent_without_api_key_shows_error(self) -> None:
        result = runner.invoke(app, ["analyze", str(SAMPLE_LOG), "--agent"])

        assert result.exit_code == 1
        assert "ANTHROPIC_API_KEY" in result.output

    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"})
    def test_agent_with_no_ai_shows_error(self) -> None:
        result = runner.invoke(app, ["analyze", str(SAMPLE_LOG), "--agent", "--no-ai"])

        assert result.exit_code == 1
        assert "cannot use with --no-ai" in result.output

    @patch("spark_advisor_cli.commands.analyze._run_analysis")
    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"})
    def test_agent_flag_passes_agent_mode(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _make_ai_result()

        result = runner.invoke(app, ["analyze", str(SAMPLE_LOG), "--agent"])

        assert result.exit_code == 0
        _, kwargs = mock_run.call_args
        assert kwargs["mode"] == AnalysisMode.AGENT
        assert kwargs["use_ai"] is True

    @patch("spark_advisor_cli.commands.analyze._run_analysis")
    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"})
    def test_no_agent_flag_passes_standard_mode(self, mock_run: MagicMock) -> None:
        mock_run.return_value = _make_ai_result()

        result = runner.invoke(app, ["analyze", str(SAMPLE_LOG)])

        assert result.exit_code == 0
        _, kwargs = mock_run.call_args
        assert kwargs["mode"] == AnalysisMode.STANDARD


class TestResolveAiEnabled:
    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"})
    def test_enabled_when_key_present(self) -> None:
        assert _resolve_ai_enabled(no_ai=False) is True

    @patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"})
    def test_disabled_with_no_ai_flag(self) -> None:
        assert _resolve_ai_enabled(no_ai=True) is False

    @patch.dict("os.environ", {}, clear=True)
    def test_disabled_when_no_key(self) -> None:
        assert _resolve_ai_enabled(no_ai=False) is False
