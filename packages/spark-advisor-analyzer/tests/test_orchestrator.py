import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "spark-advisor-models" / "tests"))

from factories import make_job, make_rule_result

from spark_advisor_analyzer.orchestrator import AdviceOrchestrator
from spark_advisor_models.model import AdvisorReport, AnalysisResult, Severity
from spark_advisor_rules import StaticAnalysisService


class TestAdviceOrchestrator:
    def test_rules_only_returns_result_without_ai(self) -> None:
        orchestrator = AdviceOrchestrator(StaticAnalysisService())
        result = orchestrator.run(make_job())

        assert isinstance(result, AnalysisResult)
        assert result.app_id == "app-test-001"
        assert result.ai_report is None

    def test_rules_plus_ai_returns_full_result(self) -> None:
        mock_llm = MagicMock()
        mock_llm.analyze.return_value = AdvisorReport(
            app_id="app-test-001",
            summary="Test summary",
            severity=Severity.WARNING,
            rule_results=[make_rule_result()],
            recommendations=[],
            causal_chain="",
            suggested_config={},
        )

        orchestrator = AdviceOrchestrator(StaticAnalysisService(), mock_llm)
        result = orchestrator.run(make_job())

        assert result.ai_report is not None
        assert result.ai_report.summary == "Test summary"
        mock_llm.analyze.assert_called_once()

    def test_rule_results_always_present(self) -> None:
        orchestrator = AdviceOrchestrator(StaticAnalysisService())
        result = orchestrator.run(make_job())

        assert isinstance(result.rule_results, list)
