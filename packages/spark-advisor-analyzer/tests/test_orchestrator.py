from unittest.mock import MagicMock

import pytest

from spark_advisor_analyzer.orchestrator import AdviceOrchestrator
from spark_advisor_models.model import AdvisorReport, AnalysisMode, AnalysisResult, Severity
from spark_advisor_models.testing import make_job, make_rule_result
from spark_advisor_rules import StaticAnalysisService


class TestAdviceOrchestrator:
    def test_rules_only_returns_result_without_ai(self) -> None:
        orchestrator = AdviceOrchestrator(StaticAnalysisService())
        result = orchestrator.run(make_job(), mode=AnalysisMode.STATIC)

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
        result = orchestrator.run(make_job(), mode=AnalysisMode.STATIC)

        assert isinstance(result.rule_results, list)

    def test_ai_mode_without_llm_raises_error(self) -> None:
        orchestrator = AdviceOrchestrator(StaticAnalysisService())
        with pytest.raises(ValueError, match="AI mode requested but no LlmAnalysisService"):
            orchestrator.run(make_job(), mode=AnalysisMode.AI)

    def test_use_agent_without_agent_raises_error(self) -> None:
        orchestrator = AdviceOrchestrator(StaticAnalysisService())
        with pytest.raises(ValueError, match="Agent mode requested but no AgentOrchestrator"):
            orchestrator.run(make_job(), mode=AnalysisMode.AGENT)

    def test_use_agent_delegates_to_agent(self) -> None:
        mock_agent = MagicMock()
        mock_agent.run.return_value = AnalysisResult(
            app_id="app-test-001",
            job=make_job(),
            rule_results=[],
        )

        orchestrator = AdviceOrchestrator(StaticAnalysisService(), agent=mock_agent)
        result = orchestrator.run(make_job(), mode=AnalysisMode.AGENT)

        mock_agent.run.assert_called_once()
        assert result.app_id == "app-test-001"
