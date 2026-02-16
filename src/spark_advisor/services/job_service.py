from pathlib import Path

from spark_advisor.ai.llm_service import llm_client
from spark_advisor.analysis.config import DEFAULT_MODEL
from spark_advisor.analysis.rules import apply_static_rules
from spark_advisor.core import AnalysisResult, JobAnalysis
from spark_advisor.sources.event_log import EventLogParser
from spark_advisor.sources.history_server_client import history_server_client


class JobService:
    def __init__(self, history_server_url: str | None = None):
        self._history_server_url = history_server_url

    def load_job(self, app_id: str) -> JobAnalysis:
        if self._history_server_url:
            with history_server_client(self._history_server_url) as client:
                return client.fetch(app_id)

        path = Path(app_id)
        if not path.exists():
            raise FileNotFoundError(f"Event log file not found: {app_id}")

        return EventLogParser.parse(path)

    def analyze(
        self,
        job: JobAnalysis,
        ai_enabled: bool = False,
        model: str = DEFAULT_MODEL,
    ) -> AnalysisResult:

        ai_report = None
        rule_results = apply_static_rules(job)
        if ai_enabled and rule_results:
            with llm_client() as client:
                ai_report = client.analyze(job, rule_results, model)

        return AnalysisResult(
            job=job,
            rule_results=rule_results,
            ai_report=ai_report,
        )

    def list_applications(self, limit: int = 10) -> list[dict[str, str]]:
        if not self._history_server_url:
            raise RuntimeError("History Server URL not configured")
        with history_server_client(self._history_server_url) as client:
            return client.list_applications(limit=limit)
