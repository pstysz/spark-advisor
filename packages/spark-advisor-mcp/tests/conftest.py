from pathlib import Path

import pytest

from spark_advisor_models.model import JobAnalysis

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SAMPLE_LOG = _REPO_ROOT / "sample_event_logs" / "sample_etl_job.json"


@pytest.fixture
def sample_job() -> JobAnalysis:
    from spark_advisor_cli.event_log.parser import parse_event_log

    return parse_event_log(SAMPLE_LOG)
