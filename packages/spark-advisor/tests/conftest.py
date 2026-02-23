import pytest

from spark_advisor.model.metrics import JobAnalysis, StageMetrics
from tests.factories import make_executors, make_job, make_stage


@pytest.fixture
def default_job() -> JobAnalysis:
    return make_job()


@pytest.fixture
def job_with_executors() -> JobAnalysis:
    return make_job(executors=make_executors())


@pytest.fixture
def skewed_stage() -> StageMetrics:
    return make_stage(stage_id=0, median_duration_ms=10, max_duration_ms=500)


@pytest.fixture
def spilling_stage() -> StageMetrics:
    return make_stage(stage_id=0, spill_to_disk_bytes=2 * 1024**3)


@pytest.fixture
def high_gc_stage() -> StageMetrics:
    return make_stage(stage_id=0, task_count=100, median_duration_ms=1000, total_gc_time_ms=50_000)
