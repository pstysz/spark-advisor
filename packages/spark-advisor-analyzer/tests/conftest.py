import pytest

from spark_advisor_models.model import JobAnalysis, StageMetrics
from spark_advisor_models.testing import make_executors, make_job, make_stage


@pytest.fixture
def default_job() -> JobAnalysis:
    return make_job()


@pytest.fixture
def job_with_executors() -> JobAnalysis:
    return make_job(executors=make_executors())


@pytest.fixture
def skewed_stage() -> StageMetrics:
    return make_stage(0, run_time_median=10, run_time_max=500)


@pytest.fixture
def spilling_stage() -> StageMetrics:
    return make_stage(0, spill_to_disk_bytes=2 * 1024**3)


@pytest.fixture
def high_gc_stage() -> StageMetrics:
    return make_stage(0, task_count=100, sum_executor_run_time_ms=100_000, total_gc_time_ms=50_000)
