from spark_advisor.ai.llm_service import ANALYSIS_TOOL
from spark_advisor.ai.prompts import SYSTEM_PROMPT, build_system_prompt, build_user_message
from spark_advisor.analysis.config import RuleThresholds
from spark_advisor.core import (
    ExecutorMetrics,
    JobAnalysis,
    RuleResult,
    Severity,
    SparkConfig,
    StageMetrics,
    TaskMetrics,
)


def _make_tasks(**overrides: object) -> TaskMetrics:
    defaults = {
        "task_count": 100,
        "median_duration_ms": 1000,
        "max_duration_ms": 2000,
        "min_duration_ms": 500,
        "total_gc_time_ms": 5000,
        "total_shuffle_read_bytes": 1024 * 1024 * 100,
        "total_shuffle_write_bytes": 1024 * 1024 * 50,
    }
    defaults.update(overrides)
    return TaskMetrics(**defaults)  # type: ignore[arg-type]


def _make_stage(stage_id: int = 0, **task_overrides: object) -> StageMetrics:
    return StageMetrics(
        stage_id=stage_id,
        stage_name=f"Stage {stage_id}",
        duration_ms=100_000,
        input_bytes=1024 * 1024 * 200,
        output_bytes=1024 * 1024 * 50,
        tasks=_make_tasks(**task_overrides),
    )


def _make_job(**overrides: object) -> JobAnalysis:
    defaults: dict[str, object] = {
        "app_id": "app-test-001",
        "app_name": "TestJob",
        "duration_ms": 300_000,
        "config": SparkConfig(raw={"spark.executor.memory": "4g", "spark.executor.cores": "4"}),
        "stages": [_make_stage(0)],
    }
    defaults.update(overrides)
    return JobAnalysis(**defaults)  # type: ignore[arg-type]


def _make_executors(**overrides: object) -> ExecutorMetrics:
    defaults = {
        "executor_count": 10,
        "peak_memory_bytes": 1024 * 1024 * 1024 * 2,
        "allocated_memory_bytes": 1024 * 1024 * 1024 * 4,
        "total_cpu_time_ms": 500_000,
        "total_run_time_ms": 1_000_000,
    }
    defaults.update(overrides)
    return ExecutorMetrics(**defaults)  # type: ignore[arg-type]


class TestAnalysisTool:
    def test_tool_has_required_top_level_fields(self) -> None:
        required = ANALYSIS_TOOL["input_schema"]["required"]
        assert "summary" in required
        assert "severity" in required
        assert "recommendations" in required
        assert "causal_chain" in required

    def test_recommendation_has_all_required_fields(self) -> None:
        rec_schema = ANALYSIS_TOOL["input_schema"]["properties"]["recommendations"]
        required = rec_schema["items"]["required"]
        assert "priority" in required
        assert "title" in required
        assert "parameter" in required
        assert "explanation" in required

    def test_severity_enum_values(self) -> None:
        severity = ANALYSIS_TOOL["input_schema"]["properties"]["severity"]
        assert severity["enum"] == ["critical", "warning", "info"]


class TestBuildSystemPrompt:
    def test_default_thresholds_injected(self) -> None:
        prompt = build_system_prompt()
        assert "> 5.0x" in prompt
        assert "> 10.0x" in prompt
        assert "> 20.0%" in prompt
        assert "> 40.0%" in prompt
        assert "< 40.0%" in prompt
        assert "~128MB" in prompt

    def test_custom_thresholds_injected(self) -> None:
        custom = RuleThresholds(
            skew_warning_ratio=3.0,
            skew_critical_ratio=8.0,
            gc_warning_percent=15.0,
            gc_critical_percent=30.0,
            min_cpu_utilization_percent=50.0,
            target_partition_size_bytes=64 * 1024 * 1024,
        )
        prompt = build_system_prompt(custom)
        assert "> 3.0x" in prompt
        assert "> 8.0x" in prompt
        assert "> 15.0%" in prompt
        assert "> 30.0%" in prompt
        assert "< 50.0%" in prompt
        assert "~64MB" in prompt

    def test_module_level_constant_uses_defaults(self) -> None:
        assert build_system_prompt() == SYSTEM_PROMPT

    def test_contains_no_problems_instruction(self) -> None:
        assert "no significant issues" in build_system_prompt()

    def test_contains_max_recommendations_limit(self) -> None:
        assert "at most 7" in build_system_prompt()

    def test_references_tool(self) -> None:
        assert "submit_analysis" in build_system_prompt()


class TestBuildUserMessage:
    def test_includes_config_values(self) -> None:
        job = _make_job()
        msg = build_user_message(job, [])
        assert "spark.executor.memory = 4g" in msg
        assert "spark.executor.cores = 4" in msg

    def test_includes_spark_version_when_present(self) -> None:
        job = _make_job(spark_version="3.5.1")
        msg = build_user_message(job, [])
        assert "Spark version: 3.5.1" in msg

    def test_excludes_spark_version_when_empty(self) -> None:
        job = _make_job(spark_version="")
        msg = build_user_message(job, [])
        assert "Spark version" not in msg

    def test_includes_job_overview(self) -> None:
        job = _make_job()
        msg = build_user_message(job, [])
        assert "app-test-001" in msg
        assert "300s" in msg
        assert "Stages: 1" in msg

    def test_includes_total_summary(self) -> None:
        job = _make_job()
        msg = build_user_message(job, [])
        assert "Total tasks: 100" in msg
        assert "Total input:" in msg
        assert "Total shuffle read:" in msg

    def test_includes_total_spill_only_when_nonzero(self) -> None:
        job = _make_job(stages=[_make_stage(0, spill_to_disk_bytes=1024 * 1024)])
        msg = build_user_message(job, [])
        assert "Total spill to disk:" in msg

        job_no_spill = _make_job()
        msg_no_spill = build_user_message(job_no_spill, [])
        assert "Total spill to disk:" not in msg_no_spill

    def test_all_stages_included(self) -> None:
        stages = [_make_stage(0), _make_stage(1), _make_stage(2)]
        job = _make_job(stages=stages)
        msg = build_user_message(job, [])
        assert "Stage 0" in msg
        assert "Stage 1" in msg
        assert "Stage 2" in msg

    def test_skew_flag_shown(self) -> None:
        stage = _make_stage(0, median_duration_ms=100, max_duration_ms=800)
        job = _make_job(stages=[stage])
        msg = build_user_message(job, [])
        assert "SKEW(8.0x)" in msg

    def test_skew_flag_uses_thresholds(self) -> None:
        stage = _make_stage(0, median_duration_ms=100, max_duration_ms=400)
        job = _make_job(stages=[stage])
        msg_default = build_user_message(job, [])
        assert "SKEW" not in msg_default

        custom = RuleThresholds(skew_warning_ratio=3.0)
        msg_custom = build_user_message(job, [], thresholds=custom)
        assert "SKEW(4.0x)" in msg_custom

    def test_spill_flag_shown(self) -> None:
        stage = _make_stage(0, spill_to_disk_bytes=1024 * 1024 * 500)
        job = _make_job(stages=[stage])
        msg = build_user_message(job, [])
        assert "SPILL" in msg

    def test_gc_flag_shown(self) -> None:
        stage = _make_stage(0, total_gc_time_ms=50_000, median_duration_ms=100, task_count=100)
        job = _make_job(stages=[stage])
        msg = build_user_message(job, [])
        assert "GC(" in msg

    def test_gc_flag_uses_thresholds(self) -> None:
        stage = _make_stage(0, total_gc_time_ms=25_000, median_duration_ms=1000, task_count=100)
        job = _make_job(stages=[stage])
        msg_default = build_user_message(job, [])
        assert "GC(25%)" in msg_default

        custom = RuleThresholds(gc_warning_percent=30.0)
        msg_custom = build_user_message(job, [], thresholds=custom)
        assert "GC(" not in msg_custom

    def test_failures_flag_shown(self) -> None:
        stage = _make_stage(0, failed_task_count=5)
        job = _make_job(stages=[stage])
        msg = build_user_message(job, [])
        assert "FAILURES(5)" in msg

    def test_no_flags_when_healthy(self) -> None:
        job = _make_job()
        msg = build_user_message(job, [])
        assert "[" not in msg.split("Stage 0")[1].split("\n")[0]

    def test_stage_includes_min_median_max_duration(self) -> None:
        job = _make_job()
        msg = build_user_message(job, [])
        assert "min=500ms" in msg
        assert "median=1000ms" in msg
        assert "max=2000ms" in msg

    def test_stage_includes_input_output(self) -> None:
        job = _make_job()
        msg = build_user_message(job, [])
        assert "Input:" in msg
        assert "Output:" in msg

    def test_includes_executor_metrics(self) -> None:
        job = _make_job(executors=_make_executors())
        msg = build_user_message(job, [])
        assert "Count: 10" in msg
        assert "Memory utilization:" in msg
        assert "CPU utilization:" in msg

    def test_includes_rule_results(self) -> None:
        rules = [
            RuleResult(
                rule_id="test_rule",
                severity=Severity.WARNING,
                title="Test Issue",
                message="Something is wrong",
            )
        ]
        job = _make_job()
        msg = build_user_message(job, rules)
        assert "Issues Detected by Rules Engine" in msg
        assert "WARNING: Test Issue" in msg
        assert "Something is wrong" in msg

    def test_no_rule_section_when_empty(self) -> None:
        job = _make_job()
        msg = build_user_message(job, [])
        assert "Issues Detected" not in msg
