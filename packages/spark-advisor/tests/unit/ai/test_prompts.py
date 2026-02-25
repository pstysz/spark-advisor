from spark_advisor.ai.config import ANALYSIS_TOOL
from spark_advisor.ai.prompts_builder import build_system_prompt, build_user_message
from spark_advisor.config import Thresholds
from spark_advisor.model import (
    RuleResult,
    Severity,
)
from tests.factories import make_executors, make_job, make_stage


class TestAnalysisTool:
    def test_tool_has_required_top_level_fields(self) -> None:
        required = ANALYSIS_TOOL["input_schema"]["required"]
        assert "summary" in required
        assert "severity" in required
        assert "recommendations" in required
        assert "causal_chain" in required

    def test_recommendation_has_all_required_fields(self) -> None:
        rec_schema = ANALYSIS_TOOL["input_schema"]["$defs"]["RecommendationInput"]
        required = rec_schema["required"]
        assert "priority" in required
        assert "title" in required
        assert "parameter" in required
        assert "explanation" in required

    def test_severity_enum_values(self) -> None:
        severity = ANALYSIS_TOOL["input_schema"]["properties"]["severity"]
        assert severity["enum"] == ["critical", "warning", "info"]
        assert severity["type"] == "string"


class TestBuildSystemPrompt:
    def test_default_thresholds_injected(self) -> None:
        prompt = build_system_prompt()
        assert "> 5.0x" in prompt
        assert "> 10.0x" in prompt
        assert ">20.0%" in prompt
        assert ">40.0%" in prompt
        assert "<40.0%" in prompt
        assert "~128MB" in prompt

    def test_custom_thresholds_injected(self) -> None:
        custom = Thresholds(
            skew_warning_ratio=3.0,
            skew_critical_ratio=8.0,
            gc_warning_percent=15.0,
            gc_critical_percent=30.0,
            min_slot_utilization_percent=50.0,
            target_partition_size_bytes=64 * 1024 * 1024,
        )
        prompt = build_system_prompt(custom)
        assert "> 3.0x" in prompt
        assert "> 8.0x" in prompt
        assert ">15.0%" in prompt
        assert ">30.0%" in prompt
        assert "<50.0%" in prompt
        assert "~64MB" in prompt

    def test_contains_no_problems_instruction(self) -> None:
        assert "no significant issues" in build_system_prompt()

    def test_contains_max_recommendations_limit(self) -> None:
        assert "at most 7" in build_system_prompt()

    def test_references_tool(self) -> None:
        assert "submit_analysis" in build_system_prompt()


class TestBuildUserMessage:
    def test_includes_config_values(self) -> None:
        job = make_job()
        msg = build_user_message(job, [])
        assert "spark.executor.memory = 4g" in msg
        assert "spark.executor.cores = 4" in msg

    def test_includes_spark_version_when_present(self) -> None:
        job = make_job(spark_version="3.5.1")
        msg = build_user_message(job, [])
        assert "Spark version: 3.5.1" in msg

    def test_excludes_spark_version_when_empty(self) -> None:
        job = make_job(spark_version="")
        msg = build_user_message(job, [])
        assert "Spark version" not in msg

    def test_includes_job_overview(self) -> None:
        job = make_job()
        msg = build_user_message(job, [])
        assert "app-test-001" in msg
        assert "300s" in msg
        assert "Stages: 1" in msg

    def test_includes_total_summary(self) -> None:
        job = make_job()
        msg = build_user_message(job, [])
        assert "Total tasks: 100" in msg
        assert "Total input:" in msg
        assert "Total shuffle read:" in msg

    def test_includes_total_spill_only_when_nonzero(self) -> None:
        job = make_job(stages=[make_stage(0, spill_to_disk_bytes=1024 * 1024)])
        msg = build_user_message(job, [])
        assert "Total spill to disk:" in msg

        job_no_spill = make_job()
        msg_no_spill = build_user_message(job_no_spill, [])
        assert "Total spill to disk:" not in msg_no_spill

    def test_all_stages_included(self) -> None:
        stages = [make_stage(0), make_stage(1), make_stage(2)]
        job = make_job(stages=stages)
        msg = build_user_message(job, [])
        assert "Stage 0" in msg
        assert "Stage 1" in msg
        assert "Stage 2" in msg

    def test_skew_flag_shown(self) -> None:
        stage = make_stage(0, run_time_median=100, run_time_max=800)
        job = make_job(stages=[stage])
        msg = build_user_message(job, [])
        assert "SKEW(8.0x)" in msg

    def test_skew_flag_uses_thresholds(self) -> None:
        stage = make_stage(0, run_time_median=100, run_time_max=400)
        job = make_job(stages=[stage])
        msg_default = build_user_message(job, [])
        assert "SKEW" not in msg_default

        custom = Thresholds(skew_warning_ratio=3.0)
        msg_custom = build_user_message(job, [], thresholds=custom)
        assert "SKEW(4.0x)" in msg_custom

    def test_spill_flag_shown(self) -> None:
        stage = make_stage(0, spill_to_disk_bytes=1024 * 1024 * 500)
        job = make_job(stages=[stage])
        msg = build_user_message(job, [])
        assert "SPILL" in msg

    def test_gc_flag_shown(self) -> None:
        stage = make_stage(0, sum_executor_run_time_ms=100_000, total_gc_time_ms=50_000, task_count=100)
        job = make_job(stages=[stage])
        msg = build_user_message(job, [])
        assert "GC(" in msg

    def test_gc_flag_uses_thresholds(self) -> None:
        stage = make_stage(0, sum_executor_run_time_ms=100_000, total_gc_time_ms=25_000, task_count=100)
        job = make_job(stages=[stage])
        msg_default = build_user_message(job, [])
        assert "GC(25%)" in msg_default

        custom = Thresholds(gc_warning_percent=30.0)
        msg_custom = build_user_message(job, [], thresholds=custom)
        assert "GC(" not in msg_custom

    def test_failures_flag_shown(self) -> None:
        stage = make_stage(0, failed_task_count=5)
        job = make_job(stages=[stage])
        msg = build_user_message(job, [])
        assert "FAILURES(5)" in msg

    def test_no_flags_when_healthy(self) -> None:
        job = make_job()
        msg = build_user_message(job, [])
        assert "[" not in msg.split("Stage 0")[1].split("\n")[0]

    def test_stage_includes_min_median_max_duration(self) -> None:
        job = make_job()
        msg = build_user_message(job, [])
        assert "Task duration (wall-clock):" in msg
        assert "min=500ms" in msg
        assert "median=1000ms" in msg
        assert "max=2000ms" in msg

    def test_stage_includes_input_output(self) -> None:
        job = make_job()
        msg = build_user_message(job, [])
        assert "Input:" in msg
        assert "Output:" in msg

    def test_includes_executor_metrics(self) -> None:
        job = make_job(executors=make_executors())
        msg = build_user_message(job, [])
        assert "Count: 10" in msg
        assert "Memory:" in msg
        assert "utilization)" in msg
        assert "Slot utilization:" in msg

    def test_includes_rule_results(self) -> None:
        rules = [
            RuleResult(
                rule_id="test_rule",
                severity=Severity.WARNING,
                title="Test Issue",
                message="Something is wrong",
            )
        ]
        job = make_job()
        msg = build_user_message(job, rules)
        assert "Issues Detected by Rules Engine" in msg
        assert "WARNING: Test Issue" in msg
        assert "Something is wrong" in msg

    def test_no_rule_section_when_empty(self) -> None:
        job = make_job()
        msg = build_user_message(job, [])
        assert "Issues Detected" not in msg

    def test_includes_executor_run_time(self) -> None:
        job = make_job()
        msg = build_user_message(job, [])
        assert "Executor run time (compute):" in msg

    def test_includes_record_counts_when_nonzero(self) -> None:
        stage = make_stage(0, input_records=1_000_000, output_records=500_000)
        job = make_job(stages=[stage])
        msg = build_user_message(job, [])
        assert "Input records: 1,000,000" in msg
        assert "Output records: 500,000" in msg

    def test_excludes_record_counts_when_zero(self) -> None:
        job = make_job()
        msg = build_user_message(job, [])
        assert "Input records:" not in msg
        assert "Output records:" not in msg

    def test_includes_shuffle_record_counts(self) -> None:
        stage = make_stage(0, shuffle_read_records=2_000_000, shuffle_write_records=1_500_000)
        job = make_job(stages=[stage])
        msg = build_user_message(job, [])
        assert "Shuffle read records: 2,000,000" in msg
        assert "Shuffle write records: 1,500,000" in msg

    def test_includes_peak_execution_memory(self) -> None:
        stage = make_stage(0, peak_memory_max=1024 * 1024 * 512)
        job = make_job(stages=[stage])
        msg = build_user_message(job, [])
        assert "Peak execution memory:" in msg

    def test_excludes_peak_memory_when_zero(self) -> None:
        job = make_job()
        msg = build_user_message(job, [])
        assert "Peak execution memory:" not in msg

    def test_includes_scheduler_delay_when_high(self) -> None:
        stage = make_stage(0, scheduler_delay_max=500)
        job = make_job(stages=[stage])
        msg = build_user_message(job, [])
        assert "Scheduler delay:" in msg

    def test_excludes_scheduler_delay_when_low(self) -> None:
        stage = make_stage(0, scheduler_delay_max=50)
        job = make_job(stages=[stage])
        msg = build_user_message(job, [])
        assert "Scheduler delay:" not in msg

    def test_includes_spill_to_memory(self) -> None:
        stage = make_stage(0, spill_to_memory_bytes=500 * 1024 * 1024)
        job = make_job(stages=[stage])
        msg = build_user_message(job, [])
        assert "Spill to memory:" in msg
        assert "Total spill to memory:" in msg

    def test_killed_flag_shown(self) -> None:
        stage = make_stage(0, killed_task_count=3)
        job = make_job(stages=[stage])
        msg = build_user_message(job, [])
        assert "KILLED(3)" in msg

    def test_executor_total_cores_shown(self) -> None:
        job = make_job(executors=make_executors(total_cores=40))
        msg = build_user_message(job, [])
        assert "Total cores: 40" in msg

    def test_executor_gc_time_shown_when_nonzero(self) -> None:
        job = make_job(executors=make_executors(total_gc_time_ms=45_000))
        msg = build_user_message(job, [])
        assert "Total GC time: 45.0s" in msg

    def test_executor_failed_tasks_shown_when_nonzero(self) -> None:
        job = make_job(executors=make_executors(failed_tasks=5))
        msg = build_user_message(job, [])
        assert "Failed tasks: 5" in msg

    def test_expanded_config_keys(self) -> None:
        config = {
            "spark.executor.memory": "4g",
            "spark.executor.memoryOverhead": "1g",
            "spark.sql.autoBroadcastJoinThreshold": "10485760",
            "spark.dynamicAllocation.maxExecutors": "100",
        }
        job = make_job(config=config)
        msg = build_user_message(job, [])
        assert "spark.executor.memoryOverhead = 1g" in msg
        assert "spark.sql.autoBroadcastJoinThreshold = 10485760" in msg
        assert "spark.dynamicAllocation.maxExecutors = 100" in msg

    def test_system_prompt_contains_metrics_guide(self) -> None:
        prompt = build_system_prompt()
        assert "METRICS GUIDE" in prompt
        assert "Task duration (wall-clock)" in prompt
        assert "Executor run time (compute)" in prompt
        assert "Peak execution memory" in prompt
        assert "Scheduler delay" in prompt
        assert "Spill to memory" in prompt
        assert "Record counts" in prompt

    def test_system_prompt_contains_key_configs(self) -> None:
        prompt = build_system_prompt()
        assert "spark.executor.memoryOverhead" in prompt
        assert "spark.sql.autoBroadcastJoinThreshold" in prompt
        assert "spark.memory.fraction" in prompt
        assert "spark.speculation" in prompt
        assert "spark.io.compression.codec" in prompt
