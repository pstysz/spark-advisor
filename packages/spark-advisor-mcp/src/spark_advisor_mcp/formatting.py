from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from spark_advisor_models.util.bytes import format_bytes

if TYPE_CHECKING:
    from spark_advisor_models.model import AnalysisResult, ApplicationSummary, JobAnalysis, RuleResult, SparkConfig
    from spark_advisor_models.model.metrics import StageMetrics


def format_job_overview(job: JobAnalysis) -> str:
    duration_s = job.duration_ms / 1000
    duration_min = job.duration_ms / 60_000
    total_tasks = sum(s.tasks.task_count for s in job.stages)
    total_shuffle_read = sum(s.total_shuffle_read_bytes for s in job.stages)
    total_shuffle_write = sum(s.total_shuffle_write_bytes for s in job.stages)
    total_spill = sum(s.spill_to_disk_bytes for s in job.stages)
    total_gc = sum(s.total_gc_time_ms for s in job.stages)
    total_run_time = sum(s.sum_executor_run_time_ms for s in job.stages)

    lines = [
        f"## Job Overview: {job.app_id}",
        "",
        f"- **App Name:** {job.app_name or 'N/A'}",
        f"- **Spark Version:** {job.spark_version or 'N/A'}",
        f"- **Duration:** {duration_min:.1f} min ({duration_s:.0f}s)",
        f"- **Stages:** {len(job.stages)}",
        f"- **Total Tasks:** {total_tasks}",
    ]

    if job.executors:
        lines.append(f"- **Executors:** {job.executors.executor_count}")
        lines.append(f"- **Memory Utilization:** {job.executors.memory_utilization_percent:.1f}%")
        slot_util = job.executors.slot_utilization_percent(job.duration_ms, job.config.executor_cores)
        if slot_util is not None:
            lines.append(f"- **Slot Utilization:** {slot_util:.1f}%")

    lines.append(f"- **Shuffle Partitions:** {job.config.shuffle_partitions}")
    lines.append(f"- **Executor Memory:** {job.config.executor_memory}")
    lines.append(f"- **Executor Cores:** {job.config.executor_cores}")
    lines.append("")
    lines.append("### Quick Stats")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Total Shuffle Read | {format_bytes(total_shuffle_read)} |")
    lines.append(f"| Total Shuffle Write | {format_bytes(total_shuffle_write)} |")
    lines.append(f"| Total Spill to Disk | {format_bytes(total_spill)} |")
    gc_pct = (total_gc / total_run_time * 100) if total_run_time > 0 else 0.0
    lines.append(f"| Aggregate GC Time | {gc_pct:.1f}% of task time |")

    return "\n".join(lines)


def format_rule_results(rule_results: list[RuleResult]) -> str:
    if not rule_results:
        return "No issues detected by rules engine."

    lines = ["## Rule Findings", ""]
    for r in rule_results:
        severity_icon = {"CRITICAL": "🔴", "WARNING": "🟡", "INFO": "🔵"}.get(r.severity.value, "⚪")
        stage_prefix = f"[Stage {r.stage_id}] " if r.stage_id is not None else ""
        lines.append(f"### {severity_icon} [{r.severity}] {stage_prefix}{r.title}")
        lines.append("")
        lines.append(r.message)
        if r.current_value and r.recommended_value:
            lines.append(f"- **Current:** `{r.current_value}`")
            lines.append(f"- **Recommended:** `{r.recommended_value}`")
        if r.estimated_impact:
            lines.append(f"- **Impact:** {r.estimated_impact}")
        lines.append("")
    return "\n".join(lines)


def format_analysis_result(result: AnalysisResult, *, use_ai: bool) -> str:
    parts = [
        format_job_overview(result.job),
        "",
        format_rule_results(result.rule_results),
    ]

    if use_ai and result.ai_report:
        report = result.ai_report
        parts.append("## AI Analysis")
        parts.append("")
        parts.append(f"**Overall Severity:** {report.severity}")
        parts.append("")
        parts.append(report.summary)
        parts.append("")

        if report.causal_chain:
            parts.append(f"**Causal Chain:** {report.causal_chain}")
            parts.append("")

        if report.recommendations:
            parts.append("### Recommendations")
            parts.append("")
            for rec in report.recommendations:
                parts.append(f"**{rec.priority}. {rec.title}**")
                if rec.parameter:
                    parts.append(f"- Parameter: `{rec.parameter}`")
                if rec.current_value:
                    parts.append(f"- Current: `{rec.current_value}`")
                if rec.recommended_value:
                    parts.append(f"- Recommended: `{rec.recommended_value}`")
                if rec.explanation:
                    parts.append(f"- {rec.explanation}")
                if rec.estimated_impact:
                    parts.append(f"- Impact: {rec.estimated_impact}")
                parts.append("")

        if report.suggested_config:
            parts.append("### Suggested spark-defaults.conf")
            parts.append("")
            parts.append("```properties")
            for key, value in sorted(report.suggested_config.items()):
                parts.append(f"{key}={value}")
            parts.append("```")

    return "\n".join(parts)


_CONFIG_CATEGORIES: list[tuple[str, list[str]]] = [
    ("Executor", ["spark.executor.", "spark.driver."]),
    ("Shuffle & Partitioning", ["spark.sql.shuffle.", "spark.shuffle.", "spark.reducer."]),
    ("SQL & AQE", ["spark.sql.adaptive.", "spark.sql.autoBroadcast", "spark.sql.files."]),
    ("Dynamic Allocation", ["spark.dynamicAllocation."]),
    ("Serialization", ["spark.serializer", "spark.kryoserializer.", "spark.kryo."]),
    ("Memory", ["spark.memory."]),
]


def _categorize_key(key: str) -> str:
    for category_name, prefixes in _CONFIG_CATEGORIES:
        if any(key.startswith(p) for p in prefixes):
            return category_name
    return "Other"


def format_config_table(config: SparkConfig) -> str:
    categorized: dict[str, list[tuple[str, str]]] = {}
    for key in sorted(config.raw):
        category = _categorize_key(key)
        categorized.setdefault(category, []).append((key, config.raw[key]))

    lines = ["## Spark Configuration", ""]

    category_order = [name for name, _ in _CONFIG_CATEGORIES] + ["Other"]
    for category in category_order:
        entries = categorized.get(category)
        if not entries:
            continue
        lines.append(f"### {category}")
        lines.append("")
        lines.append("| Key | Value |")
        lines.append("|-----|-------|")
        for key, value in entries:
            lines.append(f"| `{key}` | `{value}` |")
        lines.append("")

    return "\n".join(lines)


def format_suggested_config(rule_results: list[RuleResult]) -> str:
    rows: list[tuple[str, int | None, str, str]] = []
    for r in rule_results:
        if r.recommended_value and r.current_value:
            rows.append((r.rule_id, r.stage_id, r.current_value, r.recommended_value))

    if not rows:
        return "No configuration changes suggested — all checks passed."

    lines = [
        "## Suggested Configuration Changes",
        "",
        "| Rule | Stage | Current | Recommended |",
        "|------|-------|---------|-------------|",
    ]
    for rule_id, stage_id, current, recommended in rows:
        stage_col = f"Stage {stage_id}" if stage_id is not None else "-"
        lines.append(f"| `{rule_id}` | {stage_col} | `{current}` | `{recommended}` |")

    return "\n".join(lines)


def format_scan_results(apps: list[ApplicationSummary]) -> str:
    if not apps:
        return "No applications found."

    lines = [
        f"## Recent Spark Applications ({len(apps)})",
        "",
        "| App ID | Name | Duration | Status | Spark Version |",
        "|--------|------|----------|--------|---------------|",
    ]
    for app in apps:
        latest = app.latest_attempt
        duration = "-"
        status = ""
        spark_ver = ""
        if latest:
            if latest.duration > 0:
                duration = f"{latest.duration / 60_000:.1f} min"
            status = "completed" if latest.completed else "running"
            spark_ver = latest.app_spark_version
        lines.append(f"| `{app.id}` | {app.name} | {duration} | {status} | {spark_ver} |")
    return "\n".join(lines)


def format_stage_details(stage: StageMetrics) -> str:
    rt = stage.tasks.distributions.executor_run_time
    gc = stage.tasks.distributions.jvm_gc_time
    sr = stage.tasks.distributions.shuffle_read_metrics.read_bytes
    sw = stage.tasks.distributions.shuffle_write_metrics.write_bytes
    disk_spill = stage.tasks.distributions.disk_bytes_spilled
    inp = stage.tasks.distributions.input_metrics.bytes

    lines = [
        f"## Stage {stage.stage_id}: {stage.stage_name}",
        "",
        f"- **Task Count:** {stage.tasks.task_count}",
        f"- **Failed Tasks:** {stage.failed_task_count}",
        f"- **Killed Tasks:** {stage.killed_task_count}",
        f"- **GC Time:** {stage.gc_time_percent:.1f}% of task time",
        f"- **Skew Ratio:** {stage.tasks.duration_skew_ratio:.1f}x (max/median duration)",
        "",
        "### Duration Quantiles (ms)",
        "",
        "| Min | P25 | Median | P75 | Max |",
        "|-----|-----|--------|-----|-----|",
        f"| {rt.min:.0f} | {rt.p25:.0f} | {rt.median:.0f} | {rt.p75:.0f} | {rt.max:.0f} |",
        "",
        "### GC Time Quantiles (ms)",
        "",
        "| Min | P25 | Median | P75 | Max |",
        "|-----|-----|--------|-----|-----|",
        f"| {gc.min:.0f} | {gc.p25:.0f} | {gc.median:.0f} | {gc.p75:.0f} | {gc.max:.0f} |",
        "",
        "### I/O",
        "",
        "| Metric | Total | Per-task Quantiles (min / median / max) |",
        "|--------|-------|----------------------------------------|",
        f"| Shuffle Read | {format_bytes(stage.total_shuffle_read_bytes)}"
        f" | {format_bytes(int(sr.min))} / {format_bytes(int(sr.median))} / {format_bytes(int(sr.max))} |",
        f"| Shuffle Write | {format_bytes(stage.total_shuffle_write_bytes)}"
        f" | {format_bytes(int(sw.min))} / {format_bytes(int(sw.median))} / {format_bytes(int(sw.max))} |",
        f"| Spill to Disk | {format_bytes(stage.spill_to_disk_bytes)}"
        f" | {format_bytes(int(disk_spill.min))} / {format_bytes(int(disk_spill.median))}"
        f" / {format_bytes(int(disk_spill.max))} |",
        f"| Input | {format_bytes(stage.input_bytes)}"
        f" | {format_bytes(int(inp.min))} / {format_bytes(int(inp.median))} / {format_bytes(int(inp.max))} |",
        f"| Output | {format_bytes(stage.output_bytes)} | - |",
    ]

    return "\n".join(lines)


def _format_delta(val_a: float, val_b: float, fmt: Literal["bytes", "ms", "pct", "int"] = "bytes") -> tuple[str, str]:
    delta = val_b - val_a
    if val_a == 0:
        pct = "N/A"
    else:
        pct_val = (delta / val_a) * 100
        sign = "+" if pct_val >= 0 else ""
        pct = f"{sign}{pct_val:.1f}%"

    if fmt == "bytes":
        if delta == 0:
            delta_str = "0 B"
        else:
            sign_chr = "+" if delta >= 0 else "-"
            delta_str = f"{sign_chr}{format_bytes(abs(int(delta)))}"
        return delta_str, pct
    if fmt == "ms":
        delta_s = delta / 1000
        sign_chr = "+" if delta_s >= 0 else "-"
        return f"{sign_chr}{abs(delta_s):.1f}s", pct
    if fmt == "pct":
        sign_chr = "+" if delta >= 0 else ""
        return f"{sign_chr}{delta:.1f}%", pct

    sign_chr = "+" if delta >= 0 else ""
    return f"{sign_chr}{delta:.0f}", pct


def format_job_comparison(job_a: JobAnalysis, job_b: JobAnalysis) -> str:
    total_tasks_a = sum(s.tasks.task_count for s in job_a.stages)
    total_tasks_b = sum(s.tasks.task_count for s in job_b.stages)
    shuffle_r_a = sum(s.total_shuffle_read_bytes for s in job_a.stages)
    shuffle_r_b = sum(s.total_shuffle_read_bytes for s in job_b.stages)
    shuffle_w_a = sum(s.total_shuffle_write_bytes for s in job_a.stages)
    shuffle_w_b = sum(s.total_shuffle_write_bytes for s in job_b.stages)
    spill_a = sum(s.spill_to_disk_bytes for s in job_a.stages)
    spill_b = sum(s.spill_to_disk_bytes for s in job_b.stages)

    rows: list[tuple[str, str, str, str, str]] = []

    d_dur, p_dur = _format_delta(job_a.duration_ms, job_b.duration_ms, "ms")
    rows.append((
        "Duration",
        f"{job_a.duration_ms / 1000:.0f}s",
        f"{job_b.duration_ms / 1000:.0f}s",
        d_dur,
        p_dur,
    ))

    d_tasks, p_tasks = _format_delta(total_tasks_a, total_tasks_b, "int")
    rows.append(("Total Tasks", str(total_tasks_a), str(total_tasks_b), d_tasks, p_tasks))

    d_sr, p_sr = _format_delta(shuffle_r_a, shuffle_r_b)
    rows.append(("Shuffle Read", format_bytes(shuffle_r_a), format_bytes(shuffle_r_b), d_sr, p_sr))

    d_sw, p_sw = _format_delta(shuffle_w_a, shuffle_w_b)
    rows.append(("Shuffle Write", format_bytes(shuffle_w_a), format_bytes(shuffle_w_b), d_sw, p_sw))

    d_sp, p_sp = _format_delta(spill_a, spill_b)
    rows.append(("Spill to Disk", format_bytes(spill_a), format_bytes(spill_b), d_sp, p_sp))

    if job_a.executors and job_b.executors:
        mem_a = job_a.executors.memory_utilization_percent
        mem_b = job_b.executors.memory_utilization_percent
        d_mem, p_mem = _format_delta(mem_a, mem_b, "pct")
        rows.append((
            "Memory Utilization",
            f"{mem_a:.1f}%",
            f"{mem_b:.1f}%",
            d_mem,
            p_mem,
        ))

        d_exc, p_exc = _format_delta(
            job_a.executors.executor_count, job_b.executors.executor_count, "int"
        )
        rows.append((
            "Executors",
            str(job_a.executors.executor_count),
            str(job_b.executors.executor_count),
            d_exc,
            p_exc,
        ))

    lines = [
        f"## Job Comparison: `{job_a.app_id}` vs `{job_b.app_id}`",
        "",
        "| Metric | Job A | Job B | Delta | Change |",
        "|--------|-------|-------|-------|--------|",
    ]
    for metric, val_a, val_b, delta, pct in rows:
        lines.append(f"| {metric} | {val_a} | {val_b} | {delta} | {pct} |")

    return "\n".join(lines)
