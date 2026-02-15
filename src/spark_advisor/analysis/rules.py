"""Deterministic rules engine for Spark job analysis.

Each rule encodes expert knowledge about Spark performance problems.
Rules are fast, free (no API calls), and always correct.
The AI layer builds on top of these results.
"""

from abc import ABC, abstractmethod

from spark_advisor.models import JobAnalysis, RuleResult, Severity

# ──────────────────────────────────────────────
# Base rule
# ──────────────────────────────────────────────


class Rule(ABC):
    """Base class for all analysis rules — equivalent of Kotlin abstract class."""

    @property
    @abstractmethod
    def rule_id(self) -> str: ...

    @abstractmethod
    def evaluate(self, job: JobAnalysis) -> list[RuleResult]: ...


# ──────────────────────────────────────────────
# Concrete rules
# ──────────────────────────────────────────────


class DataSkewRule(Rule):
    """Detects data skew — when some tasks process much more data than others."""

    SKEW_THRESHOLD = 5.0

    @property
    def rule_id(self) -> str:
        return "data_skew"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        results: list[RuleResult] = []
        for stage in job.stages:
            ratio = stage.tasks.skew_ratio
            if ratio < self.SKEW_THRESHOLD:
                continue

            severity = Severity.CRITICAL if ratio > 10 else Severity.WARNING

            aqe_fix = ""
            if not job.config.aqe_enabled:
                aqe_fix = (
                    "Enable AQE: spark.sql.adaptive.enabled=true, "
                    "spark.sql.adaptive.skewJoin.enabled=true"
                )
            else:
                aqe_fix = "AQE is enabled but skew persists — consider salting the join key"

            results.append(
                RuleResult(
                    rule_id=self.rule_id,
                    severity=severity,
                    title=f"Data skew in Stage {stage.stage_id}",
                    message=(
                        f"Max task duration ({stage.tasks.max_duration_ms}ms) is "
                        f"{ratio:.1f}x the median ({stage.tasks.median_duration_ms}ms)"
                    ),
                    stage_id=stage.stage_id,
                    current_value=f"skew ratio {ratio:.1f}x",
                    recommended_value=aqe_fix,
                    estimated_impact=(
                        f"Stage {stage.stage_id} duration could decrease"
                        f" ~{int((1 - 1/ratio) * 100)}%"
                    ),
                )
            )

        return results


class SpillToDiskRule(Rule):
    """Detects disk spill — data that doesn't fit in memory during shuffle/aggregation."""

    @property
    def rule_id(self) -> str:
        return "spill_to_disk"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        results: list[RuleResult] = []
        for stage in job.stages:
            spill_bytes = stage.tasks.spill_to_disk_bytes
            if spill_bytes == 0:
                continue

            spill_gb = spill_bytes / (1024**3)

            results.append(
                RuleResult(
                    rule_id=self.rule_id,
                    severity=Severity.CRITICAL if spill_gb > 1 else Severity.WARNING,
                    title=f"Disk spill in Stage {stage.stage_id}",
                    message=f"{spill_gb:.1f} GB spilled to disk — data doesn't fit in memory",
                    stage_id=stage.stage_id,
                    current_value=f"{spill_gb:.1f} GB spill",
                    recommended_value=(
                        "Increase spark.executor.memory"
                        " or spark.sql.shuffle.partitions"
                    ),
                    estimated_impact="Eliminating spill can speed up stage 2-10x",
                )
            )

        return results


class GCPressureRule(Rule):
    """Detects excessive garbage collection time."""

    GC_THRESHOLD_PERCENT = 20.0

    @property
    def rule_id(self) -> str:
        return "gc_pressure"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        results: list[RuleResult] = []
        for stage in job.stages:
            gc_pct = stage.tasks.gc_time_percent
            if gc_pct < self.GC_THRESHOLD_PERCENT:
                continue

            results.append(
                RuleResult(
                    rule_id=self.rule_id,
                    severity=Severity.CRITICAL if gc_pct > 40 else Severity.WARNING,
                    title=f"High GC pressure in Stage {stage.stage_id}",
                    message=f"GC time is {gc_pct:.0f}% of total task time",
                    stage_id=stage.stage_id,
                    current_value=f"{gc_pct:.0f}% GC time",
                    recommended_value="Increase executor memory or reduce data cached per task",
                    estimated_impact=(
                        f"Reducing GC to <10% could save"
                        f" ~{int(gc_pct - 10)}% stage time"
                    ),
                )
            )

        return results


class ShufflePartitionsRule(Rule):
    """Checks if shuffle partition count is appropriate for the data volume."""

    TARGET_PARTITION_SIZE_BYTES = 128 * 1024 * 1024  # 128 MB

    @property
    def rule_id(self) -> str:
        return "shuffle_partitions"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        current_partitions = job.config.shuffle_partitions

        max_shuffle = max(
            (s.tasks.total_shuffle_read_bytes for s in job.stages),
            default=0,
        )
        if max_shuffle == 0:
            return []

        optimal = max(1, int(max_shuffle / self.TARGET_PARTITION_SIZE_BYTES))

        ratio = current_partitions / optimal if optimal > 0 else 1.0
        if 0.5 <= ratio <= 2.0:
            return []

        if ratio < 0.5:
            shuffle_gb = max_shuffle / (1024**3)
            msg = (
                f"Too few partitions: {current_partitions}"
                f" for {shuffle_gb:.1f} GB shuffle"
            )
            severity = Severity.WARNING
        else:
            shuffle_gb = max_shuffle / (1024**3)
            msg = (
                f"Too many partitions: {current_partitions}"
                f" for {shuffle_gb:.1f} GB shuffle"
            )
            severity = Severity.INFO

        return [
            RuleResult(
                rule_id=self.rule_id,
                severity=severity,
                title="Suboptimal shuffle partition count",
                message=msg,
                current_value=f"spark.sql.shuffle.partitions = {current_partitions}",
                recommended_value=f"spark.sql.shuffle.partitions = {optimal}",
                estimated_impact="Better partition sizing reduces spill and improves parallelism",
            )
        ]


class ExecutorIdleRule(Rule):
    """Detects over-provisioned executors based on CPU utilization."""

    IDLE_THRESHOLD_PERCENT = 60.0

    @property
    def rule_id(self) -> str:
        return "executor_idle"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        if job.executors is None:
            return []

        cpu_pct = job.executors.cpu_utilization_percent
        if cpu_pct == 0 or cpu_pct > (100 - self.IDLE_THRESHOLD_PERCENT):
            return []

        idle_pct = 100 - cpu_pct

        return [
            RuleResult(
                rule_id=self.rule_id,
                severity=Severity.WARNING,
                title="Executor over-provisioning",
                message=f"CPU utilization is only {cpu_pct:.0f}% — {idle_pct:.0f}% idle time",
                current_value=f"{job.executors.executor_count} executors, {cpu_pct:.0f}% CPU",
                recommended_value="Reduce executor count or increase parallelism",
                estimated_impact="Right-sizing can reduce cloud compute costs 30-50%",
            )
        ]


# ──────────────────────────────────────────────
# Engine
# ──────────────────────────────────────────────

DEFAULT_RULES: list[Rule] = [
    DataSkewRule(),
    SpillToDiskRule(),
    GCPressureRule(),
    ShufflePartitionsRule(),
    ExecutorIdleRule(),
]


def run_rules(job: JobAnalysis, rules: list[Rule] | None = None) -> list[RuleResult]:
    """Execute all rules against a job and return combined results, sorted by severity."""
    active_rules = rules or DEFAULT_RULES

    all_results: list[RuleResult] = []
    for rule in active_rules:
        all_results.extend(rule.evaluate(job))

    severity_order = {Severity.CRITICAL: 0, Severity.WARNING: 1, Severity.INFO: 2}
    all_results.sort(key=lambda r: severity_order.get(r.severity, 99))

    return all_results
