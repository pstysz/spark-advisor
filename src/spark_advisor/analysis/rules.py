from abc import ABC, abstractmethod
from typing import ClassVar

from spark_advisor.config import Thresholds
from spark_advisor.model import RuleResult
from spark_advisor.model.metrics import JobAnalysis, StageMetrics
from spark_advisor.model.output import Severity


class Rule(ABC):
    rule_id: ClassVar[str]

    def __init__(self, thresholds: Thresholds | None = None) -> None:
        self._thresholds = thresholds or Thresholds()

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        results: list[RuleResult] = []
        for stage in job.stages:
            result = self._check_stage(stage, job)
            if result:
                results.append(result)
        return results

    def _result(
            self,
            severity: Severity,
            title: str,
            message: str,
            *,
            stage_id: int | None = None,
            current_value: str = "",
            recommended_value: str = "",
            estimated_impact: str = "",
    ) -> RuleResult:
        return RuleResult(
            rule_id=self.rule_id,
            severity=severity,
            title=title,
            message=message,
            stage_id=stage_id,
            current_value=current_value,
            recommended_value=recommended_value,
            estimated_impact=estimated_impact,
        )

    @abstractmethod
    def _check_stage(self, stage: StageMetrics, job: JobAnalysis) -> RuleResult | None:
        ...


# Detects stages where the slowest task takes disproportionately longer than the median,
# which means one partition holds much more data than others (e.g. hot key in a join).
# skew_ratio = max_task_duration / median_task_duration
class DataSkewRule(Rule):
    rule_id: ClassVar[str] = "data_skew"

    def _check_stage(self, stage: StageMetrics, job: JobAnalysis) -> RuleResult | None:
        ratio = stage.tasks.skew_ratio
        if ratio < self._thresholds.skew_warning_ratio:
            return None

        severity = (
            Severity.CRITICAL
            if ratio > self._thresholds.skew_critical_ratio
            else Severity.WARNING
        )

        if job.config.aqe_enabled:
            fix = "AQE is enabled but skew persists — consider salting the join key"
        else:
            fix = (
                "Enable AQE: spark.sql.adaptive.enabled=true, "
                "spark.sql.adaptive.skewJoin.enabled=true"
            )

        msg = (
            f"Max task duration ({stage.tasks.max_duration_ms}ms) is "
            f"{ratio:.1f}x the median ({stage.tasks.median_duration_ms}ms)"
        )
        impact = f"Stage {stage.stage_id} duration could decrease ~{int((1 - 1 / ratio) * 100)}%"

        return self._result(
            severity,
            f"Data skew in Stage {stage.stage_id}",
            msg,
            stage_id=stage.stage_id,
            current_value=f"skew ratio {ratio:.1f}x",
            recommended_value=fix,
            estimated_impact=impact,
        )


# Spill to disk happens when a partition's data exceeds the available execution memory,
# forcing Spark to serialize and write intermediate data to disk. Any spill > 0 means
# either executor memory is too low or there are too few shuffle partitions.
class SpillToDiskRule(Rule):
    rule_id: ClassVar[str] = "spill_to_disk"

    def _check_stage(self, stage: StageMetrics, job: JobAnalysis) -> RuleResult | None:
        spill_bytes = stage.tasks.spill_to_disk_bytes
        if spill_bytes == 0:
            return None

        spill_gb = spill_bytes / (1024 ** 3)

        if spill_gb > self._thresholds.spill_critical_gb:
            severity = Severity.CRITICAL
        elif spill_gb > self._thresholds.spill_warning_gb:
            severity = Severity.WARNING
        else:
            severity = Severity.INFO

        return self._result(
            severity,
            f"Disk spill in Stage {stage.stage_id}",
            f"{spill_gb:.1f} GB spilled to disk — data doesn't fit in memory",
            stage_id=stage.stage_id,
            current_value=f"{spill_gb:.1f} GB spill",
            recommended_value="Increase spark.executor.memory or spark.sql.shuffle.partitions",
            estimated_impact="Eliminating spill can speed up stage 2-10x",
        )


# High GC (garbage collection) time means the JVM executor is spending too much time
# reclaiming memory instead of doing useful work. Above 20% is a warning sign,
# above 40% the executor is essentially thrashing.
class GCPressureRule(Rule):
    rule_id: ClassVar[str] = "gc_pressure"

    def _check_stage(self, stage: StageMetrics, job: JobAnalysis) -> RuleResult | None:
        gc_pct = stage.tasks.gc_time_percent
        if gc_pct < self._thresholds.gc_warning_percent:
            return None

        severity = (
            Severity.CRITICAL
            if gc_pct > self._thresholds.gc_critical_percent
            else Severity.WARNING
        )
        impact = f"Reducing GC to <10% could save ~{int(gc_pct - 10)}% stage time"

        return self._result(
            severity,
            f"High GC pressure in Stage {stage.stage_id}",
            f"GC time is {gc_pct:.0f}% of total task time",
            stage_id=stage.stage_id,
            current_value=f"{gc_pct:.0f}% GC time",
            recommended_value="Increase executor memory or reduce data cached per task",
            estimated_impact=impact,
        )


# Checks if spark.sql.shuffle.partitions matches the actual data volume.
# The golden rule is ~128 MB per partition. Too few partitions → large partitions → spill.
# Too many → scheduling overhead and small-file problem.
# Operates on the whole job (biggest shuffle stage), not per-stage.
class ShufflePartitionsRule(Rule):
    rule_id: ClassVar[str] = "shuffle_partitions"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        current_partitions = job.config.shuffle_partitions

        max_shuffle = max(
            (s.tasks.total_shuffle_read_bytes for s in job.stages),
            default=0,
        )
        if max_shuffle == 0:
            return []

        optimal = max(1, int(max_shuffle / self._thresholds.target_partition_size_bytes))

        ratio = current_partitions / optimal if optimal > 0 else 1.0
        if self._thresholds.partition_ratio_min <= ratio <= self._thresholds.partition_ratio_max:
            return []

        shuffle_gb = max_shuffle / (1024 ** 3)
        if ratio < self._thresholds.partition_ratio_min:
            msg = f"Too few partitions: {current_partitions} for {shuffle_gb:.1f} GB shuffle"
            severity = Severity.WARNING
        else:
            msg = f"Too many partitions: {current_partitions} for {shuffle_gb:.1f} GB shuffle"
            severity = Severity.INFO

        return [
            self._result(
                severity,
                "Suboptimal shuffle partition count",
                msg,
                current_value=f"spark.sql.shuffle.partitions = {current_partitions}",
                recommended_value=f"spark.sql.shuffle.partitions = {optimal}",
                estimated_impact="Better partition sizing reduces spill and improves parallelism",
            )
        ]

    def _check_stage(self, stage: StageMetrics, job: JobAnalysis) -> RuleResult | None:
        return None


# Detects over-provisioned clusters where executors sit idle most of the time.
# CPU utilization below 40% means you're paying for compute that's not doing work —
# either reduce executor count or increase parallelism (more partitions).
class ExecutorIdleRule(Rule):
    rule_id: ClassVar[str] = "executor_idle"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        if job.executors is None:
            return []

        cpu_pct = job.executors.cpu_utilization_percent
        if cpu_pct is None or cpu_pct == 0 or cpu_pct > self._thresholds.min_cpu_utilization_percent:
            return []

        idle_pct = 100 - cpu_pct

        return [
            self._result(
                Severity.WARNING,
                "Executor over-provisioning",
                f"CPU utilization is only {cpu_pct:.0f}% — {idle_pct:.0f}% idle time",
                current_value=f"{job.executors.executor_count} executors, {cpu_pct:.0f}% CPU",
                recommended_value="Reduce executor count or increase parallelism",
                estimated_impact="Right-sizing can reduce cloud compute costs 30-50%",
            )
        ]

    def _check_stage(self, stage: StageMetrics, job: JobAnalysis) -> RuleResult | None:
        return None


# Any failed task is a red flag — usually means OOM kill, fetch failure (shuffle block
# lost because executor died), or network timeout. Even a single failure triggers a
# warning because it often indicates a systemic problem that will get worse at scale.
class TaskFailureRule(Rule):
    rule_id: ClassVar[str] = "task_failures"

    def _check_stage(self, stage: StageMetrics, job: JobAnalysis) -> RuleResult | None:
        failed = stage.tasks.failed_task_count
        if failed < self._thresholds.task_failure_warning_count:
            return None

        return self._result(
            Severity.WARNING,
            f"Task failures in Stage {stage.stage_id}",
            f"{failed} of {stage.tasks.task_count} tasks failed",
            stage_id=stage.stage_id,
            current_value=f"{failed} failed tasks",
            recommended_value="Check executor logs for OOM or fetch failures",
        )


def default_rules() -> list[Rule]:
    _default = Thresholds()
    return [
        DataSkewRule(_default),
        SpillToDiskRule(_default),
        GCPressureRule(_default),
        ShufflePartitionsRule(_default),
        ExecutorIdleRule(_default),
        TaskFailureRule(_default),
    ]


def rules_for_threshold(thresholds: Thresholds) -> list[Rule]:
    return [
        DataSkewRule(thresholds),
        SpillToDiskRule(thresholds),
        GCPressureRule(thresholds),
        ShufflePartitionsRule(thresholds),
        ExecutorIdleRule(thresholds),
        TaskFailureRule(thresholds),
    ]
