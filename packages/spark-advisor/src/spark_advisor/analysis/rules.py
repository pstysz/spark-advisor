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
    def _check_stage(self, stage: StageMetrics, job: JobAnalysis) -> RuleResult | None: ...


# Detects stages where the slowest task takes disproportionately longer than the median,
# which means one partition holds much more data than others (e.g. hot key in a join).
class DataSkewRule(Rule):
    rule_id: ClassVar[str] = "data_skew"

    def _check_stage(self, stage: StageMetrics, job: JobAnalysis) -> RuleResult | None:
        ratio = stage.tasks.duration_skew_ratio
        if ratio < self._thresholds.skew_warning_ratio:
            return None

        severity = Severity.CRITICAL if ratio > self._thresholds.skew_critical_ratio else Severity.WARNING

        if job.config.aqe_enabled:
            fix = "AQE is enabled but skew persists — consider salting the join key"
        else:
            fix = "Enable AQE: spark.sql.adaptive.enabled=true, spark.sql.adaptive.skewJoin.enabled=true"

        run_time = stage.tasks.distributions.executor_run_time
        msg = (
            f"Max task duration ({run_time.max}ms) is "
            f"{ratio:.1f}x the median ({run_time.median}ms)"
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
        spill_bytes = stage.spill_to_disk_bytes
        if spill_bytes == 0:
            return None

        spill_gb = spill_bytes / (1024**3)

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
        gc_pct = stage.gc_time_percent
        if gc_pct < self._thresholds.gc_warning_percent:
            return None

        severity = Severity.CRITICAL if gc_pct > self._thresholds.gc_critical_percent else Severity.WARNING
        target = self._thresholds.gc_target_percent
        impact = f"Reducing GC to <{target:.0f}% could save ~{int(gc_pct - target)}% stage time"

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
            (s.total_shuffle_read_bytes for s in job.stages),
            default=0,
        )
        if max_shuffle == 0:
            return []

        optimal = max(1, int(max_shuffle / self._thresholds.target_partition_size_bytes))

        ratio = current_partitions / optimal if optimal > 0 else 1.0
        if self._thresholds.partition_ratio_min <= ratio <= self._thresholds.partition_ratio_max:
            return []

        shuffle_gb = max_shuffle / (1024**3)
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


# Detects over-provisioned clusters where executor slots sit idle most of the time.
# Slot utilization = total_task_time / (executor_count * cores * job_duration).
# Below 40% means you're paying for compute that's not doing work.
class ExecutorIdleRule(Rule):
    rule_id: ClassVar[str] = "executor_idle"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        if job.executors is None or job.executors.total_task_time_ms == 0 or job.duration_ms == 0:
            return []

        cores = job.config.executor_cores
        total_slot_time = job.executors.executor_count * cores * job.duration_ms
        if total_slot_time == 0:
            return []

        utilization = (job.executors.total_task_time_ms / total_slot_time) * 100
        if utilization > self._thresholds.min_slot_utilization_percent:
            return []

        idle_pct = 100 - utilization

        return [
            self._result(
                Severity.WARNING,
                "Executor over-provisioning",
                f"Slot utilization is only {utilization:.0f}% — {idle_pct:.0f}% idle",
                current_value=f"{job.executors.executor_count} executors x {cores} cores, {utilization:.0f}% utilized",
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
        failed = stage.failed_task_count
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
