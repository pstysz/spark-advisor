from abc import ABC
from typing import ClassVar

from spark_advisor_models.config import Thresholds
from spark_advisor_models.defaults import DEFAULT_THRESHOLDS
from spark_advisor_models.model import JobAnalysis, RuleResult, Severity, StageMetrics


class Rule(ABC):
    rule_id: ClassVar[str]

    def __init__(self, thresholds: Thresholds | None = None) -> None:
        self._thresholds = thresholds or DEFAULT_THRESHOLDS

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

    def _check_stage(self, stage: StageMetrics, job: JobAnalysis) -> RuleResult | None:
        return None


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
        msg = f"Max task duration ({run_time.max}ms) is {ratio:.1f}x the median ({run_time.median}ms)"
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


# Detects over-provisioned clusters where executor slots sit idle most of the time.
# Slot utilization = total_task_time / (executor_count * cores * job_duration).
# Below 40% means you're paying for compute that's not doing work.
class ExecutorIdleRule(Rule):
    rule_id: ClassVar[str] = "executor_idle"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        if job.executors is None:
            return []

        utilization = job.executors.slot_utilization_percent(job.duration_ms, job.config.executor_cores)
        if utilization is None or utilization > self._thresholds.min_slot_utilization_percent:
            return []

        severity = (
            Severity.CRITICAL
            if utilization < self._thresholds.min_slot_utilization_critical_percent
            else Severity.WARNING
        )
        idle_pct = 100 - utilization
        cores = job.config.executor_cores

        return [
            self._result(
                severity,
                "Executor over-provisioning",
                f"Slot utilization is only {utilization:.0f}% — {idle_pct:.0f}% idle",
                current_value=f"{job.executors.executor_count} executors x {cores} cores, {utilization:.0f}% utilized",
                recommended_value="Reduce executor count or increase parallelism",
                estimated_impact="Right-sizing can reduce cloud compute costs 30-50%",
            )
        ]


# Any failed task is a red flag — usually means OOM kill, fetch failure (shuffle block
# lost because executor died), or network timeout. Even a single failure triggers a
# warning because it often indicates a systemic problem that will get worse at scale.
class TaskFailureRule(Rule):
    rule_id: ClassVar[str] = "task_failures"

    def _check_stage(self, stage: StageMetrics, job: JobAnalysis) -> RuleResult | None:
        failed = stage.failed_task_count
        if failed < self._thresholds.task_failure_warning_count:
            return None

        severity = Severity.CRITICAL if failed >= self._thresholds.task_failure_critical_count else Severity.WARNING

        return self._result(
            severity,
            f"Task failures in Stage {stage.stage_id}",
            f"{failed} of {stage.tasks.task_count} tasks failed",
            stage_id=stage.stage_id,
            current_value=f"{failed} failed tasks",
            recommended_value="Check executor logs for OOM or fetch failures",
        )


# Small average input per task means either too many small source files or over-partitioned reads.
# Thousands of tiny tasks create excessive scheduler overhead (launch cost dominates compute).
# CRITICAL below 1 MB/task (extreme overhead), WARNING below 10 MB/task.
class SmallFileRule(Rule):
    rule_id: ClassVar[str] = "small_files"

    def _check_stage(self, stage: StageMetrics, job: JobAnalysis) -> RuleResult | None:
        if stage.input_bytes == 0 or stage.tasks.task_count == 0:
            return None
        avg_input = stage.input_bytes / stage.tasks.task_count
        if avg_input >= self._thresholds.small_file_threshold_bytes:
            return None
        severity = Severity.CRITICAL if avg_input < self._thresholds.small_file_critical_bytes else Severity.WARNING
        avg_mb = avg_input / (1024 * 1024)
        return self._result(
            severity,
            f"Small input partitions in Stage {stage.stage_id}",
            f"Average input per task is {avg_mb:.1f} MB across {stage.tasks.task_count} tasks",
            stage_id=stage.stage_id,
            current_value=f"{avg_mb:.1f} MB/task, {stage.tasks.task_count} tasks",
            recommended_value="Use coalesce() or increase spark.sql.files.maxPartitionBytes",
            estimated_impact="Reducing task count cuts scheduler overhead and improves throughput",
        )


# Checks spark.sql.autoBroadcastJoinThreshold — when a small table is below this size,
# Spark broadcasts it to all executors instead of shuffling both sides of the join.
# Disabled (-1) with shuffle stages is a WARNING because shuffle joins on small tables
# waste network I/O. A low threshold (< 10 MB) with shuffle stages is an INFO hint.
class BroadcastJoinThresholdRule(Rule):
    rule_id: ClassVar[str] = "broadcast_join"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        threshold = job.config.broadcast_join_threshold
        has_shuffle = any(s.total_shuffle_read_bytes > 0 for s in job.stages)

        if threshold == -1:
            severity = Severity.WARNING if has_shuffle else Severity.INFO
            return [
                self._result(
                    severity,
                    "Broadcast join disabled",
                    "spark.sql.autoBroadcastJoinThreshold is -1 (disabled)",
                    current_value="disabled (-1)",
                    recommended_value=(
                        f"spark.sql.autoBroadcastJoinThreshold = {self._thresholds.broadcast_join_default_bytes}"
                    ),
                    estimated_impact="Broadcasting small tables avoids expensive shuffle joins",
                )
            ]

        if has_shuffle and threshold < self._thresholds.broadcast_join_default_bytes:
            threshold_mb = threshold / (1024 * 1024)
            default_mb = self._thresholds.broadcast_join_default_bytes / (1024 * 1024)
            return [
                self._result(
                    Severity.INFO,
                    "Low broadcast join threshold",
                    f"Threshold is {threshold_mb:.0f} MB — some shuffle joins might be avoidable",
                    current_value=f"{threshold_mb:.0f} MB",
                    recommended_value=(
                        f"spark.sql.autoBroadcastJoinThreshold = "
                        f"{self._thresholds.broadcast_join_default_bytes} ({default_mb:.0f} MB)"
                    ),
                    estimated_impact="Larger threshold may convert shuffle joins to broadcast joins",
                )
            ]

        return []


# Checks if the job uses the default Java serializer with shuffle-heavy stages.
# KryoSerializer is typically 10x faster and produces more compact output, significantly
# reducing shuffle data size and network transfer time. Only triggers when shuffle is present
# because non-shuffle jobs don't benefit from serializer changes.
class SerializerChoiceRule(Rule):
    rule_id: ClassVar[str] = "serializer_choice"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        serializer = job.config.serializer
        if "KryoSerializer" in serializer:
            return []

        has_shuffle = any(s.total_shuffle_read_bytes > 0 or s.total_shuffle_write_bytes > 0 for s in job.stages)
        if not has_shuffle:
            return []

        return [
            self._result(
                Severity.INFO,
                "Using default Java serializer",
                f"Current serializer: {serializer}",
                current_value=serializer,
                recommended_value="spark.serializer = org.apache.spark.serializer.KryoSerializer",
                estimated_impact="Kryo serialization is typically 10x faster and more compact",
            )
        ]


# Validates dynamic allocation configuration. Two scenarios:
# 1) Dynamic allocation ON but without min/maxExecutors bounds — unbounded scaling can
#    over-provision in shared clusters, wasting resources and starving other jobs.
# 2) Dynamic allocation OFF with low slot utilization — suggests enabling it so Spark
#    can auto-scale executors to match actual workload instead of paying for idle slots.
class DynamicAllocationRule(Rule):
    rule_id: ClassVar[str] = "dynamic_allocation"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        results: list[RuleResult] = []

        if job.config.dynamic_allocation_enabled:
            min_exec = job.config.dynamic_allocation_min_executors
            max_exec = job.config.dynamic_allocation_max_executors
            if not min_exec or not max_exec:
                missing = []
                if not min_exec:
                    missing.append("minExecutors")
                if not max_exec:
                    missing.append("maxExecutors")
                results.append(
                    self._result(
                        Severity.WARNING,
                        "Dynamic allocation without bounds",
                        f"Dynamic allocation enabled but {', '.join(missing)} not set",
                        current_value="spark.dynamicAllocation.enabled = true",
                        recommended_value=", ".join(f"spark.dynamicAllocation.{m} = <value>" for m in missing),
                        estimated_impact="Unbounded dynamic allocation can over-provision resources",
                    )
                )
        else:
            if job.executors is not None:
                utilization = job.executors.slot_utilization_percent(job.duration_ms, job.config.executor_cores)
                if utilization is not None and utilization < self._thresholds.min_slot_utilization_percent:
                    results.append(
                        self._result(
                            Severity.WARNING,
                            "Consider enabling dynamic allocation",
                            f"Slot utilization is {utilization:.0f}% with fixed executor count",
                            current_value=(
                                f"{job.executors.executor_count} static executors, {utilization:.0f}% utilized"
                            ),
                            recommended_value="spark.dynamicAllocation.enabled = true",
                            estimated_impact="Dynamic allocation auto-scales executors to match workload",
                        )
                    )

        return results


# Detects when executor memory overhead (off-heap) is likely too low. Triggers when BOTH
# conditions are true: GC pressure above threshold AND memory utilization above threshold.
# High GC + high memory means the JVM heap is nearly full and struggling — increasing
# spark.executor.memoryOverhead gives more room for off-heap data (broadcast vars, shuffle
# buffers, internal Spark structures) and reduces container OOM kills on YARN/K8s.
class ExecutorMemoryOverheadRule(Rule):
    rule_id: ClassVar[str] = "executor_memory_overhead"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        if job.executors is None:
            return []

        mem_util = job.executors.memory_utilization_percent
        has_gc_pressure = any(
            s.gc_time_percent > self._thresholds.memory_overhead_gc_threshold_percent for s in job.stages
        )

        if not has_gc_pressure or mem_util < self._thresholds.memory_overhead_mem_utilization_percent:
            return []

        overhead = job.config.executor_memory_overhead
        current_desc = f"memoryOverhead = {overhead}" if overhead else "memoryOverhead = default (max(384MB, 10%))"

        return [
            self._result(
                Severity.WARNING,
                "Executor memory overhead may be too low",
                f"GC pressure detected with {mem_util:.0f}% memory utilization",
                current_value=current_desc,
                recommended_value="Increase spark.executor.memoryOverhead (try 20-25% of executor memory)",
                estimated_impact="More overhead memory reduces GC pressure and OOM risk",
            )
        ]


def rules_for_threshold(thresholds: Thresholds) -> list[Rule]:
    return [
        DataSkewRule(thresholds),
        SpillToDiskRule(thresholds),
        GCPressureRule(thresholds),
        ShufflePartitionsRule(thresholds),
        ExecutorIdleRule(thresholds),
        TaskFailureRule(thresholds),
        SmallFileRule(thresholds),
        BroadcastJoinThresholdRule(thresholds),
        SerializerChoiceRule(thresholds),
        DynamicAllocationRule(thresholds),
        ExecutorMemoryOverheadRule(thresholds),
    ]
