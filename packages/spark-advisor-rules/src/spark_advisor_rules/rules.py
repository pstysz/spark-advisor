from abc import ABC
from collections import Counter
from typing import ClassVar

from spark_advisor_models.config import Thresholds
from spark_advisor_models.defaults import DEFAULT_THRESHOLDS
from spark_advisor_models.model import JobAnalysis, RuleResult, Severity, StageMetrics
from spark_advisor_models.util import format_bytes, parse_memory_string, parse_spark_version


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
        if stage.tasks.task_count < self._thresholds.min_task_count_for_skew:
            return None

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

        if stage.input_bytes > 0:
            spill_ratio = spill_bytes / stage.input_bytes
            if spill_ratio < self._thresholds.spill_negligible_ratio:
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
        if stage.sum_executor_run_time_ms < self._thresholds.gc_min_stage_runtime_ms:
            return None

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
        version = parse_spark_version(job.spark_version)
        if job.config.aqe_enabled and version is not None and version >= (3, 2):
            return []

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
            if job.config.aqe_enabled:
                severity = Severity.INFO
                msg += " (AQE may handle partition coalescing automatically)"
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

        if job.duration_ms < self._thresholds.idle_min_job_duration_ms:
            return []

        utilization = job.executors.slot_utilization_percent(job.duration_ms, job.config.executor_cores)
        if utilization is None or utilization > self._thresholds.min_slot_utilization_percent:
            return []

        severity = (
            Severity.CRITICAL
            if utilization < self._thresholds.min_slot_utilization_critical_percent
            else Severity.WARNING
        )

        if job.config.dynamic_allocation_enabled:
            severity = Severity.WARNING if severity == Severity.CRITICAL else Severity.INFO

        idle_pct = 100 - utilization
        cores = job.config.executor_cores

        return [
            self._result(
                severity,
                "Executor over-provisioning",
                f"Slot utilization is only {utilization:.0f}% — {idle_pct:.0f}% idle",
                current_value=f"{job.executors.executor_count} executors x {cores} cores, {utilization:.0f}% utilized",
                recommended_value="Reduce executor count or increase parallelism",
                estimated_impact=(
                    "Right-sizing can reduce costs 30-50%"
                    " (note: sequential stages reduce measured utilization)"
                ),
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
        if stage.total_shuffle_read_bytes > 0 and stage.input_bytes == 0:
            return None

        # Prefer per-task median (HS path), fall back to average (event log path)
        input_per_task = stage.tasks.distributions.input_metrics.bytes.median
        if input_per_task == 0 and stage.input_bytes > 0 and stage.tasks.task_count > 0:
            input_per_task = stage.input_bytes / stage.tasks.task_count
        if input_per_task == 0:
            return None
        if input_per_task >= self._thresholds.small_file_threshold_bytes:
            return None
        severity = (
            Severity.CRITICAL if input_per_task < self._thresholds.small_file_critical_bytes else Severity.WARNING
        )
        input_per_task_mb = input_per_task / (1024 * 1024)
        return self._result(
            severity,
            f"Small input partitions in Stage {stage.stage_id}",
            f"Input per task is {input_per_task_mb:.1f} MB across {stage.tasks.task_count} tasks",
            stage_id=stage.stage_id,
            current_value=f"{input_per_task_mb:.1f} MB/task, {stage.tasks.task_count} tasks",
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
            severity = (
                (Severity.INFO if job.config.aqe_enabled else Severity.WARNING) if has_shuffle else Severity.INFO
            )
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


# Validates dynamic allocation configuration:
# 1) DA enabled without min/maxExecutors bounds — unbounded scaling can over-provision.
# 2) DA disabled with low utilization — suggests enabling for auto-scaling.
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
                utilization = job.executors.slot_utilization_percent(
                    job.duration_ms, job.config.executor_cores
                )
                if utilization is not None and utilization < self._thresholds.min_slot_utilization_percent:
                    results.append(
                        self._result(
                            Severity.INFO,
                            "Consider enabling dynamic allocation",
                            f"Slot utilization is {utilization:.0f}% with fixed executor count",
                            current_value=(
                                f"{job.executors.executor_count} static executors,"
                                f" {utilization:.0f}% utilized"
                            ),
                            recommended_value="spark.dynamicAllocation.enabled = true",
                            estimated_impact="Dynamic allocation auto-scales executors to match workload",
                        )
                    )

        return results


# Detects memory pressure: high GC (heap) combined with high memory utilization.
# Recommends increasing spark.executor.memory (heap) or spark.executor.memoryOverhead
# (off-heap, for container OOM kills on YARN/K8s).
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

        gc_pct = max(
            s.gc_time_percent for s in job.stages
            if s.gc_time_percent > self._thresholds.memory_overhead_gc_threshold_percent
        )

        overhead = job.config.executor_memory_overhead
        current_desc = f"memoryOverhead = {overhead}" if overhead else "memoryOverhead = default (max(384MB, 10%))"

        return [
            self._result(
                Severity.WARNING,
                "Executor memory pressure detected",
                f"High GC ({gc_pct:.0f}% in hottest stage) with {mem_util:.0f}% memory utilization",
                current_value=current_desc,
                recommended_value=(
                    "Increase spark.executor.memory to reduce heap GC pressure,"
                    " or spark.executor.memoryOverhead if seeing container OOM kills"
                ),
                estimated_impact="Reducing memory pressure prevents GC thrashing and container OOM kills",
            )
        ]


# Checks spark.driver.memory against reasonable bounds. Too low (< 512 MB) risks OOM on
# collect/broadcast; too high (> 16 GB) wastes resources. Also detects when a large cluster
# (> 50 executors) runs with a small driver — coordination metadata grows with cluster size.
class DriverMemoryRule(Rule):
    rule_id: ClassVar[str] = "driver_memory"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        memory_mb = parse_memory_string(job.config.driver_memory)
        if memory_mb == 0:
            return []

        if memory_mb < self._thresholds.driver_memory_min_mb:
            return [
                self._result(
                    Severity.WARNING,
                    "Driver memory too low",
                    f"Driver memory is too low: {job.config.driver_memory} ({memory_mb} MB) — risk of OOM",
                    current_value=f"spark.driver.memory = {job.config.driver_memory}",
                    recommended_value=f"spark.driver.memory >= {self._thresholds.driver_memory_min_mb}m",
                    estimated_impact="Insufficient driver memory causes OOM on collect/broadcast operations",
                )
            ]

        if (
            job.executors is not None
            and job.executors.executor_count > self._thresholds.driver_large_cluster_executor_count
            and memory_mb < self._thresholds.driver_large_cluster_min_memory_mb
        ):
            return [
                self._result(
                    Severity.WARNING,
                    "Driver memory insufficient for large cluster",
                    (
                        f"Driver memory {job.config.driver_memory} may be insufficient"
                        f" for cluster with {job.executors.executor_count} executors"
                    ),
                    current_value=f"spark.driver.memory = {job.config.driver_memory}",
                    recommended_value=f"spark.driver.memory >= {self._thresholds.driver_large_cluster_min_memory_mb}m",
                    estimated_impact="Large clusters need more driver memory for coordination and metadata",
                )
            ]

        if memory_mb > self._thresholds.driver_memory_max_mb:
            return [
                self._result(
                    Severity.INFO,
                    "Driver memory too high",
                    f"Driver memory is too high: {job.config.driver_memory} ({memory_mb} MB) — resource waste",
                    current_value=f"spark.driver.memory = {job.config.driver_memory}",
                    recommended_value=f"spark.driver.memory <= {self._thresholds.driver_memory_max_mb}m",
                    estimated_impact="Over-provisioned driver wastes cluster resources",
                )
            ]

        return []


# Detects over-provisioned executor memory: peak usage below 40% of allocated means most
# heap is wasted. Suppressed when GC pressure or spill exists — those signal the opposite
# problem (not enough memory), and conflicting recommendations confuse users.
class MemoryUnderutilizationRule(Rule):
    rule_id: ClassVar[str] = "memory_underutilization"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        if job.executors is None:
            return []

        has_gc_pressure = any(
            s.gc_time_percent > self._thresholds.gc_warning_percent for s in job.stages
            if s.sum_executor_run_time_ms >= self._thresholds.gc_min_stage_runtime_ms
        )
        has_spill = any(s.spill_to_disk_bytes > 0 for s in job.stages)
        if has_gc_pressure or has_spill:
            return []

        utilization = job.executors.memory_utilization_percent
        if utilization == 0.0 or utilization >= self._thresholds.memory_underutilization_percent:
            return []

        return [
            self._result(
                Severity.WARNING,
                "Executor memory underutilized",
                f"Peak memory usage is only {utilization:.0f}% of allocated — executors overprovisioned",
                current_value=f"{utilization:.0f}% memory utilization",
                recommended_value="Reduce spark.executor.memory to match actual usage + 20% buffer",
                estimated_impact="Right-sizing executor memory frees cluster resources",
            )
        ]


# Recommends enabling Adaptive Query Execution for Spark 3.x jobs. AQE auto-handles
# partition coalescing, skew joins, and broadcast threshold at runtime. Version-aware:
# INFO for 3.0-3.1 (opt-in), WARNING for 3.2+ where someone explicitly disabled the default.
class AQENotEnabledRule(Rule):
    rule_id: ClassVar[str] = "aqe_not_enabled"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        version = parse_spark_version(job.spark_version)
        if version is None or version < (3, 0):
            return []

        if job.config.aqe_enabled:
            return []

        if version >= (3, 2) and not job.config.has_explicit_aqe_config:
            return []

        if version >= (3, 2):
            return [
                self._result(
                    Severity.WARNING,
                    "AQE explicitly disabled",
                    "Adaptive Query Execution is disabled (enabled by default since Spark 3.2)",
                    current_value="spark.sql.adaptive.enabled = false",
                    recommended_value="spark.sql.adaptive.enabled = true",
                    estimated_impact="AQE auto-handles partition coalescing, skew joins, and broadcast threshold",
                )
            ]

        return [
            self._result(
                Severity.INFO,
                "AQE not enabled",
                "Consider enabling Adaptive Query Execution for automatic optimization",
                current_value="spark.sql.adaptive.enabled = false",
                recommended_value="spark.sql.adaptive.enabled = true",
                estimated_impact="AQE auto-handles partition coalescing, skew joins, and broadcast threshold",
            )
        ]


# Detects DataFrame recomputation by finding duplicate stage names — when the same stage
# appears multiple times, it usually means Spark re-executes the same lineage because
# intermediate results weren't persisted. Threshold: >= 5 distinct duplicate names.
class ExcessiveStagesRule(Rule):
    rule_id: ClassVar[str] = "excessive_stages"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        names = [s.stage_name for s in job.stages]
        counts = Counter(names)
        duplicates = sorted(name for name, count in counts.items() if count > 1)

        if len(duplicates) < self._thresholds.min_duplicate_stages_for_warning:
            return []

        return [
            self._result(
                Severity.WARNING,
                "Stages re-executed",
                f"Stages re-executed: {duplicates} — add .persist() to avoid recomputation",
                current_value=f"{len(duplicates)} duplicate stage names",
                recommended_value="Use .persist() or .cache() on reused DataFrames",
                estimated_impact="Shorter lineage reduces recomputation risk and planning overhead",
            )
        ]


# Flags stages with disproportionately large shuffle writes. Two checks:
# 1) Ratio-based: shuffle write > 3x input bytes means data explosion (e.g. cross join).
# 2) Absolute: shuffle write > 50 GB regardless of input (safety net for huge shuffles).
# Ratio check is preferred when both trigger, as it gives more actionable context.
class ShuffleDataVolumeRule(Rule):
    rule_id: ClassVar[str] = "shuffle_data_volume"

    def _check_stage(self, stage: StageMetrics, job: JobAnalysis) -> RuleResult | None:
        shuffle_write_gb = stage.total_shuffle_write_bytes / (1024**3)

        ratio_result: RuleResult | None = None
        if stage.input_bytes > 0:
            ratio = stage.total_shuffle_write_bytes / stage.input_bytes
            if ratio > self._thresholds.shuffle_ratio_warning:
                ratio_result = self._result(
                    Severity.WARNING,
                    f"High shuffle volume in Stage {stage.stage_id}",
                    (
                        f"Stage writes {shuffle_write_gb:.1f} GB shuffle data"
                        f" ({ratio:.1f}x of input) — consider pre-partitioning or bucketing"
                    ),
                    stage_id=stage.stage_id,
                    current_value=f"{shuffle_write_gb:.1f} GB shuffle write, {ratio:.1f}x input ratio",
                    recommended_value="Use bucketing, pre-partition data, or reduce shuffle with broadcast joins",
                    estimated_impact="Reduce disk I/O and network transfer",
                )

        absolute_result: RuleResult | None = None
        if shuffle_write_gb > self._thresholds.shuffle_volume_absolute_gb:
            absolute_result = self._result(
                Severity.WARNING,
                f"High shuffle volume in Stage {stage.stage_id}",
                f"Stage writes {shuffle_write_gb:.1f} GB shuffle data — consider pre-partitioning or bucketing",
                stage_id=stage.stage_id,
                current_value=f"{shuffle_write_gb:.1f} GB shuffle write",
                recommended_value="Use bucketing, pre-partition data, or reduce shuffle with broadcast joins",
                estimated_impact="Reduce disk I/O and network transfer",
            )

        if ratio_result is not None:
            return ratio_result
        return absolute_result


# Detects uneven input data distribution across tasks — when one task reads significantly
# more than the median (> 5x), the stage is bottlenecked by that task. Complements DataSkewRule
# which looks at duration; this rule catches skew at the source (file sizes, partition splits).
# Requires per-task distribution data from History Server (not available from event log parser).
class InputDataSkewRule(Rule):
    rule_id: ClassVar[str] = "input_data_skew"

    def _check_stage(self, stage: StageMetrics, job: JobAnalysis) -> RuleResult | None:
        if stage.tasks.task_count < self._thresholds.min_task_count_for_skew:
            return None

        if stage.total_shuffle_read_bytes > 0 and stage.input_bytes == 0:
            return None

        input_bytes = stage.tasks.distributions.input_metrics.bytes
        median_input = input_bytes.median
        max_input = input_bytes.max

        # Skew detection requires per-task distribution data (available from HS, not event log parser)
        if median_input == 0:
            return None

        ratio = max_input / median_input
        if ratio < self._thresholds.input_skew_warning_ratio:
            return None

        severity = (
            Severity.CRITICAL
            if ratio >= self._thresholds.input_skew_critical_ratio
            else Severity.WARNING
        )

        return self._result(
            severity,
            f"Input data skew in Stage {stage.stage_id}",
            f"Max task input ({format_bytes(max_input)}) is {ratio:.1f}x the median ({format_bytes(median_input)})",
            stage_id=stage.stage_id,
            current_value=f"input skew ratio {ratio:.1f}x",
            recommended_value="Repartition input data or use salting to distribute evenly",
            estimated_impact=f"Stage {stage.stage_id} tasks could run ~{int((1 - 1 / ratio) * 100)}% faster",
        )


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
        DriverMemoryRule(thresholds),
        MemoryUnderutilizationRule(thresholds),
        AQENotEnabledRule(thresholds),
        ExcessiveStagesRule(thresholds),
        ShuffleDataVolumeRule(thresholds),
        InputDataSkewRule(thresholds),
    ]
