# Phase 15: New Analysis Rules — Design Spec

**Date**: 2026-03-21
**Scope**: 6 new rules (12-17), deferred TaskLocalityRule to future phase
**Approach**: Layered — models changes first, then rules + tests

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| TaskLocalityRule | Deferred | Requires parser extension (locality data not extracted from event logs) |
| InputDataSkewRule thresholds | Separate from DataSkewRule | Duration skew and input bytes skew are different phenomena, need independent tuning |
| DriverMemoryRule — missing config | Use Spark defaults | `spark.driver.memory` defaults to `1g`, `memoryOverhead` to `max(384MB, 10% driver memory)` |
| AQENotEnabledRule | Version-aware (2 tiers) | 3.0-3.1 → INFO (opt-in), 3.2+ → WARNING (explicitly disabled default) |
| MemoryUnderutilizationRule — no executors | Graceful skip | `ExecutorMetrics` is `None` for event log source — return empty list |
| ShuffleDataVolumeRule severity | WARNING only | Absolute shuffle volume depends on job size — no universal CRITICAL threshold |
| ExcessiveStagesRule severity | WARNING only | High stage count is a hint, not always a problem |

## Data Availability (verified against live History Server)

| Rule | Data Source | Field | Confirmed |
|------|-----------|-------|-----------|
| DriverMemoryRule | sparkProperties | `spark.driver.memory`, `spark.driver.memoryOverhead` | Yes |
| MemoryUnderutilizationRule | allexecutors | `peakMemoryMetrics`, `maxMemory` | Yes |
| AQENotEnabledRule | sparkProperties + app | `spark.sql.adaptive.enabled`, `appSparkVersion` | Yes |
| ExcessiveStagesRule | stages | `len(stages)` | Yes |
| ShuffleDataVolumeRule | stages | `shuffleWriteBytes` per stage | Yes |
| InputDataSkewRule | taskSummary | `inputMetrics.bytesRead` quantiles (min/p25/median/p75/max) | Yes |

---

## Layer 1: Models Changes (`spark-advisor-models`)

### 1a. `SparkConfig` — new properties

Add to `SparkConfig` as `@property` methods (same pattern as existing `executor_memory`, `executor_memory_overhead` which use `self.get()`):

```python
@property
def driver_memory(self) -> str:
    return self.get("spark.driver.memory", "1g")

@property
def driver_memory_overhead(self) -> str:
    return self.get("spark.driver.memoryOverhead", "")
```

**Note**: These are properties, NOT Pydantic fields — consistent with existing SparkConfig accessors.

### 1b. `Thresholds` — new fields

```python
# DriverMemoryRule
driver_memory_min_mb: int = 512
driver_memory_max_mb: int = 8192

# MemoryUnderutilizationRule
memory_underutilization_percent: float = 40.0

# AQENotEnabledRule — no thresholds, logic based on spark_version

# ExcessiveStagesRule
excessive_stages_count: int = 50

# ShuffleDataVolumeRule
shuffle_volume_warning_gb: float = 10.0

# InputDataSkewRule (separate from DataSkewRule)
input_skew_warning_ratio: float = 5.0
input_skew_critical_ratio: float = 10.0
```

### 1c. Spark version parser utility

```python
# In models/util/spark.py (new file, alongside existing util/bytes.py)
def parse_spark_version(version: str) -> tuple[int, int] | None
```

- `"3.2.1"` → `(3, 2)`
- `"3.5.0-amzn-0"` → `(3, 5)`
- Unparseable → `None` (caller handles gracefully)

### 1d. Memory size parser utility

```python
# In models/util/bytes.py (alongside existing format_bytes)
def parse_memory_string(value: str) -> int  # returns MB
```

- `"1g"` → `1024`, `"512m"` → `512`, `"2048"` → `2048`
- New function — no existing implementation found. Placed in `util/bytes.py` next to `format_bytes()`.
- Follows Spark conventions (`k`, `m`, `g`, `t` suffixes, case-insensitive).

---

## Layer 2: Rules (`spark-advisor-rules`)

### Rule 12: `DriverMemoryRule` (job-level)

- **rule_id**: `driver_memory`
- **Scope**: job-level (overrides `evaluate()`)
- **Logic**:
  - Parse `job.config.driver_memory` → MB via `parse_memory_string()`
  - If < `driver_memory_min_mb` (512) → WARNING: "Driver memory too low ({value}), risk of OOM"
  - If > `driver_memory_max_mb` (8192) → WARNING: "Driver memory too high ({value}), resource waste"
  - `recommended_value`: range `512m–8192m`
- **Edge cases**: Missing config → Spark default `1g` = 1024 MB → in range → no finding

### Rule 13: `MemoryUnderutilizationRule` (job-level)

- **rule_id**: `memory_underutilization`
- **Scope**: job-level (overrides `evaluate()`)
- **Logic**:
  - If `job.executors is None` → `return []`
  - `utilization = job.executors.memory_utilization_percent`
  - If < `memory_underutilization_percent` (40%) → WARNING
  - Message: "Peak memory usage only {X}% of allocated — executors overprovisioned"
  - `recommended_value`: suggested `spark.executor.memory` based on peak + 20% buffer
- **Edge cases**: `allocated_memory_bytes_sum == 0` → skip (property returns 0.0)

### Rule 14: `AQENotEnabledRule` (job-level)

- **rule_id**: `aqe_not_enabled`
- **Scope**: job-level (overrides `evaluate()`)
- **Logic**:
  - Parse `job.spark_version` → `(major, minor)` via `parse_spark_version()`
  - If `None` or `< (3, 0)` → `return []`
  - If `aqe_enabled == True` → `return []`
  - If `(3, 0) <= version < (3, 2)` → INFO: "Consider enabling AQE for automatic partition coalescing and skew handling"
  - If `version >= (3, 2)` → WARNING: "AQE explicitly disabled (enabled by default since Spark 3.2)"
- **Edge cases**:
  - Unparseable version → `return []` (safe skip)
  - Event log source where `spark.sql.adaptive.enabled` is absent: `SparkConfig.aqe_enabled` defaults to `False`. For Spark 3.2+ this could be a false positive (AQE is on by default). Mitigation: check if config key exists in `raw` dict — if absent AND version >= 3.2, assume AQE is enabled (default-on), skip the rule

### Rule 15: `ExcessiveStagesRule` (job-level)

- **rule_id**: `excessive_stages`
- **Scope**: job-level (overrides `evaluate()`)
- **Logic**:
  - If `len(job.stages) > excessive_stages_count` (50) → WARNING
  - Message: "Job has {N} stages — consider persisting intermediate results to break lineage"
  - `current_value`: stage count
  - `recommended_value`: "Use .persist() or .cache() on reused DataFrames"

### Rule 16: `ShuffleDataVolumeRule` (per-stage)

- **rule_id**: `shuffle_data_volume`
- **Scope**: per-stage (overrides `_check_stage()`)
- **Logic**:
  - `shuffle_write_gb = stage.total_shuffle_write_bytes / (1024**3)`
  - If > `shuffle_volume_warning_gb` (10.0) → WARNING
  - Message: "Stage {id} writes {X:.1f} GB shuffle data — consider pre-partitioning or bucketing"
  - `current_value`: shuffle write in GB
  - `estimated_impact`: "Reduce disk I/O and network transfer"

### Rule 17: `InputDataSkewRule` (per-stage)

- **rule_id**: `input_data_skew`
- **Scope**: per-stage (overrides `_check_stage()`)
- **Logic**:
  - `max_input = stage.tasks.distributions.input_metrics.bytes.max`
  - `median_input = stage.tasks.distributions.input_metrics.bytes.median`
  - If `median_input == 0` → skip (no input data)
  - `ratio = max_input / median_input`
  - If > `input_skew_critical_ratio` (10.0) → CRITICAL: "Severe input data skew"
  - If > `input_skew_warning_ratio` (5.0) → WARNING: "Input data skew detected"
  - Message includes: max task input, median task input, ratio
- **Difference from DataSkewRule**: Analyzes **input bytes distribution**, not **task duration**. Independent triggers.

---

## Layer 3: Registration

Add all 6 rules to `rules_for_threshold()` in `static_analysis.py`:

```python
def rules_for_threshold(thresholds: Thresholds) -> list[Rule]:
    return [
        # ... existing 11 rules ...
        DriverMemoryRule(thresholds),
        MemoryUnderutilizationRule(thresholds),
        AQENotEnabledRule(thresholds),
        ExcessiveStagesRule(thresholds),
        ShuffleDataVolumeRule(thresholds),
        InputDataSkewRule(thresholds),
    ]
```

---

## Layer 4: Tests

Minimum 3 tests per rule following existing patterns:

| Rule | Test: No finding | Test: WARNING | Test: CRITICAL | Test: Edge case |
|------|-----------------|---------------|----------------|-----------------|
| DriverMemoryRule | memory in range (1g) | memory too low (256m) | — | memory too high (16g) |
| MemoryUnderutilizationRule | utilization 60% | utilization 30% | — | executors=None → skip |
| AQENotEnabledRule | AQE enabled | AQE off, Spark 3.0 → INFO | AQE off, Spark 3.4 → WARNING | Spark 2.4 → skip |
| ExcessiveStagesRule | 10 stages | 60 stages | — | 0 stages → skip |
| ShuffleDataVolumeRule | 1GB shuffle | 15GB shuffle | — | 0 bytes → skip |
| InputDataSkewRule | ratio 2x | ratio 7x → WARNING | ratio 15x → CRITICAL | median=0 → skip |

Test tooling: `make_stage()`, `make_job()`, `make_executors()` factories from `spark_advisor_models.testing`.

**Factory extension needed**: `make_stage()` currently lacks parameters for `input_metrics` quantiles. Add kwargs `input_bytes_median: int = 0`, `input_bytes_max: int = 0` to support InputDataSkewRule tests.

---

## Files Modified

| File | Change |
|------|--------|
| `packages/spark-advisor-models/src/spark_advisor_models/model/spark_config.py` | Add `driver_memory`, `driver_memory_overhead` as `@property` methods |
| `packages/spark-advisor-models/src/spark_advisor_models/config.py` | Add 7 new threshold fields to `Thresholds` |
| `packages/spark-advisor-models/src/spark_advisor_models/util/spark.py` | New file: `parse_spark_version()` |
| `packages/spark-advisor-models/src/spark_advisor_models/util/bytes.py` | Add `parse_memory_string()` alongside existing `format_bytes()` |
| `packages/spark-advisor-models/src/spark_advisor_models/testing/factories.py` | Extend `make_stage()` with `input_bytes_median`, `input_bytes_max` kwargs |
| `packages/spark-advisor-rules/src/spark_advisor_rules/rules.py` | Add 6 new rule classes |
| `packages/spark-advisor-rules/src/spark_advisor_rules/static_analysis.py` | Register 6 new rules in `rules_for_threshold()` |
| `packages/spark-advisor-rules/tests/test_rules.py` | Add ~24 new tests (min 3-4 per rule) |

## Files NOT Modified

- Parser — no changes needed (all data available via existing parsing)
- CLI output — rules auto-appear via StaticAnalysisService
- MCP — rules auto-appear via StaticAnalysisService
- Gateway/Analyzer — rules auto-appear via StaticAnalysisService

## Verification

```bash
make check  # ruff + mypy strict + pytest all packages
```

Expected: ~639 tests (615 current + ~24 new), 0 lint errors, mypy strict clean.
