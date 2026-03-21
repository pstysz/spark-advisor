# Rules Improvements — Design Spec

**Date**: 2026-03-21
**Scope**: 8 rule improvements (3 new rules + 5 existing rules)

## Changes

### 1. DriverMemoryRule — executor count correlation

- If `executor_count > 50` AND `driver_memory < 2g` → WARNING (large cluster, small driver)
- "Too high" finding: severity change WARNING → INFO
- Requires `job.executors` check (may be None)
- New threshold: `driver_large_cluster_executor_count: int = 50`, `driver_large_cluster_min_memory_mb: int = 2048`

### 2. ShuffleDataVolumeRule — ratio-based

Replace absolute-only with dual logic:
- **Ratio check** (when `input_bytes > 0`): `shuffle_write / input_bytes > 1.0` → WARNING
- **Absolute check** (always): `shuffle_write > 50GB` → WARNING (safety net, raised from 10GB)
- Stage with `input_bytes == 0` (reads from shuffle): only absolute check applies
- New thresholds: `shuffle_ratio_warning: float = 1.0`, rename `shuffle_volume_warning_gb` → `shuffle_volume_absolute_gb: float = 50.0`

### 3. ExcessiveStagesRule — duplicate stage detection

Replace count-based with recomputation detection:
- Find duplicate stage names: `names that appear > 1 time`
- If duplicates found → WARNING: "Stages X, Y execute multiple times — add .persist() to avoid recomputation"
- Remove `excessive_stages_count` threshold (no longer needed)
- New threshold: `min_duplicate_stages_for_warning: int = 2`

### 4. GCPressureRule — min execution time filter

- Skip stages where `sum_executor_run_time_ms < 60_000` (60s) — short stages with high GC% are noise
- New threshold: `gc_min_stage_runtime_ms: int = 60_000`

### 5. ShufflePartitionsRule — AQE-aware severity

- If `job.config.aqe_enabled` AND rule would trigger WARNING → downgrade to INFO
- Add note to message: "AQE may handle partition coalescing automatically"

### 6. SmallFileRule — median instead of average

- Replace `input_bytes / task_count` with `stage.tasks.distributions.input_metrics.bytes.median`
- If `median == 0` → skip (same as current `input_bytes == 0` guard)
- Thresholds unchanged, applied to median instead of average

### 7. ExecutorIdleRule — min duration filter + DA-aware

- Skip analysis when `job.duration_ms < 300_000` (5 min) — short jobs don't have meaningful utilization
- If `job.config.dynamic_allocation_enabled` → downgrade severity by one level (CRITICAL→WARNING, WARNING→INFO)
- New threshold: `idle_min_job_duration_ms: int = 300_000`

### 8. ExecutorMemoryOverheadRule — clearer message

- Change title from "Executor memory overhead may be too low" → "Increase executor memory overhead"
- Change message to explicitly mention off-heap: "High GC ({gc}%) with {mem}% memory utilization suggests off-heap memory pressure — increase spark.executor.memoryOverhead"

## Files Modified

| File | Change |
|------|--------|
| `models/config.py` | New thresholds, rename shuffle threshold |
| `rules/rules.py` | All 8 rule changes |
| `rules/tests/test_rules.py` | New/updated tests |

## Verification

`make check` — all tests pass, 0 lint, mypy strict clean.
