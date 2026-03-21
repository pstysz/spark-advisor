# Phase 15: New Analysis Rules — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 6 new Spark analysis rules (DriverMemory, MemoryUnderutilization, AQENotEnabled, ExcessiveStages, ShuffleDataVolume, InputDataSkew) to the rules engine.

**Architecture:** Two-layer approach — first extend models (SparkConfig properties, Thresholds fields, utility functions, factory extensions), then add rule classes + tests + registration. All rules follow existing Rule ABC pattern.

**Tech Stack:** Python 3.12+, Pydantic v2 (frozen models), pytest, mypy strict

**Spec:** `docs/superpowers/specs/2026-03-21-phase15-new-rules-design.md`

---

### Task 1: Add `parse_memory_string()` to `util/bytes.py`

**Files:**
- Modify: `packages/spark-advisor-models/src/spark_advisor_models/util/bytes.py`
- Modify: `packages/spark-advisor-models/src/spark_advisor_models/util/__init__.py`
- Test: `packages/spark-advisor-models/tests/test_util_bytes.py`

- [ ] **Step 1: Write failing tests**

```python
# packages/spark-advisor-models/tests/test_util_bytes.py
import pytest

from spark_advisor_models.util.bytes import parse_memory_string


class TestParseMemoryString:
    def test_gigabytes_suffix(self) -> None:
        assert parse_memory_string("1g") == 1024

    def test_megabytes_suffix(self) -> None:
        assert parse_memory_string("512m") == 512

    def test_kilobytes_suffix(self) -> None:
        assert parse_memory_string("1024k") == 1

    def test_sub_kb_rounds_up_to_1(self) -> None:
        assert parse_memory_string("512k") == 1

    def test_terabytes_suffix(self) -> None:
        assert parse_memory_string("1t") == 1024 * 1024

    def test_uppercase_suffix(self) -> None:
        assert parse_memory_string("2G") == 2048

    def test_bare_number_treated_as_mb(self) -> None:
        assert parse_memory_string("2048") == 2048

    def test_empty_string_returns_zero(self) -> None:
        assert parse_memory_string("") == 0

    def test_invalid_string_returns_zero(self) -> None:
        assert parse_memory_string("abc") == 0
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd packages/spark-advisor-models && uv run pytest tests/test_util_bytes.py -v`
Expected: FAIL — `ImportError: cannot import name 'parse_memory_string'`

- [ ] **Step 3: Implement `parse_memory_string()`**

Add to `packages/spark-advisor-models/src/spark_advisor_models/util/bytes.py`:

```python
import re

_MEMORY_PATTERN = re.compile(r"^(\d+(?:\.\d+)?)\s*([kmgt])?b?$", re.IGNORECASE)
_SUFFIX_TO_MB: dict[str, int] = {"m": 1, "g": 1024, "t": 1024 * 1024}


def parse_memory_string(value: str) -> int:
    """Parse Spark memory string (e.g. '1g', '512m', '2048') to megabytes."""
    if not value or not value.strip():
        return 0
    match = _MEMORY_PATTERN.match(value.strip())
    if not match:
        return 0
    number = float(match.group(1))
    suffix = (match.group(2) or "m").lower()
    if suffix == "k":
        return max(1, int(number / 1024))
    return int(number * _SUFFIX_TO_MB.get(suffix, 1))
```

- [ ] **Step 4: Export from `__init__.py`**

Add `parse_memory_string` to `packages/spark-advisor-models/src/spark_advisor_models/util/__init__.py`:

```python
from spark_advisor_models.util.bytes import format_bytes, parse_memory_string
from spark_advisor_models.util.stats import median_value, percentile_value, quantiles_5

__all__ = ["format_bytes", "parse_memory_string", "median_value", "percentile_value", "quantiles_5"]
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd packages/spark-advisor-models && uv run pytest tests/test_util_bytes.py -v`
Expected: 8 passed

- [ ] **Step 6: Run full models tests + mypy**

Run: `cd packages/spark-advisor-models && uv run pytest -v && uv run mypy src/`
Expected: 47+ tests passed, mypy clean

---

### Task 2: Add `parse_spark_version()` to `util/spark.py`

**Files:**
- Create: `packages/spark-advisor-models/src/spark_advisor_models/util/spark.py`
- Modify: `packages/spark-advisor-models/src/spark_advisor_models/util/__init__.py`
- Test: `packages/spark-advisor-models/tests/test_util_spark.py`

- [ ] **Step 1: Write failing tests**

```python
# packages/spark-advisor-models/tests/test_util_spark.py
from spark_advisor_models.util.spark import parse_spark_version


class TestParseSparkVersion:
    def test_standard_version(self) -> None:
        assert parse_spark_version("3.2.1") == (3, 2)

    def test_major_minor_only(self) -> None:
        assert parse_spark_version("3.4") == (3, 4)

    def test_vendor_suffix(self) -> None:
        assert parse_spark_version("3.5.0-amzn-0") == (3, 5)

    def test_spark2(self) -> None:
        assert parse_spark_version("2.4.8") == (2, 4)

    def test_empty_string(self) -> None:
        assert parse_spark_version("") is None

    def test_unparseable(self) -> None:
        assert parse_spark_version("custom-build") is None

    def test_single_number(self) -> None:
        assert parse_spark_version("3") is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd packages/spark-advisor-models && uv run pytest tests/test_util_spark.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement `parse_spark_version()`**

Create `packages/spark-advisor-models/src/spark_advisor_models/util/spark.py`:

```python
import re

_VERSION_PATTERN = re.compile(r"^(\d+)\.(\d+)")


def parse_spark_version(version: str) -> tuple[int, int] | None:
    """Parse Spark version string to (major, minor) tuple. Returns None if unparseable."""
    if not version:
        return None
    match = _VERSION_PATTERN.match(version.strip())
    if not match:
        return None
    return int(match.group(1)), int(match.group(2))
```

- [ ] **Step 4: Export from `__init__.py`**

Update `packages/spark-advisor-models/src/spark_advisor_models/util/__init__.py`:

```python
from spark_advisor_models.util.bytes import format_bytes, parse_memory_string
from spark_advisor_models.util.spark import parse_spark_version
from spark_advisor_models.util.stats import median_value, percentile_value, quantiles_5

__all__ = [
    "format_bytes",
    "parse_memory_string",
    "parse_spark_version",
    "median_value",
    "percentile_value",
    "quantiles_5",
]
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd packages/spark-advisor-models && uv run pytest tests/test_util_spark.py -v`
Expected: 7 passed

- [ ] **Step 6: Run full models tests + mypy**

Run: `cd packages/spark-advisor-models && uv run pytest -v && uv run mypy src/`
Expected: All passed, mypy clean

---

### Task 3: Add `SparkConfig` properties + `Thresholds` fields

**Files:**
- Modify: `packages/spark-advisor-models/src/spark_advisor_models/model/spark_config.py:48-51` (add after `executor_memory_overhead`)
- Modify: `packages/spark-advisor-models/src/spark_advisor_models/config.py:35` (add after `memory_utilization_critical_percent`)
- Test: `packages/spark-advisor-models/tests/test_spark_config.py` (or existing test file)

- [ ] **Step 1: Write failing tests for SparkConfig properties**

```python
# Add to existing SparkConfig tests or create new file
from spark_advisor_models.model.spark_config import SparkConfig


class TestSparkConfigDriverProperties:
    def test_driver_memory_default(self) -> None:
        config = SparkConfig(raw={})
        assert config.driver_memory == "1g"

    def test_driver_memory_explicit(self) -> None:
        config = SparkConfig(raw={"spark.driver.memory": "4g"})
        assert config.driver_memory == "4g"

    def test_driver_memory_overhead_default(self) -> None:
        config = SparkConfig(raw={})
        assert config.driver_memory_overhead == ""

    def test_driver_memory_overhead_explicit(self) -> None:
        config = SparkConfig(raw={"spark.driver.memoryOverhead": "512m"})
        assert config.driver_memory_overhead == "512m"

    def test_has_aqe_config_key_present(self) -> None:
        config = SparkConfig(raw={"spark.sql.adaptive.enabled": "true"})
        assert config.has_explicit_aqe_config is True

    def test_has_aqe_config_key_absent(self) -> None:
        config = SparkConfig(raw={})
        assert config.has_explicit_aqe_config is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd packages/spark-advisor-models && uv run pytest tests/test_spark_config.py -v`
Expected: FAIL — `AttributeError: 'SparkConfig' object has no attribute 'driver_memory'`

- [ ] **Step 3: Add properties to SparkConfig**

Add at the end of `packages/spark-advisor-models/src/spark_advisor_models/model/spark_config.py` (after line 50):

```python
    @property
    def driver_memory(self) -> str:
        return self.get("spark.driver.memory", "1g")

    @property
    def driver_memory_overhead(self) -> str:
        return self.get("spark.driver.memoryOverhead")

    @property
    def has_explicit_aqe_config(self) -> bool:
        return "spark.sql.adaptive.enabled" in self.raw
```

Note: `has_explicit_aqe_config` is needed by AQENotEnabledRule to distinguish "AQE not set" (use Spark version default) from "AQE explicitly disabled".

- [ ] **Step 4: Add Thresholds fields**

Add at the end of `Thresholds` class in `packages/spark-advisor-models/src/spark_advisor_models/config.py` (after line 35, before blank line):

```python
    driver_memory_min_mb: int = 512
    driver_memory_max_mb: int = 8192

    memory_underutilization_percent: float = 40.0

    excessive_stages_count: int = 50

    shuffle_volume_warning_gb: float = 10.0

    input_skew_warning_ratio: float = 5.0
    input_skew_critical_ratio: float = 10.0
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd packages/spark-advisor-models && uv run pytest -v && uv run mypy src/`
Expected: All passed, mypy clean

---

### Task 4: Extend `make_stage()` factory with `input_bytes_median` and `input_bytes_max`

**Files:**
- Modify: `packages/spark-advisor-models/src/spark_advisor_models/testing/factories.py:20-84`
- Test: Verify existing tests still pass

- [ ] **Step 1: Add parameters to `make_stage()`**

In `packages/spark-advisor-models/src/spark_advisor_models/testing/factories.py`:

First, add `IOQuantiles` to the import block (line 3-13):

```python
from spark_advisor_models.model import (
    ExecutorMetrics,
    IOQuantiles,
    JobAnalysis,
    Quantiles,
    RuleResult,
    Severity,
    SparkConfig,
    StageMetrics,
    TaskMetrics,
    TaskMetricsDistributions,
)
```

Then add new kwargs to `make_stage()` signature (after `scheduler_delay_max: int = 0` at line 43):

```python
    input_bytes_median: int = 0,
    input_bytes_max: int = 0,
```

Then in the function body, before the `return StageMetrics(...)` call, add:

```python
    input_io = (
        IOQuantiles(bytes=make_quantiles(median=input_bytes_median, max=input_bytes_max))
        if input_bytes_max
        else IOQuantiles()
    )
```

And update the `TaskMetricsDistributions` construction inside `return StageMetrics(...)` to include:

```python
                input_metrics=input_io,
```

- [ ] **Step 2: Run all models tests to verify no regressions**

Run: `cd packages/spark-advisor-models && uv run pytest -v`
Expected: All 47+ tests still pass (default `input_bytes_max=0` preserves existing behavior)

- [ ] **Step 3: Run all rules tests to verify no regressions**

Run: `cd packages/spark-advisor-rules && uv run pytest -v`
Expected: All 45 tests still pass

- [ ] **Step 4: Run mypy on both packages**

Run: `cd packages/spark-advisor-models && uv run mypy src/ && cd ../spark-advisor-rules && uv run mypy src/`
Expected: Clean

---

### Task 5: Implement `DriverMemoryRule`

**Files:**
- Modify: `packages/spark-advisor-rules/src/spark_advisor_rules/rules.py:420-435` (add before `rules_for_threshold`)
- Test: `packages/spark-advisor-rules/tests/test_rules.py`

- [ ] **Step 1: Write failing tests**

Add to `packages/spark-advisor-rules/tests/test_rules.py`:

```python
from spark_advisor_rules.rules import DriverMemoryRule


class TestDriverMemoryRule:
    def test_memory_in_range_no_finding(self) -> None:
        job = make_job(config={"spark.driver.memory": "1g"})
        assert DriverMemoryRule().evaluate(job) == []

    def test_memory_too_low_warning(self) -> None:
        job = make_job(config={"spark.driver.memory": "256m"})
        results = DriverMemoryRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING
        assert results[0].rule_id == "driver_memory"
        assert "too low" in results[0].message.lower()

    def test_memory_too_high_warning(self) -> None:
        job = make_job(config={"spark.driver.memory": "16g"})
        results = DriverMemoryRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING
        assert "too high" in results[0].message.lower()

    def test_default_memory_no_finding(self) -> None:
        job = make_job(config={})
        assert DriverMemoryRule().evaluate(job) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd packages/spark-advisor-rules && uv run pytest tests/test_rules.py::TestDriverMemoryRule -v`
Expected: FAIL — `ImportError`

- [ ] **Step 3: Implement DriverMemoryRule**

Add to `packages/spark-advisor-rules/src/spark_advisor_rules/rules.py` (before `rules_for_threshold` function):

```python
from spark_advisor_models.util import parse_memory_string


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
                    f"Driver memory is {job.config.driver_memory} ({memory_mb} MB) — risk of OOM",
                    current_value=f"spark.driver.memory = {job.config.driver_memory}",
                    recommended_value=f"spark.driver.memory >= {self._thresholds.driver_memory_min_mb}m",
                    estimated_impact="Insufficient driver memory causes OOM on collect/broadcast operations",
                )
            ]

        if memory_mb > self._thresholds.driver_memory_max_mb:
            return [
                self._result(
                    Severity.WARNING,
                    "Driver memory too high",
                    f"Driver memory is {job.config.driver_memory} ({memory_mb} MB) — resource waste",
                    current_value=f"spark.driver.memory = {job.config.driver_memory}",
                    recommended_value=f"spark.driver.memory <= {self._thresholds.driver_memory_max_mb}m",
                    estimated_impact="Over-provisioned driver wastes cluster resources",
                )
            ]

        return []
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd packages/spark-advisor-rules && uv run pytest tests/test_rules.py::TestDriverMemoryRule -v`
Expected: 4 passed

---

### Task 6: Implement `MemoryUnderutilizationRule`

**Files:**
- Modify: `packages/spark-advisor-rules/src/spark_advisor_rules/rules.py`
- Test: `packages/spark-advisor-rules/tests/test_rules.py`

- [ ] **Step 1: Write failing tests**

```python
from spark_advisor_rules.rules import MemoryUnderutilizationRule


class TestMemoryUnderutilizationRule:
    def test_good_utilization_no_finding(self) -> None:
        executors = make_executors(
            peak_memory_bytes_sum=3 * 1024**3,
            allocated_memory_bytes_sum=4 * 1024**3,
        )
        job = make_job(executors=executors)
        assert MemoryUnderutilizationRule().evaluate(job) == []

    def test_low_utilization_warning(self) -> None:
        executors = make_executors(
            peak_memory_bytes_sum=1 * 1024**3,
            allocated_memory_bytes_sum=4 * 1024**3,
        )
        job = make_job(executors=executors)
        results = MemoryUnderutilizationRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING
        assert results[0].rule_id == "memory_underutilization"

    def test_no_executors_skip(self) -> None:
        job = make_job(executors=None)
        assert MemoryUnderutilizationRule().evaluate(job) == []

    def test_zero_allocated_skip(self) -> None:
        executors = make_executors(
            peak_memory_bytes_sum=0,
            allocated_memory_bytes_sum=0,
        )
        job = make_job(executors=executors)
        assert MemoryUnderutilizationRule().evaluate(job) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd packages/spark-advisor-rules && uv run pytest tests/test_rules.py::TestMemoryUnderutilizationRule -v`
Expected: FAIL

- [ ] **Step 3: Implement MemoryUnderutilizationRule**

```python
class MemoryUnderutilizationRule(Rule):
    rule_id: ClassVar[str] = "memory_underutilization"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        if job.executors is None:
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd packages/spark-advisor-rules && uv run pytest tests/test_rules.py::TestMemoryUnderutilizationRule -v`
Expected: 4 passed

---

### Task 7: Implement `AQENotEnabledRule`

**Files:**
- Modify: `packages/spark-advisor-rules/src/spark_advisor_rules/rules.py`
- Test: `packages/spark-advisor-rules/tests/test_rules.py`

- [ ] **Step 1: Write failing tests**

```python
from spark_advisor_rules.rules import AQENotEnabledRule


class TestAQENotEnabledRule:
    def test_aqe_enabled_no_finding(self) -> None:
        job = make_job(
            spark_version="3.4.1",
            config={"spark.sql.adaptive.enabled": "true"},
        )
        assert AQENotEnabledRule().evaluate(job) == []

    def test_spark2_skip(self) -> None:
        job = make_job(
            spark_version="2.4.8",
            config={"spark.sql.adaptive.enabled": "false"},
        )
        assert AQENotEnabledRule().evaluate(job) == []

    def test_spark30_aqe_off_info(self) -> None:
        job = make_job(
            spark_version="3.0.3",
            config={"spark.sql.adaptive.enabled": "false"},
        )
        results = AQENotEnabledRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.INFO
        assert results[0].rule_id == "aqe_not_enabled"

    def test_spark34_aqe_off_warning(self) -> None:
        job = make_job(
            spark_version="3.4.1",
            config={"spark.sql.adaptive.enabled": "false"},
        )
        results = AQENotEnabledRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING

    def test_spark32_aqe_absent_skip(self) -> None:
        """Spark 3.2+ with no explicit AQE config = AQE is on by default, no finding."""
        job = make_job(spark_version="3.2.0", config={})
        assert AQENotEnabledRule().evaluate(job) == []

    def test_unparseable_version_skip(self) -> None:
        job = make_job(
            spark_version="custom-build",
            config={"spark.sql.adaptive.enabled": "false"},
        )
        assert AQENotEnabledRule().evaluate(job) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd packages/spark-advisor-rules && uv run pytest tests/test_rules.py::TestAQENotEnabledRule -v`
Expected: FAIL

- [ ] **Step 3: Implement AQENotEnabledRule**

```python
from spark_advisor_models.util import parse_spark_version


class AQENotEnabledRule(Rule):
    rule_id: ClassVar[str] = "aqe_not_enabled"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        version = parse_spark_version(job.spark_version)
        if version is None or version < (3, 0):
            return []

        if job.config.aqe_enabled:
            return []

        # Spark 3.2+ has AQE on by default — if config key absent, AQE is actually enabled
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd packages/spark-advisor-rules && uv run pytest tests/test_rules.py::TestAQENotEnabledRule -v`
Expected: 6 passed

---

### Task 8: Implement `ExcessiveStagesRule`

**Files:**
- Modify: `packages/spark-advisor-rules/src/spark_advisor_rules/rules.py`
- Test: `packages/spark-advisor-rules/tests/test_rules.py`

- [ ] **Step 1: Write failing tests**

```python
from spark_advisor_rules.rules import ExcessiveStagesRule


class TestExcessiveStagesRule:
    def test_few_stages_no_finding(self) -> None:
        stages = [make_stage(i) for i in range(10)]
        job = make_job(stages=stages)
        assert ExcessiveStagesRule().evaluate(job) == []

    def test_excessive_stages_warning(self) -> None:
        stages = [make_stage(i) for i in range(60)]
        job = make_job(stages=stages)
        results = ExcessiveStagesRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING
        assert results[0].rule_id == "excessive_stages"
        assert "60" in results[0].message

    def test_exactly_threshold_no_finding(self) -> None:
        stages = [make_stage(i) for i in range(50)]
        job = make_job(stages=stages)
        assert ExcessiveStagesRule().evaluate(job) == []

    def test_empty_stages_no_finding(self) -> None:
        job = make_job(stages=[])
        assert ExcessiveStagesRule().evaluate(job) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd packages/spark-advisor-rules && uv run pytest tests/test_rules.py::TestExcessiveStagesRule -v`
Expected: FAIL

- [ ] **Step 3: Implement ExcessiveStagesRule**

```python
class ExcessiveStagesRule(Rule):
    rule_id: ClassVar[str] = "excessive_stages"

    def evaluate(self, job: JobAnalysis) -> list[RuleResult]:
        stage_count = len(job.stages)
        if stage_count <= self._thresholds.excessive_stages_count:
            return []

        return [
            self._result(
                Severity.WARNING,
                "Excessive number of stages",
                f"Job has {stage_count} stages — consider persisting intermediate results to break lineage",
                current_value=f"{stage_count} stages",
                recommended_value="Use .persist() or .cache() on reused DataFrames",
                estimated_impact="Shorter lineage reduces recomputation risk and planning overhead",
            )
        ]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd packages/spark-advisor-rules && uv run pytest tests/test_rules.py::TestExcessiveStagesRule -v`
Expected: 4 passed

---

### Task 9: Implement `ShuffleDataVolumeRule`

**Files:**
- Modify: `packages/spark-advisor-rules/src/spark_advisor_rules/rules.py`
- Test: `packages/spark-advisor-rules/tests/test_rules.py`

- [ ] **Step 1: Write failing tests**

```python
from spark_advisor_rules.rules import ShuffleDataVolumeRule


class TestShuffleDataVolumeRule:
    def test_small_shuffle_no_finding(self) -> None:
        stage = make_stage(0, total_shuffle_write_bytes=1 * 1024**3)
        job = make_job(stages=[stage])
        assert ShuffleDataVolumeRule().evaluate(job) == []

    def test_large_shuffle_warning(self) -> None:
        stage = make_stage(0, total_shuffle_write_bytes=15 * 1024**3)
        job = make_job(stages=[stage])
        results = ShuffleDataVolumeRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING
        assert results[0].rule_id == "shuffle_data_volume"
        assert "15.0" in results[0].message
        assert results[0].stage_id == 0

    def test_zero_shuffle_no_finding(self) -> None:
        stage = make_stage(0, total_shuffle_write_bytes=0)
        job = make_job(stages=[stage])
        assert ShuffleDataVolumeRule().evaluate(job) == []

    def test_multiple_stages_detects_each(self) -> None:
        s1 = make_stage(0, total_shuffle_write_bytes=15 * 1024**3)
        s2 = make_stage(1, total_shuffle_write_bytes=20 * 1024**3)
        job = make_job(stages=[s1, s2])
        results = ShuffleDataVolumeRule().evaluate(job)
        assert len(results) == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd packages/spark-advisor-rules && uv run pytest tests/test_rules.py::TestShuffleDataVolumeRule -v`
Expected: FAIL

- [ ] **Step 3: Implement ShuffleDataVolumeRule**

```python
class ShuffleDataVolumeRule(Rule):
    rule_id: ClassVar[str] = "shuffle_data_volume"

    def _check_stage(self, stage: StageMetrics, job: JobAnalysis) -> RuleResult | None:
        shuffle_write_gb = stage.total_shuffle_write_bytes / (1024**3)
        if shuffle_write_gb <= self._thresholds.shuffle_volume_warning_gb:
            return None

        return self._result(
            Severity.WARNING,
            f"High shuffle volume in Stage {stage.stage_id}",
            f"Stage writes {shuffle_write_gb:.1f} GB shuffle data — consider pre-partitioning or bucketing",
            stage_id=stage.stage_id,
            current_value=f"{shuffle_write_gb:.1f} GB shuffle write",
            recommended_value="Use bucketing, pre-partition data, or reduce shuffle with broadcast joins",
            estimated_impact="Reduce disk I/O and network transfer",
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd packages/spark-advisor-rules && uv run pytest tests/test_rules.py::TestShuffleDataVolumeRule -v`
Expected: 4 passed

---

### Task 10: Implement `InputDataSkewRule`

**Files:**
- Modify: `packages/spark-advisor-rules/src/spark_advisor_rules/rules.py`
- Test: `packages/spark-advisor-rules/tests/test_rules.py`

- [ ] **Step 1: Write failing tests**

```python
from spark_advisor_rules.rules import InputDataSkewRule


class TestInputDataSkewRule:
    def test_no_skew_no_finding(self) -> None:
        stage = make_stage(0, input_bytes_median=100 * 1024**2, input_bytes_max=150 * 1024**2)
        job = make_job(stages=[stage])
        assert InputDataSkewRule().evaluate(job) == []

    def test_warning_skew(self) -> None:
        stage = make_stage(0, input_bytes_median=100 * 1024**2, input_bytes_max=700 * 1024**2)
        job = make_job(stages=[stage])
        results = InputDataSkewRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.WARNING
        assert results[0].rule_id == "input_data_skew"

    def test_critical_skew(self) -> None:
        stage = make_stage(0, input_bytes_median=100 * 1024**2, input_bytes_max=1500 * 1024**2)
        job = make_job(stages=[stage])
        results = InputDataSkewRule().evaluate(job)
        assert len(results) == 1
        assert results[0].severity == Severity.CRITICAL

    def test_zero_median_skip(self) -> None:
        stage = make_stage(0, input_bytes_median=0, input_bytes_max=0)
        job = make_job(stages=[stage])
        assert InputDataSkewRule().evaluate(job) == []

    def test_no_input_metrics_skip(self) -> None:
        stage = make_stage(0)
        job = make_job(stages=[stage])
        assert InputDataSkewRule().evaluate(job) == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd packages/spark-advisor-rules && uv run pytest tests/test_rules.py::TestInputDataSkewRule -v`
Expected: FAIL

- [ ] **Step 3: Implement InputDataSkewRule**

```python
from spark_advisor_models.util import format_bytes


class InputDataSkewRule(Rule):
    rule_id: ClassVar[str] = "input_data_skew"

    def _check_stage(self, stage: StageMetrics, job: JobAnalysis) -> RuleResult | None:
        input_bytes = stage.tasks.distributions.input_metrics.bytes
        median_input = input_bytes.median
        max_input = input_bytes.max

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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd packages/spark-advisor-rules && uv run pytest tests/test_rules.py::TestInputDataSkewRule -v`
Expected: 5 passed

---

### Task 11: Register rules + full verification

**Files:**
- Modify: `packages/spark-advisor-rules/src/spark_advisor_rules/rules.py:422-435` (update `rules_for_threshold`)

- [ ] **Step 1: Register 6 new rules in `rules_for_threshold()`**

Update `rules_for_threshold()` in `packages/spark-advisor-rules/src/spark_advisor_rules/rules.py`:

```python
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
```

- [ ] **Step 2: Run all rules tests**

Run: `cd packages/spark-advisor-rules && uv run pytest -v`
Expected: ~72 tests passed (45 existing + ~27 new)

- [ ] **Step 3: Run full `make check`**

Run: `make check`
Expected: All ~642 tests pass, 0 lint errors, mypy strict clean across all 9 packages

- [ ] **Step 4: Verify rule count in StaticAnalysisService**

Quick sanity check — add a test or verify manually:

```python
from spark_advisor_rules.static_analysis import StaticAnalysisService
service = StaticAnalysisService()
assert len(service._rules) == 17  # 11 existing + 6 new
```

---

### Task 12: Update CLAUDE.md and todo.md

**Files:**
- Modify: `CLAUDE.md` — update test count, rule count, "Current state" section
- Modify: `tasks/todo.md` — mark Phase 15 as done, update test count

- [ ] **Step 1: Update CLAUDE.md**

- Change `615 tests pass` → actual new count (~642)
- Change `11 rules implemented` → `17 rules implemented`
- Update test breakdown to include new test numbers

- [ ] **Step 2: Update todo.md**

- Mark Phase 15 as ✅
- Add details section with summary of changes
- Update Sprint 2 status

- [ ] **Step 3: Verify documentation accuracy**

Run: `make check` one more time and cross-check test count with docs.
