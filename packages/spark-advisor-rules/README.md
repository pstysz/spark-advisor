# spark-advisor-rules

Deterministic rules engine for Apache Spark job analysis. Part of the [spark-advisor](https://github.com/pstysz/spark-advisor) ecosystem.

## Install

```bash
pip install spark-advisor-rules
```

## What it detects

11 rules that identify common Spark performance problems:

| Rule | Detects |
|------|---------|
| **DataSkewRule** | Task duration skew (max/median > 5x) |
| **SpillToDiskRule** | Disk spill indicating insufficient memory |
| **GCPressureRule** | GC time > 20% of task time |
| **ShufflePartitionsRule** | Partition count far from optimal (128MB target) |
| **ExecutorIdleRule** | Slot utilization < 40% (CRITICAL if <20%) |
| **TaskFailureRule** | Failed tasks (CRITICAL if >=10, WARNING if >0) |
| **SmallFileRule** | Avg input bytes per task < 10MB (CRITICAL if <1MB) |
| **BroadcastJoinThresholdRule** | Broadcast join disabled or too low |
| **SerializerChoiceRule** | Java serializer used with shuffle stages |
| **DynamicAllocationRule** | Missing min/max bounds or disabled |
| **ExecutorMemoryOverheadRule** | High GC + high memory utilization |

All thresholds are configurable via `Thresholds` model.

## Usage

```python
from spark_advisor_rules.static_analysis import StaticAnalysisService

service = StaticAnalysisService()
results = service.analyze(job_analysis)
```

## Links

- [Main project](https://github.com/pstysz/spark-advisor)
- [Contributing](https://github.com/pstysz/spark-advisor/blob/main/CONTRIBUTING.md)

## License

Apache 2.0
