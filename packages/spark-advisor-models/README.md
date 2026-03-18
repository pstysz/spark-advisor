# spark-advisor-models

Shared Pydantic models and configuration for the [spark-advisor](https://github.com/pstysz/spark-advisor) ecosystem.

## Install

```bash
pip install spark-advisor-models
```

## What's inside

- **Spark job metrics** — stages, executors, tasks, quantile distributions (`model/metrics.py`)
- **Analysis output** — recommendations, severity levels, causal chains (`model/output.py`)
- **AI tool schemas** — input/output models for Claude API, generated from Pydantic (`model/input.py`)
- **Spark config wrapper** — typed access to spark.* properties (`model/spark_config.py`)
- **Shared configuration** — rule thresholds, AI settings, NATS settings (`config.py`, `settings.py`)
- **Structured logging** — `configure_logging()`, `bind_nats_context()`, `nats_handler_context()` (`logging.py`)
- **Distributed tracing** — OpenTelemetry W3C Traceparent propagation via NATS headers (`tracing.py`)
- **Utilities** — byte formatting, statistics helpers (`util/`)

## Usage

```python
from spark_advisor_models.model.metrics import JobAnalysis, StageMetrics
from spark_advisor_models.config import Thresholds, AiSettings
```

## Links

- [Main project](https://github.com/pstysz/spark-advisor)
- [Contributing](https://github.com/pstysz/spark-advisor/blob/main/CONTRIBUTING.md)

## License

Apache 2.0
