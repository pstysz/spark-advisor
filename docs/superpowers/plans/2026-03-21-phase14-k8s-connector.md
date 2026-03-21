# Phase 14: K8s Connector — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Kubernetes connector that discovers SparkApplication CRDs, delegates event log fetching to storage-connector, and reorganizes gateway API routes to support multiple data sources.

**Architecture:** New `spark-advisor-k8s-connector` package follows the same FastStream + SQLite + NATS pattern as hs-connector and storage-connector. K8s connector discovers CRDs via `kubernetes-asyncio`, extracts event log URIs, and delegates to storage-connector via existing `storage.fetch.{type}` NATS subjects. Gateway routes are reorganized from `/api/v1/analyze` to `/api/v1/hs/analyze` and `/api/v1/k8s/analyze`, with a new aggregator endpoint.

**Tech Stack:** kubernetes-asyncio, FastStream (NATS), SQLAlchemy + aiosqlite, Pydantic v2, structlog, OpenTelemetry

**Spec:** `docs/superpowers/specs/2026-03-21-phase14-k8s-connector-design.md`

---

## File Structure

### New files (spark-advisor-k8s-connector package)
```
packages/spark-advisor-k8s-connector/
├── pyproject.toml
├── Dockerfile
├── src/spark_advisor_k8s_connector/
│   ├── __init__.py
│   ├── app.py              # FastStream app lifecycle
│   ├── config.py           # K8sConnectorSettings, ContextKey
│   ├── client.py           # K8sClient — wrapper on kubernetes-asyncio
│   ├── mapper.py           # CRD dict → SparkApplicationRef, URI → storage type
│   ├── poller.py           # K8sPoller — background poll + delegate to storage-connector
│   ├── store.py            # K8sPollingStore — SQLite tracking
│   └── handlers.py         # NATS handlers: job.fetch.k8s, k8s.applications.list
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── test_client.py
    ├── test_mapper.py
    ├── test_poller.py
    ├── test_store.py
    └── test_handlers.py
```

### Modified files (spark-advisor-models)
```
packages/spark-advisor-models/src/spark_advisor_models/
├── model/
│   ├── k8s.py              # NEW: SparkApplicationRef
│   ├── input.py            # ADD: K8sFetchRequest, ListK8sAppsRequest
│   └── __init__.py         # ADD: SparkApplicationRef to exports
├── defaults.py             # ADD: NATS_K8S_APPLICATIONS_LIST_SUBJECT
└── tests/
    ├── test_k8s_model.py   # NEW: SparkApplicationRef tests
    └── test_input.py       # ADD: K8sFetchRequest validation tests
```

### Modified files (gateway)
```
packages/spark-advisor-gateway/src/spark_advisor_gateway/
├── config.py               # ADD: enabled_connectors, K8s NATS subjects + timeouts
├── task/executor.py        # ADD: list_k8s_applications(), submit_k8s()
├── api/
│   ├── routes.py           # MODIFY: split into hs/k8s/common routes
│   └── schemas.py          # ADD: source field to ApplicationResponse, K8sAnalyzeRequest
└── tests/                  # UPDATE: all URL changes
```

### Modified files (frontend)
```
packages/spark-advisor-frontend/src/
├── lib/api.ts              # UPDATE: API URLs
├── hooks/useAnalyze.ts     # ADD: source param
├── hooks/useApplications.ts # UPDATE: use aggregator endpoint
└── components/             # ADD: source badge, dropdown
```

### New files (Helm)
```
charts/k8s-connector/
├── Chart.yaml
├── values.yaml
├── templates/
│   ├── _helpers.tpl
│   ├── deployment.yaml
│   ├── pvc.yaml
│   ├── serviceaccount.yaml
│   ├── clusterrole.yaml
│   └── clusterrolebinding.yaml
```

### Modified files (CI/CD)
```
.github/workflows/ci.yml
release-please-config.json
Makefile
charts/spark-advisor/Chart.yaml
charts/spark-advisor/values.yaml
```

---

## Task 1: Models — SparkApplicationRef + NATS subjects + request models

**Files:**
- Create: `packages/spark-advisor-models/src/spark_advisor_models/model/k8s.py`
- Create: `packages/spark-advisor-models/tests/test_k8s_model.py`
- Modify: `packages/spark-advisor-models/src/spark_advisor_models/model/__init__.py`
- Modify: `packages/spark-advisor-models/src/spark_advisor_models/model/input.py`
- Modify: `packages/spark-advisor-models/src/spark_advisor_models/defaults.py`

- [ ] **Step 1: Write SparkApplicationRef model**

```python
# packages/spark-advisor-models/src/spark_advisor_models/model/k8s.py
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


class SparkApplicationRef(BaseModel):
    model_config = ConfigDict(frozen=True)

    # metadata
    name: str
    namespace: str
    labels: dict[str, str] = {}
    creation_timestamp: datetime | None = None

    # spec
    app_type: str | None = None
    main_application_file: str | None = None
    spark_version: str | None = None
    spark_conf: dict[str, str] = {}

    # spec.driver (requested resources)
    driver_cores: int | None = None
    driver_memory: str | None = None
    driver_memory_overhead: str | None = None

    # spec.executor (requested resources)
    executor_cores: int | None = None
    executor_memory: str | None = None
    executor_memory_overhead: str | None = None
    executor_instances: int | None = None

    # status
    app_id: str | None = None
    state: str | None = None
    error_message: str | None = None
    execution_attempts: int | None = None
    submitted_at: datetime | None = None
    completed_at: datetime | None = None

    # computed
    event_log_dir: str | None = None
    storage_type: str | None = None
```

- [ ] **Step 2: Add K8sFetchRequest and ListK8sAppsRequest to input.py**

Append to `packages/spark-advisor-models/src/spark_advisor_models/model/input.py`:

```python
from typing import Self
from pydantic import model_validator

class K8sFetchRequest(BaseModel):
    model_config = ConfigDict(frozen=True)
    namespace: str | None = None
    name: str | None = None
    app_id: str | None = None

    @model_validator(mode="after")
    def validate_identifier(self) -> Self:
        has_k8s_id = self.namespace is not None and self.name is not None
        has_app_id = self.app_id is not None
        if not has_k8s_id and not has_app_id:
            raise ValueError("Either (namespace + name) or app_id must be provided")
        return self


class ListK8sAppsRequest(BaseModel):
    model_config = ConfigDict(frozen=True)
    limit: int = Field(default=20, ge=1, le=500)
    offset: int = Field(default=0, ge=0)
    namespace: str | None = None
    state: str | None = None
    search: str | None = None
```

- [ ] **Step 3: Add NATS subject to defaults.py**

Add to `packages/spark-advisor-models/src/spark_advisor_models/defaults.py`:

```python
NATS_K8S_APPLICATIONS_LIST_SUBJECT = "k8s.applications.list"
```

- [ ] **Step 4: Export SparkApplicationRef from model/__init__.py**

Add import and `__all__` entry for `SparkApplicationRef` in `packages/spark-advisor-models/src/spark_advisor_models/model/__init__.py`:

```python
from spark_advisor_models.model.k8s import SparkApplicationRef
# Add "SparkApplicationRef" to __all__
```

- [ ] **Step 5: Write tests for SparkApplicationRef and K8sFetchRequest**

```python
# packages/spark-advisor-models/tests/test_k8s_model.py
import pytest
from pydantic import ValidationError

from spark_advisor_models.model.k8s import SparkApplicationRef
from spark_advisor_models.model.input import K8sFetchRequest, ListK8sAppsRequest


class TestSparkApplicationRef:
    def test_minimal_creation(self) -> None:
        ref = SparkApplicationRef(name="my-job", namespace="spark-prod")
        assert ref.name == "my-job"
        assert ref.namespace == "spark-prod"
        assert ref.labels == {}
        assert ref.spark_conf == {}

    def test_full_creation(self) -> None:
        ref = SparkApplicationRef(
            name="etl-daily",
            namespace="spark-prod",
            labels={"team": "data"},
            app_type="Scala",
            spark_version="3.5.1",
            spark_conf={"spark.eventLog.dir": "s3a://bucket/logs/"},
            driver_cores=1,
            driver_memory="2g",
            executor_cores=4,
            executor_memory="8g",
            executor_instances=10,
            app_id="spark-abc123",
            state="COMPLETED",
            event_log_dir="s3a://bucket/logs/",
            storage_type="s3",
        )
        assert ref.app_id == "spark-abc123"
        assert ref.executor_instances == 10

    def test_frozen(self) -> None:
        ref = SparkApplicationRef(name="x", namespace="y")
        with pytest.raises(ValidationError):
            ref.name = "z"  # type: ignore[misc]


class TestK8sFetchRequest:
    def test_valid_with_namespace_and_name(self) -> None:
        req = K8sFetchRequest(namespace="spark-prod", name="my-job")
        assert req.namespace == "spark-prod"

    def test_valid_with_app_id(self) -> None:
        req = K8sFetchRequest(app_id="spark-abc123")
        assert req.app_id == "spark-abc123"

    def test_valid_with_all_fields(self) -> None:
        req = K8sFetchRequest(namespace="ns", name="job", app_id="spark-1")
        assert req.namespace == "ns"

    def test_invalid_no_identifiers(self) -> None:
        with pytest.raises(ValidationError, match="Either"):
            K8sFetchRequest()

    def test_invalid_only_namespace(self) -> None:
        with pytest.raises(ValidationError, match="Either"):
            K8sFetchRequest(namespace="ns")

    def test_invalid_only_name(self) -> None:
        with pytest.raises(ValidationError, match="Either"):
            K8sFetchRequest(name="job")


class TestListK8sAppsRequest:
    def test_defaults(self) -> None:
        req = ListK8sAppsRequest()
        assert req.limit == 20
        assert req.offset == 0

    def test_with_filters(self) -> None:
        req = ListK8sAppsRequest(namespace="spark-prod", state="COMPLETED", search="etl")
        assert req.namespace == "spark-prod"
```

- [ ] **Step 6: Run tests**

Run: `cd packages/spark-advisor-models && uv run pytest tests/test_k8s_model.py -v`
Expected: All tests PASS

- [ ] **Step 7: Run full models test suite + lint**

Run: `cd packages/spark-advisor-models && uv run pytest -v && uv run ruff check src/ tests/ && uv run mypy src/`
Expected: All pass

- [ ] **Step 8: Commit**

```bash
git add packages/spark-advisor-models/
git commit -m "feat(models): add SparkApplicationRef, K8sFetchRequest, ListK8sAppsRequest for K8s connector"
```

---

## Task 2: K8s connector package scaffolding

**Files:**
- Create: `packages/spark-advisor-k8s-connector/pyproject.toml`
- Create: `packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/__init__.py`
- Create: `packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/config.py`
- Create: `packages/spark-advisor-k8s-connector/tests/__init__.py`
- Create: `packages/spark-advisor-k8s-connector/tests/conftest.py`

- [ ] **Step 1: Create pyproject.toml**

```toml
# packages/spark-advisor-k8s-connector/pyproject.toml
[project]
name = "spark-advisor-k8s-connector"
version = "0.1.16" # x-release-please-version
description = "Kubernetes connector for discovering SparkApplication CRDs and delegating event log analysis"
readme = { text = "K8s connector for spark-advisor", content-type = "text/plain" }
license = "Apache-2.0"
requires-python = ">=3.12"
authors = [
    { name = "Pawel Stysz" },
]
keywords = ["spark", "apache-spark", "kubernetes", "spark-operator"]
classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Apache Software License",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Typing :: Typed",
]
dependencies = [
    "spark-advisor-models==0.1.16",  # x-release-please-version
    "faststream[nats]>=0.6",
    "structlog>=24.0",
    "kubernetes-asyncio>=30.0",
    "sqlalchemy[asyncio]>=2.0",
    "aiosqlite>=0.20",
]

[project.urls]
Homepage = "https://github.com/pstysz/spark-advisor"
Repository = "https://github.com/pstysz/spark-advisor"

[project.scripts]
spark-advisor-k8s-connector = "spark_advisor_k8s_connector.app:main"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/spark_advisor_k8s_connector"]

[tool.uv.sources]
spark-advisor-models = { workspace = true }

[dependency-groups]
dev = [
    "pytest>=8.3",
    "pytest-cov>=6.1",
    "pytest-asyncio>=1.0",
    "mypy>=1.15",
    "ruff>=0.11",
]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src"]
addopts = [
    "-v",
    "--strict-markers",
    "--tb=short",
]
asyncio_mode = "strict"

[tool.ruff]
target-version = "py312"
line-length = 120
src = ["src", "tests"]

[tool.ruff.lint]
select = ["E", "W", "F", "I", "UP", "B", "SIM", "TCH", "RUF"]

[tool.ruff.lint.flake8-type-checking]
runtime-evaluated-base-classes = ["pydantic.BaseModel", "pydantic_settings.BaseSettings"]

[tool.ruff.lint.isort]
known-first-party = ["spark_advisor_k8s_connector", "spark_advisor_models"]

[tool.mypy]
python_version = "3.12"
strict = true
warn_return_any = true
warn_unused_configs = true
plugins = ["pydantic.mypy"]
```

- [ ] **Step 2: Create config.py**

```python
# packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/config.py
from __future__ import annotations

from enum import StrEnum

from pydantic_settings import SettingsConfigDict

from spark_advisor_models.defaults import NATS_FETCH_JOB_K8S_SUBJECT, NATS_K8S_APPLICATIONS_LIST_SUBJECT
from spark_advisor_models.settings import BaseConnectorNatsSettings, BaseConnectorSettings


class ContextKey(StrEnum):
    CLIENT = "k8s_client"
    POLLER = "poller"
    POLLING_TASK = "polling_task"
    POLLING_STORE = "polling_store"
    SERVICE_NAME = "service_name"
    SETTINGS = "settings"


STORAGE_FETCH_SUBJECTS: dict[str, str] = {
    "hdfs": "storage.fetch.hdfs",
    "s3": "storage.fetch.s3",
    "gcs": "storage.fetch.gcs",
}


class K8sNatsSettings(BaseConnectorNatsSettings):
    fetch_subject: str = NATS_FETCH_JOB_K8S_SUBJECT
    list_apps_subject: str = NATS_K8S_APPLICATIONS_LIST_SUBJECT


class K8sConnectorSettings(BaseConnectorSettings):
    model_config = SettingsConfigDict(
        env_prefix="SA_K8S_CONNECTOR_",
        yaml_file="/etc/spark-advisor/k8s-connector/config.yaml",
    )

    service_name: str = "spark-advisor-k8s-connector"
    nats: K8sNatsSettings = K8sNatsSettings()

    namespaces: list[str] = ["default"]
    label_selector: str | None = None
    application_states: list[str] = ["COMPLETED", "FAILED"]
    max_age_days: int = 7
    kubeconfig_path: str | None = None

    default_event_log_dir: str | None = None
    default_storage_type: str = "hdfs"

    database_url: str = "sqlite+aiosqlite:///data/k8s_connector.db"
```

- [ ] **Step 3: Create __init__.py and tests scaffolding**

```python
# packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/__init__.py
# (empty)
```

```python
# packages/spark-advisor-k8s-connector/tests/__init__.py
# (empty)
```

```python
# packages/spark-advisor-k8s-connector/tests/conftest.py
from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def make_crd(
    name: str = "my-spark-job",
    namespace: str = "spark-prod",
    *,
    app_id: str | None = "spark-abc123",
    state: str = "COMPLETED",
    app_type: str = "Scala",
    spark_version: str = "3.5.1",
    spark_conf: dict[str, str] | None = None,
    driver_cores: int = 1,
    driver_memory: str = "2g",
    executor_cores: int = 4,
    executor_memory: str = "8g",
    executor_instances: int = 10,
    event_log_dir: str = "s3a://my-bucket/spark-logs/",
    error_message: str | None = None,
    creation_timestamp: str = "2026-03-21T10:00:00Z",
    termination_time: str = "2026-03-21T10:05:00Z",
    submission_time: str = "2026-03-21T09:59:00Z",
    labels: dict[str, str] | None = None,
    main_application_file: str = "s3a://bucket/app.jar",
) -> dict[str, Any]:
    conf = spark_conf if spark_conf is not None else {
        "spark.eventLog.enabled": "true",
        "spark.eventLog.dir": event_log_dir,
        "spark.executor.memory": executor_memory,
        "spark.executor.cores": str(executor_cores),
    }
    status: dict[str, Any] = {
        "applicationState": {"state": state},
        "executionAttempts": 1,
        "lastSubmissionAttemptTime": submission_time,
        "terminationTime": termination_time,
    }
    if app_id is not None:
        status["sparkApplicationId"] = app_id
    if error_message is not None:
        status["applicationState"]["errorMessage"] = error_message

    return {
        "metadata": {
            "name": name,
            "namespace": namespace,
            "creationTimestamp": creation_timestamp,
            "labels": labels or {},
        },
        "spec": {
            "type": app_type,
            "mainApplicationFile": main_application_file,
            "sparkVersion": spark_version,
            "sparkConf": conf,
            "driver": {
                "cores": driver_cores,
                "memory": driver_memory,
            },
            "executor": {
                "cores": executor_cores,
                "memory": executor_memory,
                "instances": executor_instances,
            },
        },
        "status": status,
    }
```

- [ ] **Step 4: Register package in uv workspace**

Add to root `pyproject.toml` members list:
```toml
members = [
    # ... existing ...
    "packages/spark-advisor-k8s-connector",
]
```

- [ ] **Step 5: Run uv sync and verify**

Run: `cd /Users/pstysz/git/spark-advisor && uv sync --all-packages`
Expected: successful sync with kubernetes-asyncio installed

- [ ] **Step 6: Run lint on new package**

Run: `cd packages/spark-advisor-k8s-connector && uv run ruff check src/ tests/ && uv run mypy src/`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add packages/spark-advisor-k8s-connector/ pyproject.toml uv.lock
git commit -m "feat(k8s-connector): scaffold package with config and test fixtures"
```

---

## Task 3: K8sMapper — CRD dict to SparkApplicationRef + URI parsing

**Files:**
- Create: `packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/mapper.py`
- Create: `packages/spark-advisor-k8s-connector/tests/test_mapper.py`

- [ ] **Step 1: Write mapper tests**

```python
# packages/spark-advisor-k8s-connector/tests/test_mapper.py
import pytest

from spark_advisor_k8s_connector.mapper import map_crd, resolve_storage_type
from tests.conftest import make_crd


class TestMapCrd:
    def test_maps_full_crd(self) -> None:
        crd = make_crd()
        ref = map_crd(crd, default_event_log_dir=None, default_storage_type="hdfs")
        assert ref.name == "my-spark-job"
        assert ref.namespace == "spark-prod"
        assert ref.app_id == "spark-abc123"
        assert ref.state == "COMPLETED"
        assert ref.app_type == "Scala"
        assert ref.executor_memory == "8g"
        assert ref.executor_instances == 10
        assert ref.driver_cores == 1
        assert ref.event_log_dir == "s3a://my-bucket/spark-logs/"
        assert ref.storage_type == "s3"

    def test_uses_default_event_log_dir_when_missing(self) -> None:
        crd = make_crd(spark_conf={"spark.executor.memory": "4g"})
        ref = map_crd(crd, default_event_log_dir="hdfs:///spark-logs/", default_storage_type="hdfs")
        assert ref.event_log_dir == "hdfs:///spark-logs/"
        assert ref.storage_type == "hdfs"

    def test_no_event_log_dir_and_no_fallback(self) -> None:
        crd = make_crd(spark_conf={})
        ref = map_crd(crd, default_event_log_dir=None, default_storage_type="hdfs")
        assert ref.event_log_dir is None
        assert ref.storage_type is None

    def test_missing_app_id(self) -> None:
        crd = make_crd(app_id=None)
        ref = map_crd(crd, default_event_log_dir=None, default_storage_type="hdfs")
        assert ref.app_id is None

    def test_failed_state_with_error(self) -> None:
        crd = make_crd(state="FAILED", error_message="OOM killed")
        ref = map_crd(crd, default_event_log_dir=None, default_storage_type="hdfs")
        assert ref.state == "FAILED"
        assert ref.error_message == "OOM killed"

    def test_labels_preserved(self) -> None:
        crd = make_crd(labels={"team": "data", "env": "prod"})
        ref = map_crd(crd, default_event_log_dir=None, default_storage_type="hdfs")
        assert ref.labels == {"team": "data", "env": "prod"}

    def test_missing_driver_section(self) -> None:
        crd = make_crd()
        del crd["spec"]["driver"]
        ref = map_crd(crd, default_event_log_dir=None, default_storage_type="hdfs")
        assert ref.driver_cores is None
        assert ref.driver_memory is None

    def test_missing_executor_section(self) -> None:
        crd = make_crd()
        del crd["spec"]["executor"]
        ref = map_crd(crd, default_event_log_dir=None, default_storage_type="hdfs")
        assert ref.executor_cores is None
        assert ref.executor_instances is None


class TestResolveStorageType:
    def test_s3a_scheme(self) -> None:
        assert resolve_storage_type("s3a://bucket/path", "hdfs") == "s3"

    def test_s3_scheme(self) -> None:
        assert resolve_storage_type("s3://bucket/path", "hdfs") == "s3"

    def test_hdfs_scheme(self) -> None:
        assert resolve_storage_type("hdfs://namenode/path", "s3") == "hdfs"

    def test_gs_scheme(self) -> None:
        assert resolve_storage_type("gs://bucket/path", "hdfs") == "gcs"

    def test_no_scheme_uses_default(self) -> None:
        assert resolve_storage_type("/spark-logs/", "hdfs") == "hdfs"

    def test_no_scheme_uses_default_s3(self) -> None:
        assert resolve_storage_type("/logs/", "s3") == "s3"

    def test_file_scheme_returns_none(self) -> None:
        assert resolve_storage_type("file:///tmp/logs/", "hdfs") is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd packages/spark-advisor-k8s-connector && uv run pytest tests/test_mapper.py -v`
Expected: FAIL (mapper module not found)

- [ ] **Step 3: Implement mapper**

```python
# packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/mapper.py
from __future__ import annotations

from datetime import datetime
from typing import Any

from spark_advisor_models.model.k8s import SparkApplicationRef

_URI_SCHEME_TO_STORAGE: dict[str, str] = {
    "hdfs": "hdfs",
    "s3a": "s3",
    "s3": "s3",
    "gs": "gcs",
}


def resolve_storage_type(uri: str, default: str) -> str | None:
    scheme = uri.split("://", 1)[0].lower() if "://" in uri else ""
    if scheme == "file":
        return None
    return _URI_SCHEME_TO_STORAGE.get(scheme, default)


def map_crd(
    crd: dict[str, Any],
    *,
    default_event_log_dir: str | None,
    default_storage_type: str,
) -> SparkApplicationRef:
    metadata = crd.get("metadata", {})
    spec = crd.get("spec", {})
    status = crd.get("status", {})
    spark_conf: dict[str, str] = spec.get("sparkConf", {})
    driver = spec.get("driver", {})
    executor = spec.get("executor", {})
    app_state = status.get("applicationState", {})

    event_log_dir = spark_conf.get("spark.eventLog.dir") or default_event_log_dir
    storage_type = resolve_storage_type(event_log_dir, default_storage_type) if event_log_dir else None

    return SparkApplicationRef(
        name=metadata.get("name", ""),
        namespace=metadata.get("namespace", ""),
        labels=metadata.get("labels", {}),
        creation_timestamp=_parse_timestamp(metadata.get("creationTimestamp")),
        app_type=spec.get("type"),
        main_application_file=spec.get("mainApplicationFile"),
        spark_version=spec.get("sparkVersion"),
        spark_conf=spark_conf,
        driver_cores=driver.get("cores"),
        driver_memory=driver.get("memory"),
        driver_memory_overhead=driver.get("memoryOverhead"),
        executor_cores=executor.get("cores"),
        executor_memory=executor.get("memory"),
        executor_memory_overhead=executor.get("memoryOverhead"),
        executor_instances=executor.get("instances"),
        app_id=status.get("sparkApplicationId"),
        state=app_state.get("state"),
        error_message=app_state.get("errorMessage"),
        execution_attempts=status.get("executionAttempts"),
        submitted_at=_parse_timestamp(status.get("lastSubmissionAttemptTime")),
        completed_at=_parse_timestamp(status.get("terminationTime")),
        event_log_dir=event_log_dir,
        storage_type=storage_type,
    )


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
```

- [ ] **Step 4: Run tests**

Run: `cd packages/spark-advisor-k8s-connector && uv run pytest tests/test_mapper.py -v`
Expected: All PASS

- [ ] **Step 5: Lint**

Run: `cd packages/spark-advisor-k8s-connector && uv run ruff check src/ tests/ && uv run mypy src/`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/mapper.py packages/spark-advisor-k8s-connector/tests/test_mapper.py
git commit -m "feat(k8s-connector): add K8sMapper — CRD to SparkApplicationRef with URI parsing"
```

---

## Task 4: K8sClient — wrapper on kubernetes-asyncio

**Files:**
- Create: `packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/client.py`
- Create: `packages/spark-advisor-k8s-connector/tests/test_client.py`

- [ ] **Step 1: Write client tests**

```python
# packages/spark-advisor-k8s-connector/tests/test_client.py
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from spark_advisor_k8s_connector.client import K8sClient
from tests.conftest import make_crd

CRD_GROUP = "sparkoperator.k8s.io"
CRD_VERSION = "v1beta2"
CRD_PLURAL = "sparkapplications"


@pytest.fixture
def mock_api() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def client(mock_api: AsyncMock) -> K8sClient:
    c = K8sClient.__new__(K8sClient)
    c._api = mock_api
    c._api_client = MagicMock()
    return c


class TestListApplications:
    @pytest.mark.asyncio
    async def test_lists_from_single_namespace(self, client: K8sClient, mock_api: AsyncMock) -> None:
        mock_api.list_namespaced_custom_object.return_value = {"items": [make_crd()]}
        result = await client.list_applications(namespaces=["spark-prod"])
        assert len(result) == 1
        mock_api.list_namespaced_custom_object.assert_called_once_with(
            CRD_GROUP, CRD_VERSION, "spark-prod", CRD_PLURAL,
            label_selector=None,
        )

    @pytest.mark.asyncio
    async def test_lists_from_multiple_namespaces(self, client: K8sClient, mock_api: AsyncMock) -> None:
        mock_api.list_namespaced_custom_object.side_effect = [
            {"items": [make_crd(namespace="ns1")]},
            {"items": [make_crd(namespace="ns2")]},
        ]
        result = await client.list_applications(namespaces=["ns1", "ns2"])
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_with_label_selector(self, client: K8sClient, mock_api: AsyncMock) -> None:
        mock_api.list_namespaced_custom_object.return_value = {"items": []}
        await client.list_applications(namespaces=["default"], label_selector="team=data")
        mock_api.list_namespaced_custom_object.assert_called_once_with(
            CRD_GROUP, CRD_VERSION, "default", CRD_PLURAL,
            label_selector="team=data",
        )

    @pytest.mark.asyncio
    async def test_empty_result(self, client: K8sClient, mock_api: AsyncMock) -> None:
        mock_api.list_namespaced_custom_object.return_value = {"items": []}
        result = await client.list_applications(namespaces=["default"])
        assert result == []


class TestGetApplication:
    @pytest.mark.asyncio
    async def test_get_by_name(self, client: K8sClient, mock_api: AsyncMock) -> None:
        mock_api.get_namespaced_custom_object.return_value = make_crd()
        result = await client.get_application("spark-prod", "my-spark-job")
        assert result is not None
        mock_api.get_namespaced_custom_object.assert_called_once_with(
            CRD_GROUP, CRD_VERSION, "spark-prod", CRD_PLURAL, "my-spark-job",
        )

    @pytest.mark.asyncio
    async def test_find_by_app_id(self, client: K8sClient, mock_api: AsyncMock) -> None:
        crds = [make_crd(app_id="spark-abc123"), make_crd(name="other", app_id="spark-other")]
        mock_api.list_namespaced_custom_object.return_value = {"items": crds}
        result = await client.find_by_app_id(["default"], "spark-abc123")
        assert result is not None
        assert result["status"]["sparkApplicationId"] == "spark-abc123"

    @pytest.mark.asyncio
    async def test_find_by_app_id_not_found(self, client: K8sClient, mock_api: AsyncMock) -> None:
        mock_api.list_namespaced_custom_object.return_value = {"items": []}
        result = await client.find_by_app_id(["default"], "nonexistent")
        assert result is None


class TestClose:
    @pytest.mark.asyncio
    async def test_close(self, client: K8sClient, mock_api: AsyncMock) -> None:
        client._api_client.close = AsyncMock()
        await client.close()
        client._api_client.close.assert_called_once()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd packages/spark-advisor-k8s-connector && uv run pytest tests/test_client.py -v`
Expected: FAIL

- [ ] **Step 3: Implement client**

```python
# packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/client.py
from __future__ import annotations

from typing import Any

import structlog
from kubernetes_asyncio import client as k8s_client
from kubernetes_asyncio import config as k8s_config
from kubernetes_asyncio.client import ApiClient, CustomObjectsApi

logger = structlog.stdlib.get_logger(__name__)

CRD_GROUP = "sparkoperator.k8s.io"
CRD_VERSION = "v1beta2"
CRD_PLURAL = "sparkapplications"


class K8sClient:
    def __init__(self, api_client: ApiClient) -> None:
        self._api_client = api_client
        self._api = CustomObjectsApi(api_client)

    @classmethod
    async def create(cls, kubeconfig_path: str | None = None) -> K8sClient:
        try:
            k8s_config.load_incluster_config()
            logger.info("Using in-cluster K8s config")
        except k8s_config.ConfigException:
            await k8s_config.load_kube_config(config_file=kubeconfig_path)
            logger.info("Using kubeconfig: %s", kubeconfig_path or "default")
        api_client = ApiClient()
        return cls(api_client)

    async def list_applications(
        self,
        namespaces: list[str],
        *,
        label_selector: str | None = None,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for ns in namespaces:
            response = await self._api.list_namespaced_custom_object(
                CRD_GROUP, CRD_VERSION, ns, CRD_PLURAL,
                label_selector=label_selector,
            )
            results.extend(response.get("items", []))
        return results

    async def get_application(self, namespace: str, name: str) -> dict[str, Any]:
        return await self._api.get_namespaced_custom_object(
            CRD_GROUP, CRD_VERSION, namespace, CRD_PLURAL, name,
        )

    async def find_by_app_id(self, namespaces: list[str], app_id: str) -> dict[str, Any] | None:
        all_crds = await self.list_applications(namespaces)
        for crd in all_crds:
            if crd.get("status", {}).get("sparkApplicationId") == app_id:
                return crd
        return None

    async def close(self) -> None:
        await self._api_client.close()
```

- [ ] **Step 4: Run tests**

Run: `cd packages/spark-advisor-k8s-connector && uv run pytest tests/test_client.py -v`
Expected: All PASS

- [ ] **Step 5: Lint**

Run: `cd packages/spark-advisor-k8s-connector && uv run ruff check src/ tests/ && uv run mypy src/`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/client.py packages/spark-advisor-k8s-connector/tests/test_client.py
git commit -m "feat(k8s-connector): add K8sClient — kubernetes-asyncio wrapper for SparkApplication CRDs"
```

---

## Task 5: K8sPollingStore — SQLite tracking

**Files:**
- Create: `packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/store.py`
- Create: `packages/spark-advisor-k8s-connector/tests/test_store.py`

- [ ] **Step 1: Write store tests**

```python
# packages/spark-advisor-k8s-connector/tests/test_store.py
import pytest

from spark_advisor_k8s_connector.store import K8sPollingStore


@pytest.fixture
async def store() -> K8sPollingStore:
    s = K8sPollingStore(database_url="sqlite+aiosqlite:///:memory:")
    await s.init()
    yield s
    await s.close()


class TestK8sPollingStore:
    @pytest.mark.asyncio
    async def test_is_processed_returns_false_for_new(self, store: K8sPollingStore) -> None:
        assert not await store.is_processed("spark-prod", "my-job", "2026-03-21T10:05:00")

    @pytest.mark.asyncio
    async def test_mark_processed_and_check(self, store: K8sPollingStore) -> None:
        await store.mark_processed("spark-prod", "my-job", "2026-03-21T10:05:00")
        assert await store.is_processed("spark-prod", "my-job", "2026-03-21T10:05:00")

    @pytest.mark.asyncio
    async def test_different_completed_at_not_processed(self, store: K8sPollingStore) -> None:
        await store.mark_processed("spark-prod", "my-job", "2026-03-21T10:05:00")
        assert not await store.is_processed("spark-prod", "my-job", "2026-03-21T11:00:00")

    @pytest.mark.asyncio
    async def test_same_name_different_namespace(self, store: K8sPollingStore) -> None:
        await store.mark_processed("ns1", "my-job", "2026-03-21T10:05:00")
        assert not await store.is_processed("ns2", "my-job", "2026-03-21T10:05:00")

    @pytest.mark.asyncio
    async def test_remove(self, store: K8sPollingStore) -> None:
        await store.mark_processed("spark-prod", "my-job", "2026-03-21T10:05:00")
        await store.remove("spark-prod", "my-job", "2026-03-21T10:05:00")
        assert not await store.is_processed("spark-prod", "my-job", "2026-03-21T10:05:00")

    @pytest.mark.asyncio
    async def test_eviction(self) -> None:
        s = K8sPollingStore(database_url="sqlite+aiosqlite:///:memory:", max_size=3)
        await s.init()
        for i in range(5):
            await s.mark_processed("ns", f"job-{i}", f"2026-03-21T10:0{i}:00")
        assert not await s.is_processed("ns", "job-0", "2026-03-21T10:00:00")
        assert not await s.is_processed("ns", "job-1", "2026-03-21T10:01:00")
        assert await s.is_processed("ns", "job-4", "2026-03-21T10:04:00")
        await s.close()
```

- [ ] **Step 2: Implement store**

```python
# packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/store.py
from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime, String, delete, func, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine as _create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class _Base(DeclarativeBase):
    pass


class ProcessedAppRow(_Base):
    __tablename__ = "processed_apps"

    key: Mapped[str] = mapped_column(String(1024), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)


class K8sPollingStore:
    def __init__(self, database_url: str, max_size: int = 10_000) -> None:
        self._engine: AsyncEngine = _create_async_engine(database_url, echo=False)
        self._session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            self._engine, expire_on_commit=False
        )
        self._max_size = max_size

    async def init(self) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(text("PRAGMA journal_mode=WAL"))
            await conn.run_sync(_Base.metadata.create_all)

    async def close(self) -> None:
        await self._engine.dispose()

    @staticmethod
    def _make_key(namespace: str, name: str, completed_at: str) -> str:
        return f"{namespace}/{name}@{completed_at}"

    async def is_processed(self, namespace: str, name: str, completed_at: str) -> bool:
        key = self._make_key(namespace, name, completed_at)
        async with self._session_factory() as session:
            result = await session.execute(
                select(ProcessedAppRow.key).where(ProcessedAppRow.key == key)
            )
            return result.scalar_one_or_none() is not None

    async def mark_processed(self, namespace: str, name: str, completed_at: str) -> None:
        key = self._make_key(namespace, name, completed_at)
        async with self._session_factory() as session, session.begin():
            session.add(ProcessedAppRow(key=key, processed_at=_now()))
            await self._evict_if_needed(session)

    async def remove(self, namespace: str, name: str, completed_at: str) -> None:
        key = self._make_key(namespace, name, completed_at)
        async with self._session_factory() as session, session.begin():
            await session.execute(delete(ProcessedAppRow).where(ProcessedAppRow.key == key))

    async def _evict_if_needed(self, session: AsyncSession) -> None:
        count_result = await session.execute(select(func.count()).select_from(ProcessedAppRow))
        total: int = count_result.scalar_one()
        if total <= self._max_size:
            return
        excess = total - self._max_size
        oldest = await session.execute(
            select(ProcessedAppRow.key).order_by(ProcessedAppRow.processed_at.asc()).limit(excess)
        )
        old_keys = [row[0] for row in oldest]
        if old_keys:
            await session.execute(delete(ProcessedAppRow).where(ProcessedAppRow.key.in_(old_keys)))


def _now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)
```

- [ ] **Step 3: Run tests**

Run: `cd packages/spark-advisor-k8s-connector && uv run pytest tests/test_store.py -v`
Expected: All PASS

- [ ] **Step 4: Commit**

```bash
git add packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/store.py packages/spark-advisor-k8s-connector/tests/test_store.py
git commit -m "feat(k8s-connector): add K8sPollingStore — SQLite tracking with namespace/name+completedAt key"
```

---

## Task 6: K8sPoller — background poll + delegate to storage-connector

**Files:**
- Create: `packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/poller.py`
- Create: `packages/spark-advisor-k8s-connector/tests/test_poller.py`

- [ ] **Step 1: Write poller tests**

```python
# packages/spark-advisor-k8s-connector/tests/test_poller.py
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from spark_advisor_k8s_connector.config import K8sConnectorSettings
from spark_advisor_k8s_connector.poller import K8sPoller
from spark_advisor_k8s_connector.store import K8sPollingStore
from tests.conftest import make_crd


@pytest.fixture
def mock_client() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def mock_broker() -> AsyncMock:
    broker = AsyncMock()
    broker.request.return_value = AsyncMock(body=b'{"app_id": "spark-abc123"}')
    return broker


@pytest.fixture
async def store() -> K8sPollingStore:
    s = K8sPollingStore(database_url="sqlite+aiosqlite:///:memory:")
    await s.init()
    yield s
    await s.close()


@pytest.fixture
def settings() -> K8sConnectorSettings:
    return K8sConnectorSettings(
        namespaces=["spark-prod"],
        application_states=["COMPLETED"],
        max_age_days=7,
        default_event_log_dir="s3a://bucket/logs/",
        default_storage_type="s3",
    )


@pytest.fixture
def poller(
    mock_client: AsyncMock,
    mock_broker: AsyncMock,
    store: K8sPollingStore,
    settings: K8sConnectorSettings,
) -> K8sPoller:
    return K8sPoller(
        client=mock_client,
        broker=mock_broker,
        store=store,
        settings=settings,
    )


class TestPoll:
    @pytest.mark.asyncio
    async def test_processes_new_completed_app(self, poller: K8sPoller, mock_client: AsyncMock) -> None:
        mock_client.list_applications.return_value = [make_crd()]
        count = await poller.poll()
        assert count == 1

    @pytest.mark.asyncio
    async def test_skips_already_processed(self, poller: K8sPoller, mock_client: AsyncMock, store: K8sPollingStore) -> None:
        await store.mark_processed("spark-prod", "my-spark-job", "2026-03-21T10:05:00+00:00")
        mock_client.list_applications.return_value = [make_crd()]
        count = await poller.poll()
        assert count == 0

    @pytest.mark.asyncio
    async def test_skips_running_state(self, poller: K8sPoller, mock_client: AsyncMock) -> None:
        mock_client.list_applications.return_value = [make_crd(state="RUNNING")]
        count = await poller.poll()
        assert count == 0

    @pytest.mark.asyncio
    async def test_skips_old_apps(self, poller: K8sPoller, mock_client: AsyncMock) -> None:
        old_time = (datetime.now(UTC) - timedelta(days=30)).isoformat()
        mock_client.list_applications.return_value = [make_crd(termination_time=old_time)]
        count = await poller.poll()
        assert count == 0

    @pytest.mark.asyncio
    async def test_skips_no_event_log_dir(self, poller: K8sPoller, mock_client: AsyncMock) -> None:
        settings = K8sConnectorSettings(
            namespaces=["spark-prod"],
            default_event_log_dir=None,
            default_storage_type="hdfs",
        )
        p = K8sPoller(client=mock_client, broker=poller._broker, store=poller._store, settings=settings)
        mock_client.list_applications.return_value = [make_crd(spark_conf={})]
        count = await p.poll()
        assert count == 0

    @pytest.mark.asyncio
    async def test_empty_cluster(self, poller: K8sPoller, mock_client: AsyncMock) -> None:
        mock_client.list_applications.return_value = []
        count = await poller.poll()
        assert count == 0

    @pytest.mark.asyncio
    async def test_delegates_to_storage_connector(
        self, poller: K8sPoller, mock_client: AsyncMock, mock_broker: AsyncMock,
    ) -> None:
        mock_client.list_applications.return_value = [make_crd()]
        await poller.poll()
        mock_broker.request.assert_called_once()
        call_args = mock_broker.request.call_args
        assert "storage.fetch.s3" in str(call_args)

    @pytest.mark.asyncio
    async def test_removes_from_store_on_delegate_failure(
        self, poller: K8sPoller, mock_client: AsyncMock, mock_broker: AsyncMock, store: K8sPollingStore,
    ) -> None:
        mock_client.list_applications.return_value = [make_crd()]
        mock_broker.request.side_effect = Exception("storage down")
        count = await poller.poll()
        assert count == 0
        assert not await store.is_processed("spark-prod", "my-spark-job", "2026-03-21T10:05:00+00:00")
```

- [ ] **Step 2: Implement poller**

```python
# packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/poller.py
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import structlog

from spark_advisor_k8s_connector.config import STORAGE_FETCH_SUBJECTS
from spark_advisor_k8s_connector.mapper import map_crd
from spark_advisor_models.model.input import StorageFetchRequest
from spark_advisor_models.tracing import get_tracer, inject_correlation_context

if TYPE_CHECKING:
    from faststream.nats import NatsBroker

    from spark_advisor_k8s_connector.client import K8sClient
    from spark_advisor_k8s_connector.config import K8sConnectorSettings
    from spark_advisor_k8s_connector.store import K8sPollingStore

logger = structlog.stdlib.get_logger(__name__)


class K8sPoller:
    def __init__(
        self,
        client: K8sClient,
        broker: NatsBroker,
        store: K8sPollingStore,
        settings: K8sConnectorSettings,
    ) -> None:
        self._client = client
        self._broker = broker
        self._store = store
        self._settings = settings

    async def poll(self) -> int:
        tracer = get_tracer()
        with tracer.start_as_current_span("k8s.poll_cycle"):
            crds = await self._client.list_applications(
                self._settings.namespaces,
                label_selector=self._settings.label_selector,
            )
            cutoff = datetime.now(UTC) - timedelta(days=self._settings.max_age_days)
            published = 0
            for crd in crds:
                ref = map_crd(
                    crd,
                    default_event_log_dir=self._settings.default_event_log_dir,
                    default_storage_type=self._settings.default_storage_type,
                )

                if ref.state not in self._settings.application_states:
                    continue
                if ref.completed_at and ref.completed_at < cutoff:
                    continue
                if not ref.event_log_dir or not ref.storage_type:
                    logger.warning("No event log dir for %s/%s, skipping", ref.namespace, ref.name)
                    continue
                if not ref.app_id:
                    logger.warning("No app_id for %s/%s, skipping", ref.namespace, ref.name)
                    continue

                completed_at_str = ref.completed_at.isoformat() if ref.completed_at else ""
                if await self._store.is_processed(ref.namespace, ref.name, completed_at_str):
                    continue

                await self._store.mark_processed(ref.namespace, ref.name, completed_at_str)
                try:
                    await self._delegate_to_storage(ref.app_id, ref.event_log_dir, ref.storage_type, ref.spark_conf)
                    published += 1
                except Exception:
                    logger.exception("Failed to delegate %s/%s to storage-connector", ref.namespace, ref.name)
                    await self._store.remove(ref.namespace, ref.name, completed_at_str)

            if published:
                logger.info("Poll complete: %d new applications delegated", published)
            return published

    async def _delegate_to_storage(
        self,
        app_id: str,
        event_log_dir: str,
        storage_type: str,
        spark_conf: dict[str, str],
    ) -> None:
        tracer = get_tracer()
        event_log_uri = f"{event_log_dir.rstrip('/')}/{app_id}"
        subject = STORAGE_FETCH_SUBJECTS.get(storage_type)
        if not subject:
            raise ValueError(f"Unknown storage type: {storage_type}")

        with tracer.start_as_current_span("k8s.delegate_to_storage", attributes={"app_id": app_id, "subject": subject}):
            request = StorageFetchRequest(app_id=app_id, event_log_uri=event_log_uri, spark_conf=spark_conf)
            headers = inject_correlation_context({})
            reply = await self._broker.request(
                request.model_dump(mode="json"),
                subject=subject,
                headers=headers,
                timeout=60.0,
            )
            headers_out = inject_correlation_context({})
            await self._broker.publish(
                reply.body,
                subject=self._settings.nats.analysis_submit_subject,
                headers=headers_out,
            )
```

- [ ] **Step 3: Run tests**

Run: `cd packages/spark-advisor-k8s-connector && uv run pytest tests/test_poller.py -v`
Expected: All PASS

- [ ] **Step 4: Lint**

Run: `cd packages/spark-advisor-k8s-connector && uv run ruff check src/ tests/ && uv run mypy src/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/poller.py packages/spark-advisor-k8s-connector/tests/test_poller.py
git commit -m "feat(k8s-connector): add K8sPoller — background poll with storage-connector delegation"
```

---

## Task 7: NATS handlers

**Files:**
- Create: `packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/handlers.py`
- Create: `packages/spark-advisor-k8s-connector/tests/test_handlers.py`

- [ ] **Step 1: Write handler tests**

```python
# packages/spark-advisor-k8s-connector/tests/test_handlers.py
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from spark_advisor_k8s_connector.handlers import create_router
from spark_advisor_k8s_connector.config import ContextKey, K8sConnectorSettings
from tests.conftest import make_crd


class TestHandleFetchK8s:
    @pytest.mark.asyncio
    async def test_fetch_by_namespace_and_name(self) -> None:
        from spark_advisor_k8s_connector.handlers import _handle_fetch_k8s_logic

        mock_client = AsyncMock()
        mock_client.get_application.return_value = make_crd()

        mock_broker = AsyncMock()
        mock_broker.request.return_value = AsyncMock(body=b'{"app_id": "spark-abc123"}')

        settings = K8sConnectorSettings(default_event_log_dir="s3a://bucket/logs/", default_storage_type="s3")

        result = await _handle_fetch_k8s_logic(
            namespace="spark-prod",
            name="my-spark-job",
            app_id=None,
            client=mock_client,
            broker=mock_broker,
            settings=settings,
        )
        assert "error" not in result or result.get("error") is None
        mock_client.get_application.assert_called_once_with("spark-prod", "my-spark-job")

    @pytest.mark.asyncio
    async def test_fetch_by_app_id(self) -> None:
        from spark_advisor_k8s_connector.handlers import _handle_fetch_k8s_logic

        mock_client = AsyncMock()
        mock_client.find_by_app_id.return_value = make_crd()

        mock_broker = AsyncMock()
        mock_broker.request.return_value = AsyncMock(body=b'{"app_id": "spark-abc123"}')

        settings = K8sConnectorSettings(
            namespaces=["spark-prod"],
            default_event_log_dir="s3a://bucket/logs/",
            default_storage_type="s3",
        )

        result = await _handle_fetch_k8s_logic(
            namespace=None,
            name=None,
            app_id="spark-abc123",
            client=mock_client,
            broker=mock_broker,
            settings=settings,
        )
        assert "error" not in result or result.get("error") is None

    @pytest.mark.asyncio
    async def test_fetch_not_found(self) -> None:
        from spark_advisor_k8s_connector.handlers import _handle_fetch_k8s_logic

        mock_client = AsyncMock()
        mock_client.find_by_app_id.return_value = None

        settings = K8sConnectorSettings(namespaces=["default"], default_storage_type="hdfs")

        result = await _handle_fetch_k8s_logic(
            namespace=None, name=None, app_id="nonexistent",
            client=mock_client, broker=AsyncMock(), settings=settings,
        )
        assert result.get("error") is not None


class TestHandleListK8sApps:
    @pytest.mark.asyncio
    async def test_list_apps(self) -> None:
        from spark_advisor_k8s_connector.handlers import _handle_list_apps_logic

        mock_client = AsyncMock()
        mock_client.list_applications.return_value = [make_crd(), make_crd(name="job-2", app_id="spark-456")]

        settings = K8sConnectorSettings(
            namespaces=["spark-prod"],
            default_event_log_dir="s3a://bucket/logs/",
            default_storage_type="s3",
        )

        result = await _handle_list_apps_logic(
            client=mock_client, settings=settings,
            limit=20, offset=0, namespace=None, state=None, search=None,
        )
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_list_filtered_by_state(self) -> None:
        from spark_advisor_k8s_connector.handlers import _handle_list_apps_logic

        mock_client = AsyncMock()
        mock_client.list_applications.return_value = [
            make_crd(state="COMPLETED"),
            make_crd(name="failed-job", state="FAILED"),
        ]

        settings = K8sConnectorSettings(namespaces=["default"], default_storage_type="hdfs")

        result = await _handle_list_apps_logic(
            client=mock_client, settings=settings,
            limit=20, offset=0, namespace=None, state="COMPLETED", search=None,
        )
        assert len(result) == 1
        assert result[0].state == "COMPLETED"

    @pytest.mark.asyncio
    async def test_list_filtered_by_search(self) -> None:
        from spark_advisor_k8s_connector.handlers import _handle_list_apps_logic

        mock_client = AsyncMock()
        mock_client.list_applications.return_value = [
            make_crd(name="etl-daily"),
            make_crd(name="ml-training", app_id="spark-ml"),
        ]

        settings = K8sConnectorSettings(namespaces=["default"], default_storage_type="hdfs")

        result = await _handle_list_apps_logic(
            client=mock_client, settings=settings,
            limit=20, offset=0, namespace=None, state=None, search="etl",
        )
        assert len(result) == 1
        assert result[0].name == "etl-daily"
```

- [ ] **Step 2: Implement handlers**

```python
# packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/handlers.py
from __future__ import annotations

from typing import Any

import structlog
from faststream.context import Context
from faststream.nats import NatsBroker, NatsMessage, NatsRouter

from spark_advisor_k8s_connector.client import K8sClient
from spark_advisor_k8s_connector.config import ContextKey, K8sConnectorSettings, STORAGE_FETCH_SUBJECTS
from spark_advisor_k8s_connector.mapper import map_crd
from spark_advisor_models.defaults import NATS_FETCH_JOB_K8S_SUBJECT, NATS_K8S_APPLICATIONS_LIST_SUBJECT
from spark_advisor_models.logging import nats_handler_context
from spark_advisor_models.model import ErrorResponse, JobAnalysis
from spark_advisor_models.model.input import K8sFetchRequest, ListK8sAppsRequest, StorageFetchRequest
from spark_advisor_models.model.k8s import SparkApplicationRef
from spark_advisor_models.tracing import inject_correlation_context

logger = structlog.stdlib.get_logger(__name__)

router = NatsRouter()


@router.subscriber(NATS_FETCH_JOB_K8S_SUBJECT)
async def handle_fetch_k8s(
    data: K8sFetchRequest,
    msg: NatsMessage,
    client: K8sClient = Context(ContextKey.CLIENT),  # type: ignore[assignment]  # noqa: B008
    broker: NatsBroker = Context("broker"),  # type: ignore[assignment]
    settings: K8sConnectorSettings = Context(ContextKey.SETTINGS),  # type: ignore[assignment]  # noqa: B008
    service_name: str = Context(ContextKey.SERVICE_NAME),  # type: ignore[assignment]
) -> JobAnalysis | ErrorResponse:
    async with nats_handler_context(
        msg.headers, "k8s.fetch_job",
        {"namespace": data.namespace, "name": data.name, "app_id": data.app_id},
        service=service_name,
    ):
        return await _handle_fetch_k8s_logic(
            namespace=data.namespace, name=data.name, app_id=data.app_id,
            client=client, broker=broker, settings=settings,
        )


async def _handle_fetch_k8s_logic(
    *,
    namespace: str | None,
    name: str | None,
    app_id: str | None,
    client: K8sClient,
    broker: NatsBroker,
    settings: K8sConnectorSettings,
) -> JobAnalysis | ErrorResponse:
    try:
        if namespace and name:
            crd = await client.get_application(namespace, name)
        elif app_id:
            crd = await client.find_by_app_id(settings.namespaces, app_id)
        else:
            return ErrorResponse(error="Either (namespace + name) or app_id must be provided")

        if crd is None:
            return ErrorResponse(error=f"SparkApplication not found: {namespace}/{name} or app_id={app_id}")

        ref = map_crd(crd, default_event_log_dir=settings.default_event_log_dir, default_storage_type=settings.default_storage_type)

        if not ref.event_log_dir or not ref.storage_type or not ref.app_id:
            return ErrorResponse(error=f"Cannot resolve event log for {ref.namespace}/{ref.name}")

        subject = STORAGE_FETCH_SUBJECTS.get(ref.storage_type)
        if not subject:
            return ErrorResponse(error=f"Unknown storage type: {ref.storage_type}")

        event_log_uri = f"{ref.event_log_dir.rstrip('/')}/{ref.app_id}"
        request = StorageFetchRequest(app_id=ref.app_id, event_log_uri=event_log_uri, spark_conf=ref.spark_conf)
        headers = inject_correlation_context({})
        reply = await broker.request(request.model_dump(mode="json"), subject=subject, headers=headers, timeout=60.0)
        return JobAnalysis.model_validate_json(reply.body)

    except Exception as e:
        logger.exception("Failed to fetch K8s job")
        return ErrorResponse(error=str(e))


@router.subscriber(NATS_K8S_APPLICATIONS_LIST_SUBJECT)
async def handle_list_k8s_apps(
    data: ListK8sAppsRequest,
    msg: NatsMessage,
    client: K8sClient = Context(ContextKey.CLIENT),  # type: ignore[assignment]  # noqa: B008
    settings: K8sConnectorSettings = Context(ContextKey.SETTINGS),  # type: ignore[assignment]  # noqa: B008
    service_name: str = Context(ContextKey.SERVICE_NAME),  # type: ignore[assignment]
) -> list[dict[str, Any]]:
    async with nats_handler_context(
        msg.headers, "k8s.list_applications",
        {"limit": data.limit, "namespace": data.namespace},
        service=service_name,
    ):
        result = await _handle_list_apps_logic(
            client=client, settings=settings,
            limit=data.limit, offset=data.offset,
            namespace=data.namespace, state=data.state, search=data.search,
        )
        return [r.model_dump(mode="json") for r in result]


async def _handle_list_apps_logic(
    *,
    client: K8sClient,
    settings: K8sConnectorSettings,
    limit: int,
    offset: int,
    namespace: str | None,
    state: str | None,
    search: str | None,
) -> list[SparkApplicationRef]:
    namespaces = [namespace] if namespace else settings.namespaces
    crds = await client.list_applications(namespaces, label_selector=settings.label_selector)

    refs = [
        map_crd(crd, default_event_log_dir=settings.default_event_log_dir, default_storage_type=settings.default_storage_type)
        for crd in crds
    ]

    if state:
        refs = [r for r in refs if r.state == state]
    if search:
        search_lower = search.lower()
        refs = [r for r in refs if search_lower in r.name.lower() or (r.app_id and search_lower in r.app_id.lower())]

    return refs[offset:offset + limit]
```

- [ ] **Step 3: Run tests**

Run: `cd packages/spark-advisor-k8s-connector && uv run pytest tests/test_handlers.py -v`
Expected: All PASS

- [ ] **Step 4: Run full test suite + lint**

Run: `cd packages/spark-advisor-k8s-connector && uv run pytest -v && uv run ruff check src/ tests/ && uv run mypy src/`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/handlers.py packages/spark-advisor-k8s-connector/tests/test_handlers.py
git commit -m "feat(k8s-connector): add NATS handlers — job.fetch.k8s and k8s.applications.list"
```

---

## Task 8: FastStream app lifecycle

**Files:**
- Create: `packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/app.py`

- [ ] **Step 1: Implement app.py**

```python
# packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/app.py
import asyncio
import contextlib

import structlog
from faststream import FastStream
from faststream.nats import NatsBroker

from spark_advisor_k8s_connector.client import K8sClient
from spark_advisor_k8s_connector.config import ContextKey, K8sConnectorSettings
from spark_advisor_k8s_connector.handlers import router
from spark_advisor_k8s_connector.poller import K8sPoller
from spark_advisor_k8s_connector.store import K8sPollingStore
from spark_advisor_models.logging import configure_logging
from spark_advisor_models.tracing import configure_tracing

settings = K8sConnectorSettings()
broker = NatsBroker(settings.nats.url)
broker.include_router(router)
app = FastStream(broker)

logger = structlog.stdlib.get_logger(__name__)


@app.on_startup
async def on_startup() -> None:
    configure_logging(settings.service_name, settings.log_level, json_output=settings.json_log)
    configure_tracing(settings.service_name, settings.otel.endpoint, enabled=settings.otel.enabled)

    client = await K8sClient.create(kubeconfig_path=settings.kubeconfig_path)

    polling_store = K8sPollingStore(database_url=settings.database_url, max_size=settings.max_processed_apps)
    await polling_store.init()

    poller = K8sPoller(client=client, broker=broker, store=polling_store, settings=settings)

    app.context.set_global(ContextKey.CLIENT, client)
    app.context.set_global(ContextKey.POLLER, poller)
    app.context.set_global(ContextKey.POLLING_STORE, polling_store)
    app.context.set_global(ContextKey.SERVICE_NAME, settings.service_name)
    app.context.set_global(ContextKey.SETTINGS, settings)

    logger.info(
        "K8s Connector started: namespaces=%s poll_interval=%ds",
        settings.namespaces,
        settings.poll_interval_seconds,
    )


@app.on_shutdown
async def on_shutdown() -> None:
    polling_task: asyncio.Task[None] | None = app.context.get(ContextKey.POLLING_TASK)
    if polling_task and not polling_task.done():
        polling_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await polling_task
        logger.info("Polling task cancelled")

    polling_store: K8sPollingStore | None = app.context.get(ContextKey.POLLING_STORE)
    if polling_store is not None:
        await polling_store.close()
        logger.info("PollingStore closed")

    client: K8sClient | None = app.context.get(ContextKey.CLIENT)
    if client is not None:
        await client.close()
        logger.info("K8sClient closed")


@app.after_startup
async def start_polling() -> None:
    if not settings.polling_enabled:
        logger.info("Polling disabled (SA_K8S_CONNECTOR_POLLING_ENABLED=false)")
        return
    poller: K8sPoller = app.context.get(ContextKey.POLLER)
    polling_task = asyncio.create_task(_polling_loop(poller, settings.poll_interval_seconds))
    app.context.set_global(ContextKey.POLLING_TASK, polling_task)


async def _polling_loop(poller: K8sPoller, interval: int) -> None:
    while True:
        try:
            count = await poller.poll()
            if count > 0:
                logger.info("Published %d new K8s applications", count)
        except Exception:
            logger.exception("K8s poll cycle failed")
        await asyncio.sleep(interval)


def main() -> None:
    asyncio.run(app.run())
```

- [ ] **Step 2: Verify lint + mypy passes**

Run: `cd packages/spark-advisor-k8s-connector && uv run ruff check src/ tests/ && uv run mypy src/`
Expected: PASS

- [ ] **Step 3: Run full test suite**

Run: `cd packages/spark-advisor-k8s-connector && uv run pytest -v`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add packages/spark-advisor-k8s-connector/src/spark_advisor_k8s_connector/app.py
git commit -m "feat(k8s-connector): add FastStream app lifecycle with polling loop"
```

---

## Task 9: Gateway API reorganization — route split

**Files:**
- Modify: `packages/spark-advisor-gateway/src/spark_advisor_gateway/api/routes.py`
- Modify: `packages/spark-advisor-gateway/src/spark_advisor_gateway/config.py`
- Modify: `packages/spark-advisor-gateway/src/spark_advisor_gateway/api/schemas.py`
- Modify: `packages/spark-advisor-gateway/src/spark_advisor_gateway/task/executor.py`
- Modify: `packages/spark-advisor-gateway/src/spark_advisor_gateway/app.py`
- Modify: `packages/spark-advisor-gateway/tests/` (URL updates in all test files)

This is the largest task — it involves:
1. Adding `enabled_connectors` and K8s NATS subjects to `GatewaySettings`
2. Adding `source: DataSource` field to `ApplicationResponse`
3. Renaming `POST /api/v1/analyze` → `POST /api/v1/hs/analyze`
4. Renaming `GET /api/v1/applications` → `GET /api/v1/hs/applications`
5. Adding `POST /api/v1/k8s/analyze` and `GET /api/v1/k8s/applications`
6. Adding aggregator `GET /api/v1/applications` that queries enabled connectors
7. Adding `list_k8s_applications()` and `submit_k8s()` to TaskExecutor
8. Updating all gateway tests with new URLs

**Important references:**
- Current routes: `packages/spark-advisor-gateway/src/spark_advisor_gateway/api/routes.py`
- Current schemas: `packages/spark-advisor-gateway/src/spark_advisor_gateway/api/schemas.py`
- Current config: `packages/spark-advisor-gateway/src/spark_advisor_gateway/config.py`
- Current executor: `packages/spark-advisor-gateway/src/spark_advisor_gateway/task/executor.py`
- Current app: `packages/spark-advisor-gateway/src/spark_advisor_gateway/app.py`

- [ ] **Step 1: Update GatewaySettings — add K8s NATS subjects and enabled_connectors**

In `config.py`, add to `GatewayNatsSettings`:
```python
k8s_fetch_subject: str = NATS_FETCH_JOB_K8S_SUBJECT
k8s_list_apps_subject: str = NATS_K8S_APPLICATIONS_LIST_SUBJECT
k8s_fetch_timeout: float = 60.0
k8s_list_apps_timeout: float = 10.0
```

Add import for `NATS_FETCH_JOB_K8S_SUBJECT` and `NATS_K8S_APPLICATIONS_LIST_SUBJECT` from defaults.

Add to `GatewaySettings`:
```python
enabled_connectors: list[str] = ["hs"]
```

- [ ] **Step 2: Update ApplicationResponse — add source field**

In `schemas.py`, add `data_source` field to `ApplicationResponse`:
```python
data_source: DataSource = Field(default=DataSource.HS_MANUAL, examples=["hs_manual"])
```

Update `from_summary` classmethod to accept `data_source` parameter:
```python
@classmethod
def from_summary(cls, app: ApplicationSummary, *, analysis_count: int = 0, data_source: DataSource = DataSource.HS_MANUAL) -> ApplicationResponse:
```

Add new classmethod `from_k8s_ref`:
```python
@classmethod
def from_k8s_ref(cls, ref: SparkApplicationRef, *, analysis_count: int = 0) -> ApplicationResponse:
    return cls(
        id=ref.app_id or f"{ref.namespace}/{ref.name}",
        name=ref.name,
        start_time=ref.submitted_at.isoformat() if ref.submitted_at else "",
        end_time=ref.completed_at.isoformat() if ref.completed_at else "",
        duration_ms=int((ref.completed_at - ref.submitted_at).total_seconds() * 1000) if ref.completed_at and ref.submitted_at else 0,
        completed=ref.state == "COMPLETED",
        spark_version=ref.spark_version or "",
        user="",
        analysis_count=analysis_count,
        data_source=DataSource.K8S,
    )
```

Add new `K8sAnalyzeRequest`:
```python
class K8sAnalyzeRequest(BaseModel):
    model_config = ConfigDict(frozen=True)
    namespace: str | None = Field(default=None, examples=["spark-prod"])
    name: str | None = Field(default=None, examples=["my-etl-job"])
    app_id: str | None = Field(default=None, examples=["spark-abc123"])
    mode: AnalysisMode = Field(default=AnalysisMode.AI, examples=["ai"])
```

- [ ] **Step 3: Update TaskExecutor — add K8s methods**

Add to `executor.py`:
```python
async def list_k8s_applications(self, limit: int, offset: int = 0, namespace: str | None = None, state: str | None = None, search: str | None = None) -> list[dict[str, Any]]:
    payload = {"limit": limit, "offset": offset}
    if namespace:
        payload["namespace"] = namespace
    if state:
        payload["state"] = state
    if search:
        payload["search"] = search
    headers = inject_correlation_context({})
    reply = await self._nc.request(
        self._settings.nats.k8s_list_apps_subject,
        orjson.dumps(payload),
        timeout=self._settings.nats.k8s_list_apps_timeout,
        headers=headers,
    )
    data = orjson.loads(reply.data)
    if isinstance(data, dict) and "error" in data:
        raise HTTPException(status_code=502, detail=data["error"])
    return data
```

Add `submit_k8s` method that sends `K8sFetchRequest` to `job.fetch.k8s`:
```python
def submit_k8s(self, task_id: str, namespace: str | None, name: str | None, app_id: str | None, mode: AnalysisMode = AnalysisMode.AI) -> None:
    task = asyncio.create_task(self._execute_k8s(task_id, namespace, name, app_id, mode))
    self._background_tasks.add(task)
    task.add_done_callback(self._background_tasks.discard)

async def _execute_k8s(self, task_id: str, namespace: str | None, name: str | None, app_id: str | None, mode: AnalysisMode) -> None:
    display_id = app_id or f"{namespace}/{name}"
    tracer = get_tracer()
    attrs = {"task_id": task_id, "app_id": display_id, "mode": mode.value}
    with tracer.start_as_current_span("gateway.execute_k8s_analysis", attributes=attrs):
        trace_vars = build_trace_context_vars()
        base_headers = self._build_base_headers(trace_vars)

        structlog.contextvars.unbind_contextvars(*_TRANSIENT_CONTEXT_KEYS)
        structlog.contextvars.bind_contextvars(
            task_id=task_id, app_id=display_id, **trace_vars,
        )
        try:
            await self._tasks.mark_running(task_id)

            with tracer.start_as_current_span("gateway.nats_fetch_k8s_job"):
                payload = {}
                if namespace:
                    payload["namespace"] = namespace
                if name:
                    payload["name"] = name
                if app_id:
                    payload["app_id"] = app_id
                headers = inject_correlation_context(dict(base_headers))
                t0 = time.monotonic()
                fetch_reply = await self._nc.request(
                    self._settings.nats.k8s_fetch_subject,
                    orjson.dumps(payload),
                    timeout=self._settings.nats.k8s_fetch_timeout,
                    headers=headers,
                )
                nats_request_observe("k8s_fetch_job", time.monotonic() - t0)

            job_data = orjson.loads(fetch_reply.data)
            if "error" in job_data:
                await self._tasks.mark_failed(task_id, f"K8s fetch failed: {job_data['error']}")
                return

            await self._analyze_and_mark_completed(task_id, fetch_reply.data, mode, base_headers)

        except Exception as e:
            logger.exception("K8s task %s failed", task_id)
            await self._tasks.mark_failed(task_id, str(e))
```

- [ ] **Step 4: Update routes.py — rename HS endpoints, add K8s endpoints, add aggregator**

Move current `/api/v1/analyze` to `/api/v1/hs/analyze`.
Move current `/api/v1/applications` to `/api/v1/hs/applications`.

Add new K8s routes:
```python
@router.post("/api/v1/k8s/analyze", ...)
async def k8s_analyze(body: K8sAnalyzeRequest, ...):
    # Create task, submit_k8s, return 202

@router.get("/api/v1/k8s/applications", ...)
async def k8s_applications(executor, limit, offset, namespace, state, search):
    # Call executor.list_k8s_applications()
```

Add aggregator:
```python
@router.get("/api/v1/applications", ...)
async def list_all_applications(executor, settings, limit, offset, search):
    # Query enabled connectors concurrently with asyncio.gather
    # Map to ApplicationResponse with source field
    # Merge, sort, paginate
```

- [ ] **Step 5: Update all gateway tests with new URLs**

Replace `/api/v1/analyze` with `/api/v1/hs/analyze` and `/api/v1/applications` with `/api/v1/hs/applications` in all test files.

Add new tests for K8s routes and aggregator.

- [ ] **Step 6: Run full gateway test suite**

Run: `cd packages/spark-advisor-gateway && uv run pytest -v`
Expected: All PASS

- [ ] **Step 7: Lint**

Run: `cd packages/spark-advisor-gateway && uv run ruff check src/ tests/ && uv run mypy src/`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add packages/spark-advisor-gateway/
git commit -m "feat(gateway): reorganize API routes — /api/v1/hs/* and /api/v1/k8s/* with aggregator"
```

---

## Task 10: Dockerfile

**Files:**
- Create: `packages/spark-advisor-k8s-connector/Dockerfile`

- [ ] **Step 1: Create Dockerfile**

```dockerfile
# packages/spark-advisor-k8s-connector/Dockerfile
FROM python:3.12-slim AS base

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY packages/spark-advisor-models/ packages/spark-advisor-models/
COPY packages/spark-advisor-k8s-connector/ packages/spark-advisor-k8s-connector/

RUN uv sync --frozen --no-dev --package spark-advisor-k8s-connector

CMD ["uv", "run", "--package", "spark-advisor-k8s-connector", "spark-advisor-k8s-connector"]
```

- [ ] **Step 2: Commit**

```bash
git add packages/spark-advisor-k8s-connector/Dockerfile
git commit -m "feat(k8s-connector): add Dockerfile"
```

---

## Task 11: Helm subchart

**Files:**
- Create: `charts/k8s-connector/Chart.yaml`
- Create: `charts/k8s-connector/values.yaml`
- Create: `charts/k8s-connector/templates/_helpers.tpl`
- Create: `charts/k8s-connector/templates/deployment.yaml`
- Create: `charts/k8s-connector/templates/pvc.yaml`
- Create: `charts/k8s-connector/templates/serviceaccount.yaml`
- Create: `charts/k8s-connector/templates/clusterrole.yaml`
- Create: `charts/k8s-connector/templates/clusterrolebinding.yaml`
- Modify: `charts/spark-advisor/Chart.yaml` (add dependency)
- Modify: `charts/spark-advisor/values.yaml` (add k8s-connector section)

Use `charts/hs-connector/` as template. Key differences:
- Add RBAC templates (clusterrole, clusterrolebinding)
- Add kubeconfig Secret mount option
- Add ServiceAccount template
- K8s-specific config values

- [ ] **Step 1: Create Chart.yaml**

```yaml
# charts/k8s-connector/Chart.yaml
apiVersion: v2
name: k8s-connector
description: Kubernetes connector for spark-advisor
type: application
version: 0.1.16 # x-release-please-version
appVersion: "0.1.16" # x-release-please-version
```

- [ ] **Step 2: Create values.yaml**

Follow `charts/storage-connector/values.yaml` structure with added RBAC, kubeconfig, and K8s-specific config sections.

- [ ] **Step 3: Create all templates**

Follow `charts/hs-connector/templates/` pattern for deployment.yaml, pvc.yaml, _helpers.tpl. Add new templates for RBAC (serviceaccount.yaml, clusterrole.yaml, clusterrolebinding.yaml).

- [ ] **Step 4: Add dependency to umbrella chart**

Add to `charts/spark-advisor/Chart.yaml` dependencies:
```yaml
- name: k8s-connector
  version: 0.1.16 # x-release-please-version
  condition: k8s-connector.enabled
```

Add to `charts/spark-advisor/values.yaml`:
```yaml
k8s-connector:
  enabled: false
  # ... default values
```

- [ ] **Step 5: Run helm lint**

Run: `helm lint charts/k8s-connector/`
Expected: PASS

- [ ] **Step 6: Run helm dependency update on umbrella chart**

Run: `cd charts/spark-advisor && helm dependency update`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add charts/k8s-connector/ charts/spark-advisor/Chart.yaml charts/spark-advisor/values.yaml
git commit -m "feat(helm): add k8s-connector subchart with RBAC and kubeconfig support"
```

---

## Task 12: CI/CD updates

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/release.yml`
- Modify: `release-please-config.json`
- Modify: `Makefile`

- [ ] **Step 1: Update ci.yml**

Add `spark-advisor-k8s-connector` to the Python test matrix and `charts/k8s-connector` to the Helm lint step.

- [ ] **Step 2: Update release-please-config.json**

Add entries for:
```json
"packages/spark-advisor-k8s-connector": {
  "release-type": "python",
  "component": "spark-advisor-k8s-connector",
  "extra-files": ["pyproject.toml"]
},
"charts/k8s-connector": {
  "release-type": "helm",
  "component": "k8s-connector-chart"
}
```

- [ ] **Step 3: Update release.yml**

Add Docker build + push job for `spark-advisor-k8s-connector` following the pattern used by hs-connector and storage-connector.

- [ ] **Step 4: Update Makefile**

Add target:
```makefile
test-k8s-connector:
	cd packages/spark-advisor-k8s-connector && uv run pytest -v
```

Add to `test-all` target.

- [ ] **Step 5: Verify CI locally**

Run: `make check`
Expected: All pass (lint + mypy + all tests)

- [ ] **Step 6: Commit**

```bash
git add .github/workflows/ci.yml .github/workflows/release.yml release-please-config.json Makefile
git commit -m "ci: add spark-advisor-k8s-connector to test matrix and release config"
```

---

## Task 13: Frontend updates

**Files:**
- Modify: `packages/spark-advisor-frontend/src/lib/api.ts`
- Modify: `packages/spark-advisor-frontend/src/hooks/useAnalyze.ts`
- Modify: `packages/spark-advisor-frontend/src/hooks/useApplications.ts`
- Modify: Various component files for source badge/dropdown

- [ ] **Step 1: Update API URLs in api.ts**

Change `/api/v1/analyze` → `/api/v1/hs/analyze` in the analyze function.
Add new `analyzeK8s` function for `/api/v1/k8s/analyze`.
Change applications endpoint to use aggregator `/api/v1/applications`.

- [ ] **Step 2: Update useAnalyze hook**

Add `source` parameter (default: "hs"). Route to correct API function based on source.

- [ ] **Step 3: Add source dropdown to analysis form**

Add a select/dropdown in the analysis submission component for choosing source (History Server / Kubernetes). Conditionally show namespace/name fields for K8s.

- [ ] **Step 4: Add source badge to task/application lists**

Display `data_source` field as a badge (HS / K8s) in task and application list components.

- [ ] **Step 5: Build frontend**

Run: `cd packages/spark-advisor-frontend && npm run build`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add packages/spark-advisor-frontend/
git commit -m "feat(frontend): update API URLs and add K8s source selection"
```

---

## Task 14: Final verification and CLAUDE.md updates

- [ ] **Step 1: Run full make check**

Run: `make check`
Expected: All lint, mypy, and tests pass across ALL packages

- [ ] **Step 2: Verify Helm lint**

Run: `helm lint charts/k8s-connector/ && cd charts/spark-advisor && helm dependency update && cd ../.. && helm lint charts/spark-advisor/`
Expected: PASS

- [ ] **Step 3: Update root CLAUDE.md**

Update test count, package count (10), and add k8s-connector to current state section.

- [ ] **Step 4: Create CLAUDE.md for k8s-connector package**

```markdown
# spark-advisor-k8s-connector

K8s connector — discovers SparkApplication CRDs and delegates event log analysis to storage-connector via NATS.

## File structure
...
```

- [ ] **Step 5: Update tasks/todo.md**

Mark Phase 14 as ✅ complete.

- [ ] **Step 6: Commit**

```bash
git add CLAUDE.md packages/spark-advisor-k8s-connector/CLAUDE.md tasks/todo.md
git commit -m "docs: update CLAUDE.md and todo.md for Phase 14 completion"
```
