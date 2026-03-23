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
