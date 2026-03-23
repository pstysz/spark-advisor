from __future__ import annotations

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
