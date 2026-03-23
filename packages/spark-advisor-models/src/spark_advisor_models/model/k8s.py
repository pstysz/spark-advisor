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
