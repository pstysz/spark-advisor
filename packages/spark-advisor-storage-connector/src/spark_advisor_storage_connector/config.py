from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, ConfigDict, model_validator
from pydantic_settings import SettingsConfigDict

from spark_advisor_models.defaults import (
    DEFAULT_EVENT_LOGS_DIR,
    NATS_STORAGE_FETCH_GCS_SUBJECT,
    NATS_STORAGE_FETCH_HDFS_SUBJECT,
    NATS_STORAGE_FETCH_S3_SUBJECT,
)
from spark_advisor_models.settings import BaseConnectorNatsSettings, BaseConnectorSettings


class ConnectorType(StrEnum):
    HDFS = "hdfs"
    S3 = "s3"
    GCS = "gcs"


CONNECTOR_FETCH_SUBJECTS: dict[ConnectorType, str] = {
    ConnectorType.HDFS: NATS_STORAGE_FETCH_HDFS_SUBJECT,
    ConnectorType.S3: NATS_STORAGE_FETCH_S3_SUBJECT,
    ConnectorType.GCS: NATS_STORAGE_FETCH_GCS_SUBJECT,
}


class ContextKey(StrEnum):
    POLLER = "poller"
    CONNECTOR = "connector"
    POLLING_TASK = "polling_task"
    POLLING_STORE = "polling_store"
    SERVICE_NAME = "service_name"


class StorageNatsSettings(BaseConnectorNatsSettings):
    pass


class HdfsSettings(BaseModel):
    model_config = ConfigDict(frozen=True)
    namenode_url: str = "http://localhost:9870"
    event_log_dir: str = DEFAULT_EVENT_LOGS_DIR


class S3Settings(BaseModel):
    model_config = ConfigDict(frozen=True)
    bucket: str = ""
    prefix: str = DEFAULT_EVENT_LOGS_DIR
    region: str = "us-east-1"
    endpoint_url: str | None = None


class GcsSettings(BaseModel):
    model_config = ConfigDict(frozen=True)
    bucket: str = ""
    prefix: str = DEFAULT_EVENT_LOGS_DIR
    project_id: str | None = None


class StorageConnectorSettings(BaseConnectorSettings):
    model_config = SettingsConfigDict(
        env_prefix="SA_STORAGE_",
        yaml_file="/etc/spark-advisor/storage-connector/config.yaml",
    )

    service_name: str = "spark-advisor-storage-connector"
    nats: StorageNatsSettings = StorageNatsSettings()
    connector_type: ConnectorType = ConnectorType.HDFS
    hdfs: HdfsSettings = HdfsSettings()
    s3: S3Settings = S3Settings()
    gcs: GcsSettings = GcsSettings()
    database_url: str = ""

    @model_validator(mode="after")
    def _set_database_url(self) -> StorageConnectorSettings:
        if not self.database_url:
            object.__setattr__(self, "database_url", f"sqlite+aiosqlite:///data/storage_{self.connector_type}_connector.db")
        return self
