from __future__ import annotations

from enum import StrEnum

from pydantic import Field
from pydantic_settings import SettingsConfigDict

from spark_advisor_models.defaults import (
    NATS_FETCH_JOB_K8S_SUBJECT,
    NATS_K8S_APPLICATIONS_LIST_SUBJECT,
)
from spark_advisor_models.settings import BaseConnectorNatsSettings, BaseConnectorSettings


class ContextKey(StrEnum):
    CLIENT = "k8s_client"
    POLLER = "poller"
    POLLING_TASK = "polling_task"
    POLLING_STORE = "polling_store"
    SERVICE_NAME = "service_name"
    SETTINGS = "settings"


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

    namespaces: list[str] = Field(default_factory=lambda: ["default"])
    label_selector: str | None = None
    application_states: list[str] = Field(default_factory=lambda: ["COMPLETED", "FAILED"])
    max_age_days: int = 7
    kubeconfig_path: str | None = None

    default_event_log_dir: str | None = None
    default_storage_type: str = "hdfs"

    database_url: str = "sqlite+aiosqlite:///data/k8s_connector.db"
