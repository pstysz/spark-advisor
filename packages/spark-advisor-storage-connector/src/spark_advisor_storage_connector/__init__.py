"""Storage connector for reading Spark event logs from HDFS, S3, and GCS."""

from spark_advisor_storage_connector.connectors.gcs import GcsConnector
from spark_advisor_storage_connector.connectors.hdfs import HdfsConnector
from spark_advisor_storage_connector.connectors.s3 import S3Connector
from spark_advisor_storage_connector.event_log_builder import fetch_and_parse_event_log
from spark_advisor_storage_connector.poller import StoragePoller
from spark_advisor_storage_connector.store import PollingStore

__all__ = [
    "GcsConnector",
    "HdfsConnector",
    "PollingStore",
    "S3Connector",
    "StoragePoller",
    "fetch_and_parse_event_log",
]
