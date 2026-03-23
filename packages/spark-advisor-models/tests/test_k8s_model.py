import pytest
from pydantic import ValidationError

from spark_advisor_models.model.input import K8sFetchRequest, ListK8sAppsRequest
from spark_advisor_models.model.k8s import SparkApplicationRef


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
