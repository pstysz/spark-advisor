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
