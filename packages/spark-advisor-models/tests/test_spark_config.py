from spark_advisor_models.model.spark_config import SparkConfig


class TestSparkConfigDriverProperties:
    def test_driver_memory_default(self) -> None:
        config = SparkConfig(raw={})
        assert config.driver_memory == "1g"

    def test_driver_memory_explicit(self) -> None:
        config = SparkConfig(raw={"spark.driver.memory": "4g"})
        assert config.driver_memory == "4g"

    def test_driver_memory_overhead_default(self) -> None:
        config = SparkConfig(raw={})
        assert config.driver_memory_overhead == ""

    def test_driver_memory_overhead_explicit(self) -> None:
        config = SparkConfig(raw={"spark.driver.memoryOverhead": "512m"})
        assert config.driver_memory_overhead == "512m"

    def test_has_aqe_config_key_present(self) -> None:
        config = SparkConfig(raw={"spark.sql.adaptive.enabled": "true"})
        assert config.has_explicit_aqe_config is True

    def test_has_aqe_config_key_absent(self) -> None:
        config = SparkConfig(raw={})
        assert config.has_explicit_aqe_config is False
