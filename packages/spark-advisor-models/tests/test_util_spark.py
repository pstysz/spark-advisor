from spark_advisor_models.util.spark import parse_spark_version


class TestParseSparkVersion:
    def test_standard_version(self) -> None:
        assert parse_spark_version("3.2.1") == (3, 2)

    def test_major_minor_only(self) -> None:
        assert parse_spark_version("3.4") == (3, 4)

    def test_vendor_suffix(self) -> None:
        assert parse_spark_version("3.5.0-amzn-0") == (3, 5)

    def test_spark2(self) -> None:
        assert parse_spark_version("2.4.8") == (2, 4)

    def test_empty_string(self) -> None:
        assert parse_spark_version("") is None

    def test_unparseable(self) -> None:
        assert parse_spark_version("custom-build") is None

    def test_single_number(self) -> None:
        assert parse_spark_version("3") is None
