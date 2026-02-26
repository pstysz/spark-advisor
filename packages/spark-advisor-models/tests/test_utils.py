import pytest

from spark_advisor_models.util import format_bytes, median_value, percentile_value


class TestFormatBytes:
    def test_bytes(self) -> None:
        assert format_bytes(0) == "0.0 B"
        assert format_bytes(512) == "512.0 B"

    def test_kilobytes_fractional(self) -> None:
        assert format_bytes(1536) == "1.5 KB"

    def test_megabytes(self) -> None:
        assert format_bytes(1_048_576) == "1.0 MB"

    def test_gigabytes_fractional(self) -> None:
        assert format_bytes(1_610_612_736) == "1.5 GB"

    def test_terabytes(self) -> None:
        assert format_bytes(1_099_511_627_776) == "1.0 TB"

    def test_accepts_float(self) -> None:
        assert format_bytes(1536.0) == "1.5 KB"


class TestPercentileValue:
    def test_supported_quantiles(self) -> None:
        values = [10, 20, 30, 40, 50]
        assert percentile_value(values, 0.0) == 10.0
        assert percentile_value(values, 0.25) == 20.0
        assert percentile_value(values, 0.5) == 30.0
        assert percentile_value(values, 0.75) == 40.0
        assert percentile_value(values, 1.0) == 50.0

    def test_unsupported_quantile_raises(self) -> None:
        with pytest.raises(ValueError, match=r"Unsupported quantile: 0\.95"):
            percentile_value([10, 20, 30, 40, 50], 0.95)

    def test_empty_values(self) -> None:
        assert percentile_value([], 0.5) == 0.0

    def test_median_value(self) -> None:
        assert median_value([100, 200, 300, 400, 500]) == 300.0
