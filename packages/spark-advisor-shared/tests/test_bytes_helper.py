from spark_advisor_shared.util.bytes_helper import format_bytes


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
