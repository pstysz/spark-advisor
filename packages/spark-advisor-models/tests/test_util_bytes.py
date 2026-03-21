from spark_advisor_models.util.bytes import parse_memory_string


class TestParseMemoryString:
    def test_gigabytes_suffix(self) -> None:
        assert parse_memory_string("1g") == 1024

    def test_megabytes_suffix(self) -> None:
        assert parse_memory_string("512m") == 512

    def test_kilobytes_suffix(self) -> None:
        assert parse_memory_string("1024k") == 1

    def test_sub_kb_rounds_up_to_1(self) -> None:
        assert parse_memory_string("512k") == 1

    def test_terabytes_suffix(self) -> None:
        assert parse_memory_string("1t") == 1024 * 1024

    def test_uppercase_suffix(self) -> None:
        assert parse_memory_string("2G") == 2048

    def test_bare_number_treated_as_mb(self) -> None:
        assert parse_memory_string("2048") == 2048

    def test_empty_string_returns_zero(self) -> None:
        assert parse_memory_string("") == 0

    def test_invalid_string_returns_zero(self) -> None:
        assert parse_memory_string("abc") == 0
