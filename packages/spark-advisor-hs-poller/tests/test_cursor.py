from spark_advisor_hs_poller.pooling_state import PollingState


class TestPollingCursor:
    def test_new_app_not_processed(self) -> None:
        cursor = PollingState()
        assert not cursor.is_processed("app-001")

    def test_mark_processed(self) -> None:
        cursor = PollingState()
        cursor.mark_processed("app-001")
        assert cursor.is_processed("app-001")

    def test_filter_new(self) -> None:
        cursor = PollingState()
        cursor.mark_processed("app-001")
        cursor.mark_processed("app-003")
        result = cursor.filter_new(["app-001", "app-002", "app-003", "app-004"])
        assert result == ["app-002", "app-004"]

    def test_processed_count(self) -> None:
        cursor = PollingState()
        assert cursor.processed_count == 0
        cursor.mark_processed("app-001")
        cursor.mark_processed("app-002")
        assert cursor.processed_count == 2

    def test_idempotent_mark(self) -> None:
        cursor = PollingState()
        cursor.mark_processed("app-001")
        cursor.mark_processed("app-001")
        assert cursor.processed_count == 1
