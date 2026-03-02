from spark_advisor_hs_connector.polling_state import PollingState


class TestPollingState:
    def test_new_app_not_processed(self) -> None:
        state = PollingState()
        assert not state.is_processed("app-001")

    def test_mark_processed(self) -> None:
        state = PollingState()
        state.mark_processed("app-001")
        assert state.is_processed("app-001")

    def test_filter_new(self) -> None:
        state = PollingState()
        state.mark_processed("app-001")
        state.mark_processed("app-003")
        result = state.filter_new(["app-001", "app-002", "app-003", "app-004"])
        assert result == ["app-002", "app-004"]

    def test_processed_count(self) -> None:
        state = PollingState()
        assert state.processed_count == 0
        state.mark_processed("app-001")
        state.mark_processed("app-002")
        assert state.processed_count == 2

    def test_idempotent_mark(self) -> None:
        state = PollingState()
        state.mark_processed("app-001")
        state.mark_processed("app-001")
        assert state.processed_count == 1
