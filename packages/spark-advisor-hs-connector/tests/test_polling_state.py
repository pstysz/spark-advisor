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

    def test_eviction_removes_oldest(self) -> None:
        state = PollingState(max_size=3)
        state.mark_processed("app-001")
        state.mark_processed("app-002")
        state.mark_processed("app-003")
        state.mark_processed("app-004")
        assert state.processed_count == 3
        assert not state.is_processed("app-001")
        assert state.is_processed("app-002")
        assert state.is_processed("app-003")
        assert state.is_processed("app-004")

    def test_eviction_fifo_order(self) -> None:
        state = PollingState(max_size=2)
        state.mark_processed("a")
        state.mark_processed("b")
        state.mark_processed("c")
        state.mark_processed("d")
        assert not state.is_processed("a")
        assert not state.is_processed("b")
        assert state.is_processed("c")
        assert state.is_processed("d")

    def test_filter_new_and_mark_returns_new_only(self) -> None:
        state = PollingState()
        state.mark_processed("app-001")
        new = state.filter_new_and_mark(["app-001", "app-002", "app-003"])
        assert new == ["app-002", "app-003"]

    def test_filter_new_and_mark_marks_atomically(self) -> None:
        state = PollingState()
        state.filter_new_and_mark(["app-001", "app-002"])
        assert state.is_processed("app-001")
        assert state.is_processed("app-002")
        assert state.processed_count == 2

    def test_filter_new_and_mark_empty_input(self) -> None:
        state = PollingState()
        assert state.filter_new_and_mark([]) == []

    def test_filter_new_and_mark_all_known(self) -> None:
        state = PollingState()
        state.mark_processed("app-001")
        assert state.filter_new_and_mark(["app-001"]) == []

    def test_filter_new_and_mark_respects_eviction(self) -> None:
        state = PollingState(max_size=2)
        state.mark_processed("app-001")
        state.filter_new_and_mark(["app-002", "app-003"])
        assert not state.is_processed("app-001")
        assert state.is_processed("app-002")
        assert state.is_processed("app-003")

    def test_remove_existing(self) -> None:
        state = PollingState()
        state.mark_processed("app-001")
        state.remove("app-001")
        assert not state.is_processed("app-001")
        assert state.processed_count == 0

    def test_remove_nonexistent_is_noop(self) -> None:
        state = PollingState()
        state.remove("nonexistent")
        assert state.processed_count == 0
