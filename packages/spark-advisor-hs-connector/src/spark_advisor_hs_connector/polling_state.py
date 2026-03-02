import threading


class PollingState:
    """Tracks which app IDs have already been processed."""

    def __init__(self) -> None:
        self._processed: set[str] = set()
        self._lock = threading.Lock()

    def is_processed(self, app_id: str) -> bool:
        return app_id in self._processed

    def mark_processed(self, app_id: str) -> None:
        with self._lock:
            self._processed.add(app_id)

    def filter_new(self, app_ids: list[str]) -> list[str]:
        with self._lock:
            return [aid for aid in app_ids if aid not in self._processed]

    @property
    def processed_count(self) -> int:
        return len(self._processed)
