from collections import OrderedDict
from threading import Lock


class PollingState:
    """Tracks which app IDs have already been processed with bounded memory."""

    def __init__(self, max_size: int = 10_000) -> None:
        self._processed: OrderedDict[str, None] = OrderedDict()
        self._max_size = max_size
        self._lock = Lock()

    def is_processed(self, app_id: str) -> bool:
        with self._lock:
            return app_id in self._processed

    def mark_processed(self, app_id: str) -> None:
        with self._lock:
            self._processed[app_id] = None
            if len(self._processed) > self._max_size:
                self._processed.popitem(last=False)

    def filter_new(self, app_ids: list[str]) -> list[str]:
        with self._lock:
            return [aid for aid in app_ids if aid not in self._processed]

    @property
    def processed_count(self) -> int:
        with self._lock:
            return len(self._processed)
