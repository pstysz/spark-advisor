class PollingState:
    """Tracks which app IDs have already been processed.
    ToDo: In-memory set for now. Interface is ready for Redis/DB backend swap.
    """

    def __init__(self) -> None:
        self._processed: set[str] = set()

    def is_processed(self, app_id: str) -> bool:
        return app_id in self._processed

    def mark_processed(self, app_id: str) -> None:
        self._processed.add(app_id)

    def filter_new(self, app_ids: list[str]) -> list[str]:
        return [app_id for app_id in app_ids if app_id not in self._processed]

    @property
    def processed_count(self) -> int:
        return len(self._processed)
