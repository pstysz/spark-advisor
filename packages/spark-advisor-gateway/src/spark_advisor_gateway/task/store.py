from collections import OrderedDict
from typing import Protocol

from spark_advisor_gateway.task.models import AnalysisTask


class TaskStore(Protocol):
    def create(self, task: AnalysisTask) -> None: ...
    def get(self, task_id: str) -> AnalysisTask | None: ...
    def update(self, task: AnalysisTask) -> None: ...
    def list_recent(self, limit: int) -> list[AnalysisTask]: ...


class InMemoryTaskStore:
    def __init__(self, max_size: int = 1000) -> None:
        self._tasks: OrderedDict[str, AnalysisTask] = OrderedDict()
        self._max_size = max_size

    def create(self, task: AnalysisTask) -> None:
        if len(self._tasks) >= self._max_size:
            self._tasks.popitem(last=False)
        self._tasks[task.task_id] = task

    def get(self, task_id: str) -> AnalysisTask | None:
        return self._tasks.get(task_id)

    def update(self, task: AnalysisTask) -> None:
        self._tasks[task.task_id] = task

    def list_recent(self, limit: int) -> list[AnalysisTask]:
        return list(reversed(list(self._tasks.values())))[:limit]
