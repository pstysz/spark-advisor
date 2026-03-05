import asyncio
from collections import OrderedDict
from itertools import islice
from typing import Protocol

from spark_advisor_gateway.task.models import AnalysisTask


class TaskStore(Protocol):
    async def create(self, task: AnalysisTask) -> None: ...
    async def get(self, task_id: str) -> AnalysisTask | None: ...
    async def update(self, task: AnalysisTask) -> None: ...
    async def list_recent(self, limit: int) -> list[AnalysisTask]: ...


class InMemoryTaskStore:
    def __init__(self, max_size: int = 1000) -> None:
        self._tasks: OrderedDict[str, AnalysisTask] = OrderedDict()
        self._max_size = max_size
        self._lock = asyncio.Lock()

    async def create(self, task: AnalysisTask) -> None:
        async with self._lock:
            if len(self._tasks) >= self._max_size:
                self._tasks.popitem(last=False)
            self._tasks[task.task_id] = task

    async def get(self, task_id: str) -> AnalysisTask | None:
        async with self._lock:
            return self._tasks.get(task_id)

    async def update(self, task: AnalysisTask) -> None:
        async with self._lock:
            self._tasks[task.task_id] = task

    async def list_recent(self, limit: int) -> list[AnalysisTask]:
        async with self._lock:
            return list(islice(reversed(self._tasks.values()), limit))
