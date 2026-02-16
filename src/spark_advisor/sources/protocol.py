from typing import Protocol

from spark_advisor.core import JobAnalysis


class DataSource(Protocol):
    def fetch(self, app_id: str) -> JobAnalysis: ...

    def list_applications(self, limit: int = 20) -> list[dict[str, str]]: ...
