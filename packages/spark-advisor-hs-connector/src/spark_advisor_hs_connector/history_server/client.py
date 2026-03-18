from typing import Any

import httpx
import structlog
from pydantic import TypeAdapter

from spark_advisor_models.model import ApplicationSummary

logger = structlog.stdlib.get_logger(__name__)

_APP_LIST_ADAPTER = TypeAdapter(list[ApplicationSummary])


class HistoryServerClient:
    def __init__(self, base_url: str, timeout: float = 30.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._client: httpx.Client | None = None

    def open(self) -> "HistoryServerClient":
        if self._client is None:
            self._client = httpx.Client(
                base_url=f"{self._base_url}/api/v1",
                timeout=self._timeout,
                headers={"Accept": "application/json"},
                transport=httpx.HTTPTransport(retries=2),
            )
        return self

    def __enter__(self) -> "HistoryServerClient":
        return self.open()

    def __exit__(self, *_: object) -> None:
        self.close()

    def _get_client(self) -> httpx.Client:
        if self._client is None:
            raise RuntimeError("Client not initialized — call open() or use as context manager")
        return self._client

    def list_applications(self, limit: int = 20) -> list[ApplicationSummary]:
        response = self._get_client().get("/applications", params={"limit": limit})
        response.raise_for_status()
        return _APP_LIST_ADAPTER.validate_json(response.content)

    def get_app_info(self, app_id: str) -> dict[str, Any]:
        response = self._get_client().get(f"/applications/{app_id}")
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]

    def get_environment(self, base_path: str) -> dict[str, Any]:
        response = self._get_client().get(f"{base_path}/environment")
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]

    def get_stages(self, base_path: str) -> list[dict[str, Any]]:
        response = self._get_client().get(
            f"{base_path}/stages",
            params={"status": "complete"},
        )
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]

    def get_task_summary(self, base_path: str, stage_id: int, stage_attempt_id: int) -> dict[str, Any]:
        response = self._get_client().get(
            f"{base_path}/stages/{stage_id}/{stage_attempt_id}/taskSummary",
            params={"quantiles": "0.0,0.25,0.5,0.75,1.0"},
        )
        if response.status_code == 404:
            logger.debug("Task summary not available for stage %d/%d (404)", stage_id, stage_attempt_id)
            return {}
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]

    def get_executors(self, base_path: str) -> list[dict[str, Any]]:
        response = self._get_client().get(f"{base_path}/executors")
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
