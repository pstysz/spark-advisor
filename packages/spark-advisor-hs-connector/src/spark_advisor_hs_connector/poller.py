from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from spark_advisor_hs_connector.history_server_mapper import map_job_analysis

if TYPE_CHECKING:
    from faststream.nats import NatsBroker

    from spark_advisor_hs_connector.history_server_client import HistoryServerClient
    from spark_advisor_hs_connector.polling_state import PollingState

logger = logging.getLogger(__name__)


class HistoryServerPoller:
    def __init__(
        self,
        hs_client: HistoryServerClient,
        broker: NatsBroker,
        publish_subject: str,
        polling_state: PollingState,
        batch_size: int = 50,
    ) -> None:
        self._hs_client = hs_client
        self._broker = broker
        self._publish_subject = publish_subject
        self._polling_state = polling_state
        self._batch_size = batch_size

    async def poll(self) -> int:
        apps = self._hs_client.list_applications(limit=self._batch_size)
        all_ids = [app.id for app in apps]
        new_ids = self._polling_state.filter_new(all_ids)

        if not new_ids:
            logger.debug("No new applications found")
            return 0

        published = 0
        for app_id in new_ids:
            try:
                await self._fetch_and_publish(app_id)
                self._polling_state.mark_processed(app_id)
                published += 1
            except Exception:
                logger.exception("Failed to fetch/publish app %s, will retry next cycle", app_id)

        logger.info("Poll complete: %d/%d new apps published", published, len(new_ids))
        return published

    async def _fetch_and_publish(self, app_id: str) -> None:
        app_info = self._hs_client.get_app_info(app_id)
        base_path = self._resolve_base_path(app_id, app_info)

        environment = self._hs_client.get_environment(base_path)
        raw_stages = self._hs_client.get_stages(base_path)
        stages_data = self._deduplicate_stages(raw_stages)
        task_summaries = self._fetch_task_summaries(base_path, stages_data)
        executors_data = self._hs_client.get_executors(base_path)

        job = map_job_analysis(
            app_id=app_id,
            app_info=app_info,
            environment=environment,
            stages_data=stages_data,
            task_summaries=task_summaries,
            executors_data=executors_data,
        )

        await self._broker.publish(job.model_dump(mode="json"), subject=self._publish_subject)

    def _fetch_task_summaries(
        self, base_path: str, stages_data: list[dict[str, Any]],
    ) -> dict[int, dict[str, Any]]:
        summaries: dict[int, dict[str, Any]] = {}
        for stage in stages_data:
            stage_id = int(stage["stageId"])
            attempt_id = int(stage.get("attemptId", 0))
            summaries[stage_id] = self._hs_client.get_task_summary(base_path, stage_id, attempt_id)
        return summaries

    @staticmethod
    def _deduplicate_stages(stages_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        latest: dict[int, dict[str, Any]] = {}
        for stage in stages_data:
            sid = int(stage["stageId"])
            aid = int(stage.get("attemptId", 0))
            if sid not in latest or aid > int(latest[sid].get("attemptId", 0)):
                latest[sid] = stage
        return list(latest.values())

    @staticmethod
    def _resolve_base_path(app_id: str, app_info: dict[str, Any]) -> str:
        attempts = app_info.get("attempts", [])
        if attempts:
            attempt_id = attempts[-1].get("attemptId")
            if attempt_id:
                return f"/applications/{app_id}/{attempt_id}"
        return f"/applications/{app_id}"
