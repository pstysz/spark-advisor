from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from spark_advisor_hs_connector.hs_fetcher import fetch_job_analysis

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
        job = fetch_job_analysis(self._hs_client, app_id)
        await self._broker.publish(job.model_dump(mode="json"), subject=self._publish_subject)
