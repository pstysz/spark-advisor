from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from spark_advisor_models.tracing import get_tracer, inject_correlation_context
from spark_advisor_storage_connector.event_log_builder import fetch_and_parse_event_log

if TYPE_CHECKING:
    from faststream.nats import NatsBroker

    from spark_advisor_storage_connector.connectors.protocol import EventLogRef, StorageConnector
    from spark_advisor_storage_connector.store import PollingStore

logger = structlog.stdlib.get_logger(__name__)


class StoragePoller:
    def __init__(
            self,
            connector: StorageConnector,
            broker: NatsBroker,
            publish_subject: str,
            store: PollingStore,
    ) -> None:
        self._connector = connector
        self._broker = broker
        self._publish_subject = publish_subject
        self.store = store

    async def poll(self) -> int:
        tracer = get_tracer()
        with tracer.start_as_current_span("storage.poll"):
            refs = await self._connector.list_event_logs()
            all_paths = [ref.path for ref in refs]
            new_paths = await self.store.filter_new_and_mark(all_paths)

            if not new_paths:
                logger.debug("No new event logs found")
                return 0

            path_to_ref = {ref.path: ref for ref in refs}
            published = 0
            for path in new_paths:
                ref = path_to_ref[path]
                try:
                    await self._fetch_and_publish(ref)
                    published += 1
                except Exception:
                    logger.exception("Failed to fetch/publish event log %s, will retry next cycle", ref.path)
                    await self.store.remove(path)

            logger.info("Poll complete: %d/%d new event logs published", published, len(new_paths))
            return published

    async def _fetch_and_publish(self, ref: EventLogRef) -> None:
        tracer = get_tracer()
        with tracer.start_as_current_span("storage.fetch_and_publish", attributes={"path": ref.path}):
            job = await fetch_and_parse_event_log(self._connector, ref)
            headers = inject_correlation_context({})
            await self._broker.publish(
                job.model_dump(mode="json"), subject=self._publish_subject, headers=headers,
            )
