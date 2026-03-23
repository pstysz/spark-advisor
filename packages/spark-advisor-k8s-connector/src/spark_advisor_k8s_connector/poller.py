from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import structlog

from spark_advisor_k8s_connector.mapper import map_crd
from spark_advisor_models.defaults import STORAGE_FETCH_SUBJECTS
from spark_advisor_models.model.input import StorageFetchRequest
from spark_advisor_models.tracing import get_tracer, inject_correlation_context

if TYPE_CHECKING:
    from faststream.nats import NatsBroker

    from spark_advisor_k8s_connector.client import K8sClient
    from spark_advisor_k8s_connector.config import K8sConnectorSettings
    from spark_advisor_k8s_connector.store import K8sPollingStore

logger = structlog.stdlib.get_logger(__name__)


class K8sPoller:
    def __init__(
        self,
        client: K8sClient,
        broker: NatsBroker,
        store: K8sPollingStore,
        settings: K8sConnectorSettings,
    ) -> None:
        self._client = client
        self._broker = broker
        self._store = store
        self._settings = settings

    async def poll(self) -> int:
        tracer = get_tracer()
        with tracer.start_as_current_span("k8s.poll_cycle"):
            crds = await self._client.list_applications(
                self._settings.namespaces,
                label_selector=self._settings.label_selector,
            )
            cutoff = datetime.now(UTC) - timedelta(days=self._settings.max_age_days)
            allowed_states = set(self._settings.application_states)
            published = 0
            for crd in crds:
                raw_state = crd.get("status", {}).get("applicationState", {}).get("state")
                if raw_state not in allowed_states:
                    continue

                ref = map_crd(
                    crd,
                    default_event_log_dir=self._settings.default_event_log_dir,
                    default_storage_type=self._settings.default_storage_type,
                )

                if ref.completed_at and ref.completed_at < cutoff:
                    continue
                if not ref.event_log_dir or not ref.storage_type:
                    logger.warning("No event log dir for %s/%s, skipping", ref.namespace, ref.name)
                    continue
                if not ref.app_id:
                    logger.warning("No app_id for %s/%s, skipping", ref.namespace, ref.name)
                    continue

                completed_at_str = ref.completed_at.isoformat() if ref.completed_at else ""
                if await self._store.is_processed(ref.namespace, ref.name, completed_at_str):
                    continue

                await self._store.mark_processed(ref.namespace, ref.name, completed_at_str)
                try:
                    await self._delegate_to_storage(ref.app_id, ref.event_log_dir, ref.storage_type, ref.spark_conf)
                    published += 1
                except Exception:
                    logger.exception("Failed to delegate %s/%s to storage-connector", ref.namespace, ref.name)
                    await self._store.remove(ref.namespace, ref.name, completed_at_str)

            if published:
                logger.info("Poll complete: %d new applications delegated", published)
            return published

    async def _delegate_to_storage(
        self,
        app_id: str,
        event_log_dir: str,
        storage_type: str,
        spark_conf: dict[str, str],
    ) -> None:
        tracer = get_tracer()
        event_log_uri = f"{event_log_dir.rstrip('/')}/{app_id}"
        subject = STORAGE_FETCH_SUBJECTS.get(storage_type)
        if not subject:
            raise ValueError(f"Unknown storage type: {storage_type}")

        with tracer.start_as_current_span("k8s.delegate_to_storage", attributes={"app_id": app_id, "subject": subject}):
            request = StorageFetchRequest(app_id=app_id, event_log_uri=event_log_uri, spark_conf=spark_conf)
            headers = inject_correlation_context({})
            reply = await self._broker.request(
                request.model_dump(mode="json"),
                subject=subject,
                headers=headers,
                timeout=60.0,
            )
            headers_out = inject_correlation_context({})
            await self._broker.publish(
                reply.body,
                subject=self._settings.nats.analysis_submit_subject,
                headers=headers_out,
            )
