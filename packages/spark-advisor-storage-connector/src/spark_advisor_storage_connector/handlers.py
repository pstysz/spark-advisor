from datetime import UTC, datetime

import structlog
from faststream.context import Context
from faststream.nats import NatsMessage, NatsRouter

from spark_advisor_models.logging import nats_handler_context
from spark_advisor_models.model import ErrorResponse, JobAnalysis
from spark_advisor_models.model.input import StorageFetchRequest
from spark_advisor_storage_connector.config import ContextKey
from spark_advisor_storage_connector.connectors.protocol import EventLogRef, StorageConnector
from spark_advisor_storage_connector.event_log_builder import fetch_and_parse_event_log

logger = structlog.stdlib.get_logger(__name__)


def create_router(subject: str, connector_type: str) -> NatsRouter:
    router = NatsRouter()
    span_name = f"storage.fetch.{connector_type}"

    @router.subscriber(subject)
    async def handle_storage_fetch(
            data: StorageFetchRequest,
            msg: NatsMessage,
            connector: StorageConnector = Context(ContextKey.CONNECTOR),  # type: ignore[assignment]  # noqa: B008
            service_name: str = Context(ContextKey.SERVICE_NAME),  # type: ignore[assignment]
    ) -> JobAnalysis | ErrorResponse:
        async with nats_handler_context(
                msg.headers, span_name,
                {"app_id": data.app_id, "uri": data.event_log_uri},
                app_id=data.app_id, service=service_name,
        ):
            try:
                ref = _resolve_ref_from_uri(data.event_log_uri)
                return await fetch_and_parse_event_log(connector, ref)
            except Exception as e:
                logger.exception("Failed to fetch event log for %s from %s", data.app_id, data.event_log_uri)
                return ErrorResponse(error=str(e))

    return router


def _resolve_ref_from_uri(uri: str) -> EventLogRef:
    name = uri.rsplit("/", 1)[-1] if "/" in uri else uri
    return EventLogRef(
        path=uri,
        name=name,
        size=0,
        modified_at=datetime.now(UTC),
    )
