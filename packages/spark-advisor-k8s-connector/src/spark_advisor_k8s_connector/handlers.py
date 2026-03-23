from __future__ import annotations

from typing import TYPE_CHECKING, Any

import orjson
import structlog
from faststream.context import Context
from faststream.nats import NatsBroker, NatsMessage, NatsRouter

from spark_advisor_k8s_connector.config import ContextKey, K8sConnectorSettings
from spark_advisor_k8s_connector.mapper import map_crd
from spark_advisor_models.defaults import (
    NATS_FETCH_JOB_K8S_SUBJECT,
    NATS_K8S_APPLICATIONS_LIST_SUBJECT,
    STORAGE_FETCH_SUBJECTS,
)
from spark_advisor_models.logging import nats_handler_context
from spark_advisor_models.model import ErrorResponse, JobAnalysis
from spark_advisor_models.model.input import K8sFetchRequest, ListK8sAppsRequest, StorageFetchRequest
from spark_advisor_models.tracing import inject_correlation_context

if TYPE_CHECKING:
    from spark_advisor_k8s_connector.client import K8sClient
    from spark_advisor_models.model.k8s import SparkApplicationRef

logger = structlog.stdlib.get_logger(__name__)

router = NatsRouter()


@router.subscriber(NATS_FETCH_JOB_K8S_SUBJECT)
async def handle_fetch_k8s(
    data: K8sFetchRequest,
    msg: NatsMessage,
    client: K8sClient = Context(ContextKey.CLIENT),  # type: ignore[assignment]  # noqa: B008
    broker: NatsBroker = Context("broker"),  # type: ignore[assignment]  # noqa: B008
    settings: K8sConnectorSettings = Context(ContextKey.SETTINGS),  # type: ignore[assignment]  # noqa: B008
    service_name: str = Context(ContextKey.SERVICE_NAME),  # type: ignore[assignment]
) -> JobAnalysis | ErrorResponse:
    async with nats_handler_context(
        msg.headers, "k8s.fetch_job",
        {"namespace": data.namespace, "name": data.name, "app_id": data.app_id},
        service=service_name,
    ):
        return await _handle_fetch_k8s_logic(
            namespace=data.namespace, name=data.name, app_id=data.app_id,
            client=client, broker=broker, settings=settings,
        )


async def _handle_fetch_k8s_logic(
    *,
    namespace: str | None,
    name: str | None,
    app_id: str | None,
    client: K8sClient,
    broker: NatsBroker,
    settings: K8sConnectorSettings,
) -> JobAnalysis | ErrorResponse:
    try:
        crd: dict[str, Any] | None = None
        if namespace and name:
            crd = await client.get_application(namespace, name)
        elif app_id:
            crd = await client.find_by_app_id(settings.namespaces, app_id)
        else:
            return ErrorResponse(error="Either (namespace + name) or app_id must be provided")

        if crd is None:
            return ErrorResponse(error=f"SparkApplication not found: {namespace}/{name} or app_id={app_id}")

        ref = map_crd(
            crd,
            default_event_log_dir=settings.default_event_log_dir,
            default_storage_type=settings.default_storage_type,
        )

        if not ref.event_log_dir or not ref.storage_type or not ref.app_id:
            return ErrorResponse(error=f"Cannot resolve event log for {ref.namespace}/{ref.name}")

        subject = STORAGE_FETCH_SUBJECTS.get(ref.storage_type)
        if not subject:
            return ErrorResponse(error=f"Unknown storage type: {ref.storage_type}")

        event_log_uri = f"{ref.event_log_dir.rstrip('/')}/{ref.app_id}"
        request = StorageFetchRequest(app_id=ref.app_id, event_log_uri=event_log_uri, spark_conf=ref.spark_conf)
        headers = inject_correlation_context({})
        reply = await broker.request(
            request.model_dump(mode="json"), subject=subject, headers=headers, timeout=60.0,
        )
        response_data = orjson.loads(reply.body)
        if isinstance(response_data, dict) and "error" in response_data:
            return ErrorResponse(error=response_data["error"])
        return JobAnalysis.model_validate(response_data)

    except Exception as e:
        logger.exception("Failed to fetch K8s job")
        return ErrorResponse(error=str(e))


@router.subscriber(NATS_K8S_APPLICATIONS_LIST_SUBJECT)
async def handle_list_k8s_apps(
    data: ListK8sAppsRequest,
    msg: NatsMessage,
    client: K8sClient = Context(ContextKey.CLIENT),  # type: ignore[assignment]  # noqa: B008
    settings: K8sConnectorSettings = Context(ContextKey.SETTINGS),  # type: ignore[assignment]  # noqa: B008
    service_name: str = Context(ContextKey.SERVICE_NAME),  # type: ignore[assignment]
) -> list[dict[str, Any]]:
    async with nats_handler_context(
        msg.headers, "k8s.list_applications",
        {"limit": data.limit, "namespace": data.namespace},
        service=service_name,
    ):
        result = await _handle_list_apps_logic(
            client=client, settings=settings,
            limit=data.limit, offset=data.offset,
            namespace=data.namespace, state=data.state, search=data.search,
        )
        return [r.model_dump(mode="json") for r in result]


async def _handle_list_apps_logic(
    *,
    client: K8sClient,
    settings: K8sConnectorSettings,
    limit: int,
    offset: int,
    namespace: str | None,
    state: str | None,
    search: str | None,
) -> list[SparkApplicationRef]:
    namespaces = [namespace] if namespace else settings.namespaces
    crds = await client.list_applications(namespaces, label_selector=settings.label_selector)

    if state:
        crds = [c for c in crds if c.get("status", {}).get("applicationState", {}).get("state") == state]
    if search:
        search_lower = search.lower()
        crds = [
            c for c in crds
            if search_lower in c.get("metadata", {}).get("name", "").lower()
            or search_lower in c.get("status", {}).get("sparkApplicationId", "").lower()
        ]

    page = crds[offset:offset + limit]
    return [
        map_crd(
            crd,
            default_event_log_dir=settings.default_event_log_dir,
            default_storage_type=settings.default_storage_type,
        )
        for crd in page
    ]
