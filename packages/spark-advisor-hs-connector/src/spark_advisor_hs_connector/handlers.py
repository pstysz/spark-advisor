import asyncio

import structlog
from faststream.context import Context
from faststream.nats import NatsMessage, NatsRouter
from pydantic import BaseModel, Field

from spark_advisor_hs_connector.config import ContextKey
from spark_advisor_hs_connector.history_server.client import HistoryServerClient
from spark_advisor_hs_connector.job_analysis_builder import fetch_job_analysis
from spark_advisor_models.defaults import NATS_APPLICATIONS_LIST_SUBJECT, NATS_FETCH_JOB_SUBJECT
from spark_advisor_models.logging import bind_nats_context
from spark_advisor_models.model import ApplicationSummary, JobAnalysis

router = NatsRouter()
logger = structlog.stdlib.get_logger(__name__)


class ErrorResponse(BaseModel, frozen=True):
    error: str


class FetchJobRequest(BaseModel, frozen=True):
    app_id: str


class ListAppsRequest(BaseModel, frozen=True):
    limit: int = Field(default=20, ge=1, le=500)


@router.subscriber(NATS_FETCH_JOB_SUBJECT)
async def handle_fetch_job(
    data: FetchJobRequest,
    msg: NatsMessage,
    hs_client: HistoryServerClient = Context(ContextKey.HS_CLIENT),  # type: ignore[assignment]  # noqa: B008
) -> JobAnalysis | ErrorResponse:
    bind_nats_context(msg.headers, app_id=data.app_id, service="hs-connector")
    try:
        return await asyncio.to_thread(fetch_job_analysis, hs_client, data.app_id)
    except Exception as e:
        logger.exception("Failed to fetch job %s", data.app_id)
        return ErrorResponse(error=str(e))


@router.subscriber(NATS_APPLICATIONS_LIST_SUBJECT)
async def handle_list_applications(
    data: ListAppsRequest,
    msg: NatsMessage,
    hs_client: HistoryServerClient = Context(ContextKey.HS_CLIENT),  # type: ignore[assignment]  # noqa: B008
) -> list[ApplicationSummary] | ErrorResponse:
    bind_nats_context(msg.headers, service="hs-connector")
    try:
        return await asyncio.to_thread(hs_client.list_applications, limit=data.limit)
    except Exception as e:
        logger.exception("Failed to list applications")
        return ErrorResponse(error=str(e))
