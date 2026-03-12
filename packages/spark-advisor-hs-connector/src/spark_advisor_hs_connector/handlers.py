import asyncio
import logging
from typing import Any

from faststream.context import Context
from faststream.nats import NatsRouter
from pydantic import BaseModel

from spark_advisor_hs_connector.config import ContextKey
from spark_advisor_hs_connector.history_server.client import HistoryServerClient
from spark_advisor_hs_connector.job_analysis_builder import fetch_job_analysis
from spark_advisor_models.defaults import NATS_FETCH_JOB_SUBJECT, NATS_LIST_APPLICATIONS_SUBJECT
from spark_advisor_models.model import ApplicationSummary, JobAnalysis

router = NatsRouter()
logger = logging.getLogger(__name__)


class ErrorResponse(BaseModel):
    error: str


@router.subscriber(NATS_FETCH_JOB_SUBJECT)
async def handle_fetch_job(
    data: dict[str, str],
    hs_client: HistoryServerClient = Context(ContextKey.HS_CLIENT),  # type: ignore[assignment]  # noqa: B008
) -> JobAnalysis | ErrorResponse:
    try:
        return await asyncio.to_thread(fetch_job_analysis, hs_client, data["app_id"])
    except Exception as e:
        logger.exception("Failed to fetch job %s", data.get("app_id"))
        return ErrorResponse(error=str(e))


@router.subscriber(NATS_LIST_APPLICATIONS_SUBJECT)
async def handle_list_applications(
    data: dict[str, Any],
    hs_client: HistoryServerClient = Context(ContextKey.HS_CLIENT),  # type: ignore[assignment]  # noqa: B008
) -> list[ApplicationSummary] | ErrorResponse:
    try:
        limit = data.get("limit", 20)
        return await asyncio.to_thread(hs_client.list_applications, limit=limit)
    except Exception as e:
        logger.exception("Failed to list applications")
        return ErrorResponse(error=str(e))
