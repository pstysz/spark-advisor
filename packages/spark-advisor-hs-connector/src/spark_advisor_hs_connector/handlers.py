import asyncio
import logging
from typing import Any

from faststream.context import Context
from faststream.nats import NatsRouter

from spark_advisor_hs_connector.history_server_client import HistoryServerClient
from spark_advisor_hs_connector.hs_fetcher import fetch_job_analysis

router = NatsRouter()
logger = logging.getLogger(__name__)


@router.subscriber("fetch.job")
async def handle_fetch_job(
    data: dict[str, str],
    hs_client: HistoryServerClient = Context("hs_client"),  # type: ignore[assignment]  # noqa: B008
) -> dict[str, Any]:
    try:
        job = await asyncio.to_thread(fetch_job_analysis, hs_client, data["app_id"])
        return job.model_dump(mode="json")
    except Exception as e:
        logger.exception("Failed to fetch job %s", data.get("app_id"))
        return {"error": str(e)}


@router.subscriber("list.applications")
async def handle_list_applications(
    data: dict[str, Any],
    hs_client: HistoryServerClient = Context("hs_client"),  # type: ignore[assignment]  # noqa: B008
) -> list[dict[str, Any]] | dict[str, Any]:
    try:
        limit = data.get("limit", 20)
        apps = await asyncio.to_thread(hs_client.list_applications, limit=limit)
        return [app.model_dump(mode="json") for app in apps]
    except Exception as e:
        logger.exception("Failed to list applications")
        return {"error": str(e)}
