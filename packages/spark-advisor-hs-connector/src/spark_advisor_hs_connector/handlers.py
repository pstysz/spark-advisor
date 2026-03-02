import asyncio
from typing import Any

from faststream.context import Context
from faststream.nats import NatsRouter

from spark_advisor_hs_connector.history_server_client import HistoryServerClient
from spark_advisor_hs_connector.hs_fetcher import fetch_job_analysis

router = NatsRouter()


@router.subscriber("fetch.job")
async def handle_fetch_job(
    data: dict[str, str],
    hs_client: HistoryServerClient = Context("hs_client"),  # type: ignore[assignment]  # noqa: B008
) -> dict[str, Any]:
    job = await asyncio.to_thread(fetch_job_analysis, hs_client, data["app_id"])
    return job.model_dump(mode="json")
