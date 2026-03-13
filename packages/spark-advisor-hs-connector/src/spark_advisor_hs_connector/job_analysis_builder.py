from concurrent.futures import ThreadPoolExecutor
from typing import Any

from spark_advisor_hs_connector.history_server.client import HistoryServerClient
from spark_advisor_hs_connector.history_server.mapper import map_job_analysis
from spark_advisor_models.model import JobAnalysis


def fetch_job_analysis(hs_client: HistoryServerClient, app_id: str) -> JobAnalysis:
    app_info = hs_client.get_app_info(app_id)
    base_path = resolve_base_path(app_id, app_info)

    environment = hs_client.get_environment(base_path)
    raw_stages = hs_client.get_stages(base_path)
    stages_data = deduplicate_stages(raw_stages)
    task_summaries = fetch_task_summaries(hs_client, base_path, stages_data)
    executors_data = hs_client.get_executors(base_path)

    return map_job_analysis(
        app_id=app_id,
        app_info=app_info,
        environment=environment,
        stages_data=stages_data,
        task_summaries=task_summaries,
        executors_data=executors_data,
    )


def resolve_base_path(app_id: str, app_info: dict[str, Any]) -> str:
    attempts = app_info.get("attempts", [])
    if attempts:
        attempt_id = attempts[-1].get("attemptId")
        if attempt_id:
            return f"/applications/{app_id}/{attempt_id}"
    return f"/applications/{app_id}"


def deduplicate_stages(stages_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest: dict[int, dict[str, Any]] = {}
    for stage in stages_data:
        sid = int(stage["stageId"])
        aid = int(stage.get("attemptId", 0))
        if sid not in latest or aid > int(latest[sid].get("attemptId", 0)):
            latest[sid] = stage
    return list(latest.values())


def fetch_task_summaries(
    hs_client: HistoryServerClient,
    base_path: str,
    stages_data: list[dict[str, Any]],
) -> dict[int, dict[str, Any]]:
    if not stages_data:
        return {}

    def _fetch_one(stage: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        stage_id = int(stage["stageId"])
        attempt_id = int(stage.get("attemptId", 0))
        return stage_id, hs_client.get_task_summary(base_path, stage_id, attempt_id)

    max_workers = min(8, len(stages_data))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(_fetch_one, stages_data)
        return dict(results)
