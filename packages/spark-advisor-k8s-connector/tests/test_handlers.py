from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from spark_advisor_k8s_connector.config import K8sConnectorSettings
from spark_advisor_models.model import ErrorResponse
from tests.conftest import make_crd

MOCK_JOB_ANALYSIS_JSON = json.dumps({
    "app_id": "spark-abc123",
    "app_name": "test-app",
    "spark_version": "3.5.1",
    "duration_ms": 60000,
    "config": {"raw": {}},
    "stages": [],
}).encode()


class TestHandleFetchK8s:
    @pytest.mark.asyncio
    async def test_fetch_by_namespace_and_name(self) -> None:
        from spark_advisor_k8s_connector.handlers import _handle_fetch_k8s_logic

        mock_client = AsyncMock()
        mock_client.get_application.return_value = make_crd()

        mock_broker = AsyncMock()
        mock_broker.request.return_value = AsyncMock(body=MOCK_JOB_ANALYSIS_JSON)

        settings = K8sConnectorSettings(default_event_log_dir="s3a://bucket/logs/", default_storage_type="s3")

        result = await _handle_fetch_k8s_logic(
            namespace="spark-prod",
            name="my-spark-job",
            app_id=None,
            client=mock_client,
            broker=mock_broker,
            settings=settings,
        )
        assert not isinstance(result, ErrorResponse)
        mock_client.get_application.assert_called_once_with("spark-prod", "my-spark-job")

    @pytest.mark.asyncio
    async def test_fetch_by_app_id(self) -> None:
        from spark_advisor_k8s_connector.handlers import _handle_fetch_k8s_logic

        mock_client = AsyncMock()
        mock_client.find_by_app_id.return_value = make_crd()

        mock_broker = AsyncMock()
        mock_broker.request.return_value = AsyncMock(body=MOCK_JOB_ANALYSIS_JSON)

        settings = K8sConnectorSettings(
            namespaces=["spark-prod"],
            default_event_log_dir="s3a://bucket/logs/",
            default_storage_type="s3",
        )

        result = await _handle_fetch_k8s_logic(
            namespace=None,
            name=None,
            app_id="spark-abc123",
            client=mock_client,
            broker=mock_broker,
            settings=settings,
        )
        assert not isinstance(result, ErrorResponse)

    @pytest.mark.asyncio
    async def test_fetch_not_found(self) -> None:
        from spark_advisor_k8s_connector.handlers import _handle_fetch_k8s_logic

        mock_client = AsyncMock()
        mock_client.find_by_app_id.return_value = None

        settings = K8sConnectorSettings(namespaces=["default"], default_storage_type="hdfs")

        result = await _handle_fetch_k8s_logic(
            namespace=None, name=None, app_id="nonexistent",
            client=mock_client, broker=AsyncMock(), settings=settings,
        )
        assert isinstance(result, ErrorResponse)
        assert result.error is not None


    @pytest.mark.asyncio
    async def test_fetch_returns_error_when_storage_connector_fails(self) -> None:
        from spark_advisor_k8s_connector.handlers import _handle_fetch_k8s_logic

        mock_client = AsyncMock()
        mock_client.get_application.return_value = make_crd()

        error_body = json.dumps({"error": "Event log not found"}).encode()
        mock_broker = AsyncMock()
        mock_broker.request.return_value = AsyncMock(body=error_body)

        settings = K8sConnectorSettings(default_event_log_dir="s3a://bucket/logs/", default_storage_type="s3")

        result = await _handle_fetch_k8s_logic(
            namespace="spark-prod", name="my-spark-job", app_id=None,
            client=mock_client, broker=mock_broker, settings=settings,
        )
        assert isinstance(result, ErrorResponse)
        assert "Event log not found" in result.error


class TestHandleListK8sApps:
    @pytest.mark.asyncio
    async def test_list_apps(self) -> None:
        from spark_advisor_k8s_connector.handlers import _handle_list_apps_logic

        mock_client = AsyncMock()
        mock_client.list_applications.return_value = [make_crd(), make_crd(name="job-2", app_id="spark-456")]

        settings = K8sConnectorSettings(
            namespaces=["spark-prod"],
            default_event_log_dir="s3a://bucket/logs/",
            default_storage_type="s3",
        )

        result = await _handle_list_apps_logic(
            client=mock_client, settings=settings,
            limit=20, offset=0, namespace=None, state=None, search=None,
        )
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_list_filtered_by_state(self) -> None:
        from spark_advisor_k8s_connector.handlers import _handle_list_apps_logic

        mock_client = AsyncMock()
        mock_client.list_applications.return_value = [
            make_crd(state="COMPLETED"),
            make_crd(name="failed-job", state="FAILED"),
        ]

        settings = K8sConnectorSettings(namespaces=["default"], default_storage_type="hdfs")

        result = await _handle_list_apps_logic(
            client=mock_client, settings=settings,
            limit=20, offset=0, namespace=None, state="COMPLETED", search=None,
        )
        assert len(result) == 1
        assert result[0].state == "COMPLETED"

    @pytest.mark.asyncio
    async def test_list_filtered_by_search(self) -> None:
        from spark_advisor_k8s_connector.handlers import _handle_list_apps_logic

        mock_client = AsyncMock()
        mock_client.list_applications.return_value = [
            make_crd(name="etl-daily"),
            make_crd(name="ml-training", app_id="spark-ml"),
        ]

        settings = K8sConnectorSettings(namespaces=["default"], default_storage_type="hdfs")

        result = await _handle_list_apps_logic(
            client=mock_client, settings=settings,
            limit=20, offset=0, namespace=None, state=None, search="etl",
        )
        assert len(result) == 1
        assert result[0].name == "etl-daily"
