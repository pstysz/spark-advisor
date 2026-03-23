from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from spark_advisor_k8s_connector.client import CRD_GROUP, CRD_PLURAL, CRD_VERSION, K8sClient
from tests.conftest import make_crd


@pytest.fixture
def mock_api() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def client(mock_api: AsyncMock) -> K8sClient:
    c = K8sClient.__new__(K8sClient)
    c._api = mock_api
    c._api_client = MagicMock()
    return c


class TestListApplications:
    @pytest.mark.asyncio
    async def test_lists_from_single_namespace(self, client: K8sClient, mock_api: AsyncMock) -> None:
        mock_api.list_namespaced_custom_object.return_value = {"items": [make_crd()]}
        result = await client.list_applications(namespaces=["spark-prod"])
        assert len(result) == 1
        mock_api.list_namespaced_custom_object.assert_called_once_with(
            CRD_GROUP, CRD_VERSION, "spark-prod", CRD_PLURAL,
            label_selector=None,
        )

    @pytest.mark.asyncio
    async def test_lists_from_multiple_namespaces(self, client: K8sClient, mock_api: AsyncMock) -> None:
        mock_api.list_namespaced_custom_object.side_effect = [
            {"items": [make_crd(namespace="ns1")]},
            {"items": [make_crd(namespace="ns2")]},
        ]
        result = await client.list_applications(namespaces=["ns1", "ns2"])
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_with_label_selector(self, client: K8sClient, mock_api: AsyncMock) -> None:
        mock_api.list_namespaced_custom_object.return_value = {"items": []}
        await client.list_applications(namespaces=["default"], label_selector="team=data")
        mock_api.list_namespaced_custom_object.assert_called_once_with(
            CRD_GROUP, CRD_VERSION, "default", CRD_PLURAL,
            label_selector="team=data",
        )

    @pytest.mark.asyncio
    async def test_empty_result(self, client: K8sClient, mock_api: AsyncMock) -> None:
        mock_api.list_namespaced_custom_object.return_value = {"items": []}
        result = await client.list_applications(namespaces=["default"])
        assert result == []


class TestGetApplication:
    @pytest.mark.asyncio
    async def test_get_by_name(self, client: K8sClient, mock_api: AsyncMock) -> None:
        mock_api.get_namespaced_custom_object.return_value = make_crd()
        result = await client.get_application("spark-prod", "my-spark-job")
        assert result is not None
        mock_api.get_namespaced_custom_object.assert_called_once_with(
            CRD_GROUP, CRD_VERSION, "spark-prod", CRD_PLURAL, "my-spark-job",
        )


class TestFindByAppId:
    @pytest.mark.asyncio
    async def test_find_by_app_id(self, client: K8sClient, mock_api: AsyncMock) -> None:
        crds = [make_crd(app_id="spark-abc123"), make_crd(name="other", app_id="spark-other")]
        mock_api.list_namespaced_custom_object.return_value = {"items": crds}
        result = await client.find_by_app_id(["default"], "spark-abc123")
        assert result is not None
        assert result["status"]["sparkApplicationId"] == "spark-abc123"

    @pytest.mark.asyncio
    async def test_find_by_app_id_not_found(self, client: K8sClient, mock_api: AsyncMock) -> None:
        mock_api.list_namespaced_custom_object.return_value = {"items": []}
        result = await client.find_by_app_id(["default"], "nonexistent")
        assert result is None


class TestClose:
    @pytest.mark.asyncio
    async def test_close(self, client: K8sClient) -> None:
        client._api_client.close = AsyncMock()
        await client.close()
        client._api_client.close.assert_called_once()
