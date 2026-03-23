from __future__ import annotations

import asyncio
from typing import Any

import structlog
from kubernetes_asyncio import config as k8s_config
from kubernetes_asyncio.client import ApiClient, CustomObjectsApi

logger = structlog.stdlib.get_logger(__name__)

CRD_GROUP = "sparkoperator.k8s.io"
CRD_VERSION = "v1beta2"
CRD_PLURAL = "sparkapplications"


class K8sClient:
    def __init__(self, api_client: ApiClient) -> None:
        self._api_client = api_client
        self._api = CustomObjectsApi(api_client)

    @classmethod
    async def create(cls, kubeconfig_path: str | None = None) -> K8sClient:
        try:
            k8s_config.load_incluster_config()  # type: ignore[no-untyped-call]
            logger.info("Using in-cluster K8s config")
        except k8s_config.ConfigException:
            await k8s_config.load_kube_config(config_file=kubeconfig_path)
            logger.info("Using kubeconfig: %s", kubeconfig_path or "default")
        api_client = ApiClient()
        return cls(api_client)

    async def list_applications(
        self,
        namespaces: list[str],
        *,
        label_selector: str | None = None,
    ) -> list[dict[str, Any]]:
        if len(namespaces) == 1:
            response = await self._list_namespace(namespaces[0], label_selector)
            items: list[dict[str, Any]] = response.get("items", [])
            return items
        responses = await asyncio.gather(
            *(self._list_namespace(ns, label_selector) for ns in namespaces)
        )
        return [item for resp in responses for item in resp.get("items", [])]

    async def _list_namespace(self, namespace: str, label_selector: str | None) -> dict[str, Any]:
        return await self._api.list_namespaced_custom_object(  # type: ignore[no-any-return]
            CRD_GROUP, CRD_VERSION, namespace, CRD_PLURAL,
            label_selector=label_selector,  # type: ignore[arg-type]
        )

    async def get_application(self, namespace: str, name: str) -> dict[str, Any]:
        return await self._api.get_namespaced_custom_object(  # type: ignore[no-any-return]
            CRD_GROUP, CRD_VERSION, namespace, CRD_PLURAL, name,
        )

    async def find_by_app_id(self, namespaces: list[str], app_id: str) -> dict[str, Any] | None:
        all_crds = await self.list_applications(namespaces)
        for crd in all_crds:
            if crd.get("status", {}).get("sparkApplicationId") == app_id:
                return crd
        return None

    async def close(self) -> None:
        await self._api_client.close()
