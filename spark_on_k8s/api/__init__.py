from __future__ import annotations

from typing import TYPE_CHECKING

from httpx import AsyncClient

from spark_on_k8s.api.config import APIConfiguration
from spark_on_k8s.k8s.async_client import KubernetesAsyncClientManager

if TYPE_CHECKING:
    from kubernetes_asyncio.client import ApiClient


class KubernetesClientSingleton:
    """Kubernetes client singleton."""

    _client: ApiClient | None = None

    @classmethod
    async def client(cls) -> ApiClient:
        if not cls._client:
            cls._client = await KubernetesAsyncClientManager(
                config_file=APIConfiguration.K8S_CONFIG_FILE,
                context=APIConfiguration.K8S_CONTEXT,
                client_configuration=APIConfiguration.K8S_CLIENT_CONFIG,
                in_cluster=APIConfiguration.K8S_IN_CLUSTER,
            ).create_client()
        return cls._client


class AsyncHttpClientSingleton:
    """Async HTTP client singleton."""

    _client: AsyncClient | None = None

    @classmethod
    def client(cls) -> AsyncClient:
        if not cls._client:
            cls._client = AsyncClient()
        return cls._client
