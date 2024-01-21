from __future__ import annotations

from typing import TYPE_CHECKING

from httpx import AsyncClient

from spark_on_k8s.api.kubernetes_async_client import KubernetesAsyncClientManager

if TYPE_CHECKING:
    from kubernetes.client import ApiClient


class KubernetesClientSingleton:
    """Kubernetes client singleton."""

    _client: ApiClient | None = None

    @classmethod
    async def client(cls) -> ApiClient:
        if not cls._client:
            cls._client = await KubernetesAsyncClientManager().create_client()
        return cls._client


class AsyncHttpClientSingleton:
    """Async HTTP client singleton."""

    _client: AsyncClient | None = None

    @classmethod
    def client(cls) -> AsyncClient:
        if not cls._client:
            cls._client = AsyncClient()
        return cls._client
