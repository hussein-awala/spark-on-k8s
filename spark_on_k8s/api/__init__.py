from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from httpx import AsyncClient

from spark_on_k8s.k8s.async_client import KubernetesAsyncClientManager
from spark_on_k8s.utils.configuration import Configuration

if TYPE_CHECKING:
    from kubernetes_asyncio.client import ApiClient

logger = logging.getLogger("spark_on_k8s.api")


class KubernetesClientSingleton:
    """Kubernetes client singleton."""

    client_manager: KubernetesAsyncClientManager = KubernetesAsyncClientManager(
        config_file=Configuration.SPARK_ON_K8S_CONFIG_FILE,
        context=Configuration.SPARK_ON_K8S_CONTEXT,
        client_configuration=Configuration.SPARK_ON_K8S_ASYNC_CLIENT_CONFIG,
        in_cluster=Configuration.SPARK_ON_K8S_IN_CLUSTER,
    )
    _client: ApiClient | None = None

    @classmethod
    async def client(cls) -> ApiClient:
        if not cls._client:
            cls._client = await cls.client_manager.create_client()
        return cls._client


class AsyncHttpClientSingleton:
    """Async HTTP client singleton."""

    _client: AsyncClient | None = None

    @classmethod
    def client(cls) -> AsyncClient:
        if not cls._client:
            cls._client = AsyncClient()
        return cls._client
