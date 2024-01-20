from __future__ import annotations

from contextlib import asynccontextmanager

from kubernetes_asyncio import client as k8s, config


class KubernetesAsyncClientManager:
    """Kubernetes async client manager.

    Args:
        config_file (str, optional): Path to the Kubernetes configuration file. Defaults to None.
        context (str, optional): Kubernetes context. Defaults to None.
        client_configuration (k8s.Configuration, optional): Kubernetes client configuration.
            Defaults to None.
    """

    def __init__(
        self,
        config_file: str | None = None,
        context: str | None = None,
        client_configuration: k8s.Configuration | None = None,
    ) -> None:
        self.config_file = config_file
        self.context = context
        self.client_configuration = client_configuration

    @asynccontextmanager
    async def client(self) -> k8s.ApiClient:
        """Create a Kubernetes client in a context manager.

        Examples:
            >>> import asyncio
            >>> from spark_on_k8s.kubernetes_async_client import KubernetesAsyncClientManager
            >>> async def get_namespaces():
            >>>     async with KubernetesAsyncClientManager().client() as async_client:
            ...         api = k8s.CoreV1Api(async_client)
            ...         namespaces = [ns.metadata.name for ns in await api.list_namespace().items]
            ...         print(namespaces)
            >>> asyncio.run(get_namespaces())
            ['default', 'kube-node-lease', 'kube-public', 'kube-system', 'spark']

        Yields:
            k8s.ApiClient: Kubernetes client.
        """
        async_client = await self.create_client()
        try:
            yield async_client
        finally:
            await async_client.close()

    async def create_client(self) -> k8s.ApiClient:
        """Load the Kubernetes configuration and create a Kubernetes client.

        This method could be overridden to create a Kubernetes client in a different way without
        overriding the client method.

        Returns:
            k8s.ApiClient: Kubernetes client.
        """
        await config.load_kube_config(
            config_file=self.config_file,
            context=self.context,
            client_configuration=self.client_configuration,
        )
        return k8s.ApiClient()
