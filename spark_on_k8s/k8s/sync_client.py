from __future__ import annotations

from contextlib import contextmanager

from kubernetes import client as k8s, config

from spark_on_k8s.utils.configuration import Configuration
from spark_on_k8s.utils.types import NOTSET, ArgNotSet


class KubernetesClientManager:
    """Kubernetes client manager.

    Args:
        config_file (str, optional): Path to the Kubernetes configuration file. Defaults to None.
        context (str, optional): Kubernetes context. Defaults to None.
        client_configuration (k8s.Configuration, optional): Kubernetes client configuration.
            Defaults to None.
    """

    def __init__(
        self,
        config_file: str | ArgNotSet = NOTSET,
        context: str | ArgNotSet = NOTSET,
        client_configuration: k8s.Configuration | ArgNotSet = NOTSET,
        in_cluster: bool | ArgNotSet = NOTSET,
    ) -> None:
        self.config_file = (
            config_file if config_file is not NOTSET else Configuration.SPARK_ON_K8S_CONFIG_FILE
        )
        self.context = context if context is not NOTSET else Configuration.SPARK_ON_K8S_CONTEXT
        self.client_configuration = (
            client_configuration
            if client_configuration is not NOTSET
            else Configuration.SPARK_ON_K8S_CLIENT_CONFIG
        )
        self.in_cluster = in_cluster if in_cluster is not NOTSET else Configuration.SPARK_ON_K8S_IN_CLUSTER

    @contextmanager
    def client(self) -> k8s.ApiClient:
        """Create a Kubernetes client in a context manager.

        Examples:
            >>> from spark_on_k8s.k8s.sync_client import KubernetesClientManager
            >>> with KubernetesClientManager().client() as client:
            ...     api = k8s.CoreV1Api(client)
            ...     namespaces = [ns.metadata.name for ns in api.list_namespace().items]
            ...     print(namespaces)
            ['default', 'kube-node-lease', 'kube-public', 'kube-system', 'spark']

        Yields:
            k8s.ApiClient: Kubernetes client.
        """
        _client = None
        try:
            _client = self.create_client()
            yield _client
        finally:
            if _client:
                _client.close()

    def create_client(self) -> k8s.ApiClient:
        """Load the Kubernetes configuration and create a Kubernetes client.

        This method could be overridden to create a Kubernetes client in a different way without
        overriding the client method.

        Returns:
            k8s.ApiClient: Kubernetes client.
        """
        if not self.in_cluster:
            config.load_kube_config(
                config_file=self.config_file,
                context=self.context,
                client_configuration=self.client_configuration,
            )
        else:
            config.load_incluster_config()
        return k8s.ApiClient()
