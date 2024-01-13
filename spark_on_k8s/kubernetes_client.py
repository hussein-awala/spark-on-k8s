from __future__ import annotations

from contextlib import contextmanager

from kubernetes import client as k8s, config


class KubernetesClientManager:
    """Kubernetes client manager."""

    def __init__(
        self,
        config_file: str | None = None,
        context: str | None = None,
        client_configuration: k8s.Configuration | None = None,
    ) -> None:
        """Create a Kubernetes client manager."""
        self.config_file = config_file
        self.context = context
        self.client_configuration = client_configuration

    @contextmanager
    def client(self) -> k8s.ApiClient:
        """Create a Kubernetes client."""
        _client = None
        try:
            _client = self.create_client()
            yield _client
        finally:
            if _client:
                _client.close()

    def create_client(self) -> k8s.ApiClient:
        """Get a Kubernetes client."""
        config.load_kube_config(
            config_file=self.config_file,
            context=self.context,
            client_configuration=self.client_configuration,
        )
        return k8s.ApiClient()
