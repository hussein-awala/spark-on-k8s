from __future__ import annotations

import json
from os import getenv

from kubernetes_asyncio import client as k8s


class APIConfiguration:
    """API configuration."""

    # K8S client configuration
    K8S_CONFIG_FILE = getenv("K8S_CONFIG_FILE", None)
    K8S_CONTEXT = getenv("K8S_CONTEXT", None)
    K8S_CLIENT_CONFIG = (
        k8s.Configuration(json.loads(getenv("K8S_CLIENT_CONFIG")))
        if getenv("K8S_CLIENT_CONFIG", None)
        else None,
    )
    K8S_IN_CLUSTER = bool(getenv("K8S_IN_CLUSTER", False))

    # API configuration
    K8S_DEFAULT_NAMESPACE = getenv("K8S_DEFAULT_NAMESPACE", "default")
