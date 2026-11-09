from __future__ import annotations

import pytest
from spark_on_k8s.api import namespace_scope
from spark_on_k8s.api.configuration import APIConfiguration


@pytest.fixture(autouse=True)
def unrestricted_namespace_scope(monkeypatch):
    """Route tests target arbitrary namespaces; scope tests narrow this explicitly."""
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_NAMESPACES", "*")
    # Never resolve the developer's kubeconfig or a mounted service account in unit tests.
    monkeypatch.setattr(namespace_scope, "current_namespace", lambda: "default")
