from __future__ import annotations

import runpy

import pytest
from spark_on_k8s.utils import configuration


@pytest.mark.parametrize(
    "value, expected",
    [(None, False), ("", False), ("false", False), ("FALSE", False), ("true", True), ("True", True)],
)
def test_in_cluster_environment_boolean(monkeypatch, value, expected):
    if value is None:
        monkeypatch.delenv("SPARK_ON_K8S_IN_CLUSTER", raising=False)
    else:
        monkeypatch.setenv("SPARK_ON_K8S_IN_CLUSTER", value)

    settings = runpy.run_path(configuration.__file__)["Configuration"]

    assert settings.SPARK_ON_K8S_IN_CLUSTER is expected
