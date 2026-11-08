from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from spark_on_k8s.utils.app_manager import SparkAppManager
from spark_on_k8s.utils.async_app_manager import AsyncSparkAppManager
from spark_on_k8s.utils.run import RunHandle


@pytest.mark.parametrize("uid", [None, "", 12])
def test_manager_cancellation_requires_saved_uid_before_cluster_access(uid):
    connection = MagicMock()
    manager = SparkAppManager(k8s_client_manager=connection)
    run = RunHandle(namespace="spark", app_id="job", driver_pod="job-driver", driver_uid=uid)
    with pytest.raises(ValueError, match="driver UID"):
        manager.cancel_run(run)
    connection.client.assert_not_called()


def test_async_delete_rejects_ambiguous_driver_selection_without_mutation():
    connection = MagicMock()
    api = AsyncMock()
    api.list_namespaced_pod.return_value = SimpleNamespace(
        items=[SimpleNamespace(metadata=SimpleNamespace(name=name)) for name in ("driver-a", "driver-b")]
    )
    manager = AsyncSparkAppManager(k8s_client_manager=connection)
    with patch("spark_on_k8s.utils.async_app_manager.k8s_async.CoreV1Api", return_value=api):
        with pytest.raises(ValueError, match="Multiple driver pods"):
            asyncio.run(manager.delete_app(namespace="spark", app_id="job"))
    api.list_namespaced_pod.assert_awaited_once_with(
        namespace="spark", label_selector="spark-app-id=job,spark-role=driver"
    )
    api.delete_namespaced_pod.assert_not_awaited()
