from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from spark_on_k8s.utils import app_manager, async_app_manager
from spark_on_k8s.utils.spark_app_status import SparkAppStatus


@pytest.fixture(params=["sync", "async"])
def status_manager(request, monkeypatch):
    asynchronous = request.param == "async"
    module = async_app_manager if asynchronous else app_manager
    client_module = module.k8s_async if asynchronous else module.k8s
    manager_type = module.AsyncSparkAppManager if asynchronous else module.SparkAppManager
    api = MagicMock()
    if asynchronous:
        api.list_namespaced_pod = AsyncMock()
        api.read_namespaced_pod = AsyncMock()
    monkeypatch.setattr(client_module, "CoreV1Api", MagicMock(return_value=api))
    manager = manager_type(k8s_client_manager=MagicMock())

    def call(method, **kwargs):
        result = getattr(manager, method)(namespace="spark", **kwargs)
        return asyncio.run(result) if asynchronous else result

    return SimpleNamespace(api=api, call=call, exception=module.ApiException)


@pytest.mark.parametrize("use_supplied_client", [False, True])
def test_missing_app_id_raises_not_found(status_manager, use_supplied_client):
    status_manager.api.list_namespaced_pod.return_value = SimpleNamespace(items=[])
    kwargs = {"client": status_manager.api} if use_supplied_client else {}

    with pytest.raises(status_manager.exception) as raised:
        status_manager.call("app_status", app_id="missing-app", **kwargs)

    assert raised.value.status == 404
    assert "missing-app" in raised.value.reason
    status_manager.api.list_namespaced_pod.assert_called_once_with(
        namespace="spark", label_selector="spark-app-id=missing-app"
    )


def test_missing_pod_name_preserves_not_found(status_manager):
    error = status_manager.exception(status=404, reason="Pod not found")
    status_manager.api.read_namespaced_pod.side_effect = error

    with pytest.raises(status_manager.exception) as raised:
        status_manager.call("app_status", pod_name="missing-driver", client=status_manager.api)

    assert raised.value is error
    status_manager.api.read_namespaced_pod.assert_called_once_with(namespace="spark", name="missing-driver")
    status_manager.api.list_namespaced_pod.assert_not_called()


def test_existing_app_id_returns_status(status_manager):
    pod = SimpleNamespace(status=SimpleNamespace(phase="Running"))
    status_manager.api.list_namespaced_pod.return_value = SimpleNamespace(items=[pod])

    assert status_manager.call("app_status", app_id="running-app") == SparkAppStatus.Running


@pytest.mark.parametrize("already_running", [False, True])
def test_waiter_reports_an_unknown_outcome_when_app_id_disappears(status_manager, already_running):
    from spark_on_k8s.utils.spark_app_status import SparkAppDeletedError

    responses = [SimpleNamespace(items=[])]
    if already_running:
        pod = SimpleNamespace(status=SimpleNamespace(phase="Running"))
        responses.insert(0, SimpleNamespace(items=[pod]))
    status_manager.api.list_namespaced_pod.side_effect = responses

    # A vanished driver is not a completed one: its outcome is unknown.
    with pytest.raises(SparkAppDeletedError, match="outcome is unknown"):
        status_manager.call("wait_for_app", app_id="deleted-app", poll_interval=0)

    assert status_manager.api.list_namespaced_pod.call_count == len(responses)
