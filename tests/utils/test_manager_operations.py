from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from spark_on_k8s.utils import app_manager, async_app_manager


def driver_pod(phase="Running"):
    return SimpleNamespace(
        metadata=SimpleNamespace(name="actual-driver", labels={"spark-app-id": "report-123"}),
        spec=SimpleNamespace(
            containers=[SimpleNamespace(name="spark-driver"), SimpleNamespace(name="metrics")]
        ),
        status=SimpleNamespace(phase=phase),
    )


@pytest.fixture(params=["sync", "async"])
def manager_case(request, monkeypatch):
    asynchronous = request.param == "async"
    module = async_app_manager if asynchronous else app_manager
    client_module = module.k8s_async if asynchronous else module.k8s
    manager_type = module.AsyncSparkAppManager if asynchronous else module.SparkAppManager
    api = MagicMock()
    if asynchronous:
        for name in ["read_namespaced_pod", "list_namespaced_pod", "delete_namespaced_pod"]:
            setattr(api, name, AsyncMock())
    api.read_namespaced_pod.return_value = driver_pod()
    api.list_namespaced_pod.return_value = SimpleNamespace(items=[driver_pod()])
    clients = Mock()
    context = MagicMock()
    context.__exit__.return_value = False
    context.__aexit__.return_value = False
    clients.client.return_value = context
    websocket = MagicMock()
    websocket.close = AsyncMock()
    websocket.__aenter__.return_value = websocket

    async def close_websocket(*args):
        await websocket.close()

    websocket.__aexit__.side_effect = close_websocket
    ws_api = Mock(api_client=websocket)

    def make_api(*args, **kwargs):
        return ws_api if "api_client" in kwargs else api

    monkeypatch.setattr(client_module, "CoreV1Api", Mock(side_effect=make_api))
    if asynchronous:
        monkeypatch.setattr(module, "WsApiClient", Mock(return_value=websocket))
    execute = AsyncMock() if asynchronous else Mock()
    monkeypatch.setattr(module, "stream", execute)
    manager = manager_type(k8s_client_manager=clients)

    def call(method, **kwargs):
        result = getattr(manager, method)(namespace="analytics", **kwargs)
        return asyncio.run(result) if asynchronous else result

    def assert_closed(error=None):
        if asynchronous:
            context.__aexit__.assert_awaited_once()
            args = context.__aexit__.await_args.args
        else:
            context.__exit__.assert_called_once()
            args = context.__exit__.call_args.args
        assert args[1] is error

    return SimpleNamespace(
        api=api,
        call=call,
        module=module,
        manager=manager,
        clients=clients,
        execute=execute,
        websocket=websocket,
        ws_api=ws_api,
        asynchronous=asynchronous,
        assert_closed=assert_closed,
    )


@pytest.mark.parametrize("method", ["app_status", "kill_app", "delete_app"])
def test_operations_require_a_target(manager_case, method):
    with pytest.raises(ValueError, match="Either pod_name or app_id must be specified"):
        manager_case.call(method)

    manager_case.api.read_namespaced_pod.assert_not_called()
    manager_case.api.list_namespaced_pod.assert_not_called()
    manager_case.api.delete_namespaced_pod.assert_not_called()
    manager_case.execute.assert_not_called()


@pytest.mark.parametrize("method", ["kill_app", "delete_app"])
def test_missing_application_never_mutates_resources(manager_case, method):
    manager_case.api.list_namespaced_pod.return_value = SimpleNamespace(items=[])

    with pytest.raises(ValueError, match="No pods found for app absent") as raised:
        manager_case.call(method, app_id="absent")

    manager_case.api.list_namespaced_pod.assert_called_once_with(
        namespace="analytics",
        label_selector="spark-app-id=absent" + (",spark-role=driver" if method == "delete_app" else ""),
    )
    manager_case.api.delete_namespaced_pod.assert_not_called()
    manager_case.execute.assert_not_called()
    manager_case.assert_closed(raised.value)


@pytest.mark.parametrize("target", [{"pod_name": "actual-driver"}, {"app_id": "report-123"}])
def test_kill_targets_the_selected_pod_and_driver_container(manager_case, target):
    manager_case.call("kill_app", **target)

    if "pod_name" in target:
        manager_case.api.read_namespaced_pod.assert_called_once_with(
            namespace="analytics", name="actual-driver"
        )
        manager_case.api.list_namespaced_pod.assert_not_called()
    else:
        manager_case.api.list_namespaced_pod.assert_called_once_with(
            namespace="analytics", label_selector="spark-app-id=report-123"
        )
        manager_case.api.read_namespaced_pod.assert_not_called()
    exec_api = manager_case.ws_api if manager_case.asynchronous else manager_case.api
    manager_case.execute.assert_called_once_with(
        exec_api.connect_get_namespaced_pod_exec,
        "actual-driver",
        "analytics",
        command=["/bin/sh", "-c", "kill 1"],
        container="spark-driver",
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        _preload_content=manager_case.asynchronous,
    )
    if manager_case.asynchronous:
        manager_case.execute.assert_awaited_once()
        manager_case.websocket.close.assert_awaited_once()
    manager_case.assert_closed()


@pytest.mark.parametrize("phase", ["Pending", "Succeeded", "Failed"])
def test_kill_does_not_exec_into_a_nonrunning_pod(manager_case, phase):
    manager_case.api.read_namespaced_pod.return_value = driver_pod(phase)

    if manager_case.asynchronous:
        with pytest.raises(ValueError, match="Pod actual-driver is not running") as raised:
            manager_case.call("kill_app", pod_name="actual-driver")
        manager_case.assert_closed(raised.value)
    else:
        assert manager_case.call("kill_app", pod_name="actual-driver") is None
        manager_case.assert_closed()

    manager_case.execute.assert_not_called()
    manager_case.websocket.close.assert_not_called()


@pytest.mark.parametrize("target", [{"pod_name": "actual-driver"}, {"app_id": "report-123"}])
@pytest.mark.parametrize("force", [False, True])
def test_delete_uses_foreground_propagation_and_explicit_force(manager_case, target, force):
    manager_case.call("delete_app", force=force, **target)

    if "app_id" in target:
        manager_case.api.list_namespaced_pod.assert_called_once_with(
            namespace="analytics", label_selector="spark-app-id=report-123,spark-role=driver"
        )
    else:
        manager_case.api.list_namespaced_pod.assert_not_called()
    manager_case.api.delete_namespaced_pod.assert_called_once()
    kwargs = manager_case.api.delete_namespaced_pod.call_args.kwargs
    assert kwargs["name"] == "actual-driver"
    assert kwargs["namespace"] == "analytics"
    assert kwargs["body"].propagation_policy == "Foreground"
    assert kwargs["body"].grace_period_seconds == (0 if force else None)
    if manager_case.asynchronous:
        manager_case.api.delete_namespaced_pod.assert_awaited_once()
    manager_case.assert_closed()


@pytest.mark.parametrize(
    "method,api_method", [("kill_app", "read_namespaced_pod"), ("delete_app", "delete_namespaced_pod")]
)
def test_operation_failures_propagate_and_release_the_client(manager_case, method, api_method):
    error = manager_case.module.ApiException(status=403, reason="Forbidden")
    getattr(manager_case.api, api_method).side_effect = error

    with pytest.raises(manager_case.module.ApiException) as raised:
        manager_case.call(method, pod_name="actual-driver")

    assert raised.value is error
    manager_case.execute.assert_not_called()
    manager_case.assert_closed(error)


def test_list_apps_selects_drivers_and_returns_application_ids(monkeypatch):
    api = Mock()
    first = driver_pod()
    second = driver_pod()
    second.metadata.labels = {"spark-app-id": "other-456"}
    api.list_namespaced_pod.return_value = SimpleNamespace(items=[first, second])
    monkeypatch.setattr(app_manager.k8s, "CoreV1Api", Mock(return_value=api))
    manager = app_manager.SparkAppManager(k8s_client_manager=MagicMock())

    assert manager.list_apps("analytics") == ["report-123", "other-456"]
    api.list_namespaced_pod.assert_called_once_with(namespace="analytics", label_selector="spark-role=driver")


def test_list_apps_skips_drivers_not_submitted_by_this_client(monkeypatch):
    # spark-submit cluster mode and other launchers also label drivers with spark-role=driver.
    api = Mock()
    native = driver_pod()
    native.metadata.labels = {"spark-role": "driver", "spark-app-selector": "spark-0123"}
    unlabeled = driver_pod()
    unlabeled.metadata.labels = None
    api.list_namespaced_pod.return_value = SimpleNamespace(items=[native, driver_pod(), unlabeled])
    monkeypatch.setattr(app_manager.k8s, "CoreV1Api", Mock(return_value=api))
    manager = app_manager.SparkAppManager(k8s_client_manager=MagicMock())

    assert manager.list_apps("analytics") == ["report-123"]


def test_status_with_supplied_client_does_not_acquire_another_client(manager_case):
    status = manager_case.call("app_status", pod_name="actual-driver", client=manager_case.api)

    assert status.value == "Running"
    manager_case.api.read_namespaced_pod.assert_called_once_with(namespace="analytics", name="actual-driver")
    manager_case.api.list_namespaced_pod.assert_not_called()
    manager_case.clients.client.assert_not_called()
