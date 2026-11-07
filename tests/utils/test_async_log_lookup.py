from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from spark_on_k8s.utils import async_app_manager


@pytest.fixture
def log_lookup(monkeypatch):
    api = Mock()
    api.list_namespaced_pod = AsyncMock(
        return_value=SimpleNamespace(
            items=[SimpleNamespace(metadata=SimpleNamespace(name="selected-driver"))]
        )
    )
    api.read_namespaced_pod = AsyncMock(return_value=SimpleNamespace(status=SimpleNamespace(phase="Running")))
    context = AsyncMock()
    context.__aexit__.return_value = False
    clients = Mock()
    clients.client.return_value = context
    watcher = MagicMock()
    watcher.__aenter__.return_value = watcher
    watcher.__aexit__.return_value = False

    async def log_lines():
        yield "first"
        yield "second"

    watcher.stream.return_value = log_lines()
    watch_factory = Mock(return_value=watcher)
    monkeypatch.setattr(async_app_manager.watch, "Watch", watch_factory)
    monkeypatch.setattr(async_app_manager.k8s_async, "CoreV1Api", Mock(return_value=api))
    manager = async_app_manager.AsyncSparkAppManager(k8s_client_manager=clients)

    async def collect(**kwargs):
        return [line async for line in manager.logs_streamer(namespace="analytics", **kwargs)]

    return SimpleNamespace(
        api=api,
        clients=clients,
        context=context,
        watcher=watcher,
        watch_factory=watch_factory,
        collect=collect,
    )


@pytest.mark.parametrize("tail_lines", [-1, 0, 7])
def test_async_logs_resolve_app_id_and_forward_tail_limit(log_lookup, tail_lines):
    assert asyncio.run(log_lookup.collect(app_id="report-123", tail_lines=tail_lines)) == ["first", "second"]

    log_lookup.api.list_namespaced_pod.assert_awaited_once_with(
        namespace="analytics", label_selector="spark-app-id=report-123"
    )
    log_lookup.api.read_namespaced_pod.assert_awaited_once_with(namespace="analytics", name="selected-driver")
    log_lookup.watcher.stream.assert_called_once_with(
        log_lookup.api.read_namespaced_pod_log,
        namespace="analytics",
        name="selected-driver",
        tail_lines=tail_lines if tail_lines > 0 else None,
        follow=True,
    )
    assert log_lookup.context.__aexit__.await_count == 2
    assert all(args.args == (None, None, None) for args in log_lookup.context.__aexit__.await_args_list)
    log_lookup.watcher.__aexit__.assert_awaited_once_with(None, None, None)


def test_async_logs_require_a_target_before_acquiring_resources(log_lookup):
    with pytest.raises(ValueError, match="Either pod_name or app_id must be specified"):
        asyncio.run(log_lookup.collect())

    log_lookup.clients.client.assert_not_called()
    log_lookup.watch_factory.assert_not_called()


def test_async_logs_missing_app_closes_lookup_client(log_lookup):
    log_lookup.api.list_namespaced_pod.return_value = SimpleNamespace(items=[])

    with pytest.raises(ValueError, match="No pods found for app absent") as raised:
        asyncio.run(log_lookup.collect(app_id="absent"))

    log_lookup.context.__aexit__.assert_awaited_once()
    assert log_lookup.context.__aexit__.await_args.args[1] is raised.value
    log_lookup.watch_factory.assert_not_called()


@pytest.mark.parametrize(
    "target,request_method",
    [({"app_id": "report-123"}, "list_namespaced_pod"), ({"pod_name": "driver"}, "read_namespaced_pod")],
)
def test_async_log_lookup_errors_release_client_without_starting_watch(log_lookup, target, request_method):
    error = async_app_manager.ApiException(status=403, reason="Forbidden")
    getattr(log_lookup.api, request_method).side_effect = error

    with pytest.raises(async_app_manager.ApiException) as raised:
        asyncio.run(log_lookup.collect(**target))

    assert raised.value is error
    log_lookup.context.__aexit__.assert_awaited_once()
    assert log_lookup.context.__aexit__.await_args.args[1] is error
    log_lookup.watch_factory.assert_not_called()
