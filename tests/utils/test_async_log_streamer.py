from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from kubernetes_asyncio.watch import Watch
from spark_on_k8s.utils import async_app_manager


def test_pending_log_stream_waits_between_status_requests(monkeypatch):
    calls = []
    phases = iter(["Pending", "Pending", "Running"])
    api = MagicMock()

    async def read_pod(**kwargs):
        phase = next(phases)
        calls.append(("read", phase))
        return SimpleNamespace(
            status=SimpleNamespace(phase=phase),
            spec=SimpleNamespace(
                containers=[SimpleNamespace(name="spark-driver"), SimpleNamespace(name="metrics")]
            ),
        )

    api.read_namespaced_pod = AsyncMock(side_effect=read_pod)
    monkeypatch.setattr(async_app_manager.k8s_async, "CoreV1Api", Mock(return_value=api))
    monkeypatch.setattr(
        async_app_manager.asyncio,
        "sleep",
        AsyncMock(side_effect=lambda delay: calls.append(("sleep", delay))),
    )
    manager = async_app_manager.AsyncSparkAppManager(k8s_client_manager=MagicMock())

    async def collect_logs():
        watcher = Watch()
        watcher.next = AsyncMock(side_effect=["ready", StopAsyncIteration])
        monkeypatch.setattr(async_app_manager.watch, "Watch", lambda: watcher)
        try:
            return [line async for line in manager.logs_streamer(namespace="spark", pod_name="driver")]
        finally:
            await watcher.close()

    assert asyncio.run(collect_logs()) == ["ready"]
    assert calls == [
        ("read", "Pending"),
        ("sleep", 5),
        ("read", "Pending"),
        ("sleep", 5),
        ("read", "Running"),
    ]


@pytest.mark.parametrize("termination", ["exhausted", "error", "consumer_close", "cancelled"])
def test_log_stream_closes_watch_resources(monkeypatch, termination):
    api = MagicMock()
    api.read_namespaced_pod = AsyncMock(
        return_value=SimpleNamespace(
            status=SimpleNamespace(phase="Running"),
            spec=SimpleNamespace(
                containers=[SimpleNamespace(name="spark-driver"), SimpleNamespace(name="metrics")]
            ),
        )
    )
    monkeypatch.setattr(async_app_manager.k8s_async, "CoreV1Api", Mock(return_value=api))
    client_manager = MagicMock()
    manager = async_app_manager.AsyncSparkAppManager(k8s_client_manager=client_manager)

    async def check_lifecycle():
        watcher = Watch()
        response = Mock()
        watcher.resp = response
        monkeypatch.setattr(async_app_manager.watch, "Watch", lambda: watcher)
        reading = asyncio.Event()
        call_count = 0

        async def next_line():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return "first line"
            if termination == "error":
                raise RuntimeError("log connection lost")
            if termination == "cancelled":
                reading.set()
                await asyncio.Event().wait()
            raise StopAsyncIteration

        watcher.next = next_line
        logs = manager.logs_streamer(namespace="spark", pod_name="driver")
        task = None
        try:
            assert await logs.__anext__() == "first line"
            if termination == "consumer_close":
                await logs.aclose()
            elif termination == "error":
                with pytest.raises(RuntimeError, match="log connection lost"):
                    await logs.__anext__()
            elif termination == "cancelled":
                task = asyncio.create_task(logs.__anext__())
                await asyncio.wait_for(reading.wait(), timeout=5)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                with pytest.raises(StopAsyncIteration):
                    await logs.__anext__()
            response.release.assert_called_once()
            client_manager.client.return_value.__aexit__.assert_awaited_once()
            assert watcher._api_client.rest_client.pool_manager.closed
        finally:
            if task is not None:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await logs.aclose()
            await watcher.close()

    asyncio.run(check_lifecycle())


def collect_logs_from_response(monkeypatch, response):
    """Run the stream through the real asynchronous client, replacing only its HTTP transport."""
    import aiohttp
    from spark_on_k8s.k8s import async_client

    k8s_async = async_app_manager.k8s_async
    requests = []

    async def request(self, **kwargs):
        requests.append(kwargs["url"])
        return response

    monkeypatch.setattr(aiohttp.ClientSession, "request", request)
    monkeypatch.setattr(async_client.config, "load_kube_config", AsyncMock())
    monkeypatch.setattr(
        k8s_async.CoreV1Api,
        "read_namespaced_pod",
        AsyncMock(
            return_value=SimpleNamespace(
                status=SimpleNamespace(phase="Failed"),
                spec=SimpleNamespace(containers=[SimpleNamespace(name="driver")]),
            )
        ),
    )
    manager = async_app_manager.AsyncSparkAppManager(
        k8s_client_manager=async_client.KubernetesAsyncClientManager(
            client_configuration=k8s_async.Configuration(host="https://cluster.example"), in_cluster=False
        )
    )

    async def collect_logs():
        return [line async for line in manager.logs_streamer(namespace="spark", pod_name="driver")]

    try:
        return asyncio.run(collect_logs())
    finally:
        assert requests == [
            "https://cluster.example/api/v1/namespaces/spark/pods/driver/log?container=driver&follow=True"
        ]
        response.release.assert_called_once()


@pytest.mark.parametrize(
    "status,reason",
    [(400, "Bad Request"), (403, "Forbidden"), (404, "Not Found"), (500, "Internal Server Error")],
)
def test_log_stream_raises_a_rejected_log_request_instead_of_yielding_its_body(monkeypatch, status, reason):
    """The asynchronous client does not check a streamed response: an error document is not log text."""
    body = (
        b'{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure",'
        b'"message":"the log request was rejected","code":%d}\n' % status
    )
    response = Mock(status=status, reason=reason)
    response.content.readline = AsyncMock(side_effect=[body, b""])

    with pytest.raises(async_app_manager.ApiException) as raised:
        collect_logs_from_response(monkeypatch, response)

    assert (raised.value.status, raised.value.reason) == (status, reason)
    assert raised.value.body == body.decode()


def test_log_stream_yields_the_lines_of_an_accepted_log_request(monkeypatch):
    response = Mock(status=200, reason="OK")
    response.content.readline = AsyncMock(side_effect=[b"first line\n", b'{"code": 500}\n', b""])

    assert collect_logs_from_response(monkeypatch, response) == ["first line\n", '{"code": 500}\n']
