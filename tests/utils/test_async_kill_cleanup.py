from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from kubernetes_asyncio.stream import WsApiClient
from spark_on_k8s.utils import async_app_manager


@pytest.mark.parametrize("outcome", ["success", "api_error", "cancelled"])
def test_kill_closes_websocket_and_outer_client(monkeypatch, outcome):
    async def check_cleanup():
        websocket = WsApiClient()
        context = AsyncMock()
        context.__aexit__.return_value = False
        clients = Mock()
        clients.client.return_value = context
        api = Mock()
        api.read_namespaced_pod = AsyncMock(
            return_value=SimpleNamespace(
                metadata=SimpleNamespace(name="actual-driver"),
                spec=SimpleNamespace(containers=[SimpleNamespace(name="spark-driver")]),
                status=SimpleNamespace(phase="Running"),
            )
        )
        ws_api = Mock(api_client=websocket)
        monkeypatch.setattr(
            async_app_manager.k8s_async,
            "CoreV1Api",
            Mock(side_effect=lambda *args, **kwargs: ws_api if "api_client" in kwargs else api),
        )
        monkeypatch.setattr(async_app_manager, "WsApiClient", Mock(return_value=websocket))
        request_started = asyncio.Event()
        error = async_app_manager.ApiException(status=403, reason="Exec forbidden")

        async def execute(*args, **kwargs):
            request_started.set()
            if outcome == "api_error":
                raise error
            if outcome == "cancelled":
                await asyncio.Event().wait()
            return ""

        execute_mock = AsyncMock(side_effect=execute)
        monkeypatch.setattr(async_app_manager, "stream", execute_mock)
        manager = async_app_manager.AsyncSparkAppManager(k8s_client_manager=clients)
        try:
            task = asyncio.create_task(manager.kill_app(namespace="analytics", pod_name="actual-driver"))
            if outcome == "cancelled":
                await asyncio.wait_for(request_started.wait(), timeout=1)
                task.cancel()
                with pytest.raises(asyncio.CancelledError) as raised:
                    await task
                expected_error = raised.value
            elif outcome == "api_error":
                with pytest.raises(async_app_manager.ApiException) as raised:
                    await task
                assert raised.value is error
                expected_error = error
            else:
                assert await task is None
                expected_error = None

            execute_mock.assert_awaited_once_with(
                ws_api.connect_get_namespaced_pod_exec,
                "actual-driver",
                "analytics",
                command=["/bin/sh", "-c", "kill 1"],
                container="spark-driver",
                stderr=True,
                stdin=False,
                stdout=True,
                tty=False,
                _preload_content=True,
            )
            context.__aexit__.assert_awaited_once()
            assert context.__aexit__.await_args.args[1] is expected_error
            assert websocket.rest_client.pool_manager.closed
        finally:
            # Keep the failing baseline probe from leaking its real aiohttp session.
            await websocket.close()

    asyncio.run(check_cleanup())
