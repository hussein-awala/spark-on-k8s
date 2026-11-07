from __future__ import annotations

from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from spark_on_k8s.api.webserver import router


@pytest.mark.parametrize("query, tail", [("", -1), ("?tail=25", 25)])
@pytest.mark.parametrize("lines", [[], ["first line", "<literal log entry>", "last line"]])
def test_websocket_sends_ordered_logs_and_closes(query, tail, lines):
    finished = []

    async def logs():
        try:
            for line in lines:
                yield line
        finally:
            finished.append(True)

    app = FastAPI()
    app.include_router(router)
    with mock.patch("spark_on_k8s.api.webserver.AsyncSparkAppManager") as manager_class:
        manager_class.return_value.logs_streamer.return_value = logs()
        with TestClient(app) as client:
            with client.websocket_connect("/webserver/ws/logs/analytics/app-1" + query) as websocket:
                assert [websocket.receive_text() for _ in lines] == lines
                close = websocket.receive()
        manager_class.return_value.logs_streamer.assert_called_once_with(
            namespace="analytics", app_id="app-1", tail_lines=tail
        )
    assert close["type"] == "websocket.close"
    assert close["code"] == 1000
    assert finished == [True]
