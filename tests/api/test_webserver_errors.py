from __future__ import annotations

from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from kubernetes_asyncio.client import ApiException
from spark_on_k8s.api.webserver import router
from starlette.responses import Response


@pytest.mark.parametrize("status, expected_status", [(403, 403), (404, 404), (500, 500), (None, 500)])
def test_webserver_preserves_kubernetes_error_status(status, expected_status):
    app = FastAPI()
    app.include_router(router)
    app.add_route("/static/{path:path}", lambda request: Response(), name="static")
    error = ApiException(status=status, reason="Kubernetes request failed")
    with mock.patch("spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client") as http_client:
        http_client.return_value.send = mock.AsyncMock(side_effect=error)
        response = TestClient(app).get("/webserver/ui/analytics/app-1")

    assert response.status_code == expected_status
    assert response.headers["content-type"].startswith("text/html")
    assert "Kubernetes request failed" in response.text
