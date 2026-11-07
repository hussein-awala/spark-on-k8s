from __future__ import annotations

from unittest import mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from spark_on_k8s.api.configuration import APIConfiguration
from spark_on_k8s.api.webserver import router


@pytest.mark.parametrize("history_host", [None, ""])
def test_unconfigured_history_proxy_returns_precondition_failed(monkeypatch, history_host):
    monkeypatch.setattr(APIConfiguration, "SPARK_ON_K8S_API_SPARK_HISTORY_HOST", history_host)
    app = FastAPI()
    app.include_router(router)

    with mock.patch("spark_on_k8s.api.webserver.AsyncHttpClientSingleton.client") as client:
        response = TestClient(app).get("/webserver/ui-history/history/app-1/jobs/")

    assert response.status_code == 412
    assert response.json() == {"detail": "SPARK_ON_K8S_API_SPARK_HISTORY_HOST is not set."}
    client.assert_not_called()
