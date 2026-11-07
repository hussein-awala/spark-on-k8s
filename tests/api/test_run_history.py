from __future__ import annotations

import asyncio
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from kubernetes_asyncio.client import ApiException
from spark_on_k8s.api import remote_auth, run_history
from spark_on_k8s.api.remote_auth import RemoteAccessMiddleware
from spark_on_k8s.api.submission_policy import SubmissionSettings
from spark_on_k8s.history.settings import HistorySettings
from spark_on_k8s.history.store import RunStore


def record(uid="one", namespace="analytics", phase="Running"):
    return {
        "uid": uid,
        "namespace": namespace,
        "app_id": "report",
        "name": "Report",
        "driver_pod": "report-driver",
        "image": "spark:4",
        "spark_version": "4.0.3",
        "status": phase,
        "created_at": None,
    }


@pytest.fixture
def api(tmp_path, monkeypatch):
    settings = HistorySettings(
        enabled=True, path=tmp_path / "runs.sqlite3", namespaces=("analytics",), metrics_enabled=True
    )
    store = RunStore(settings.path)
    store.initialize()
    lease = store.acquire("collector", 300)
    store.snapshot("analytics", [record()], lease)
    monkeypatch.setattr(run_history, "get_history_settings", lambda: settings)
    monkeypatch.setattr(run_history, "_collector", SimpleNamespace(store=store))
    k8s = Mock(list_namespaced_pod=AsyncMock(return_value=object()))
    monkeypatch.setattr(run_history.KubernetesClientSingleton, "client", AsyncMock(return_value=object()))
    monkeypatch.setattr(run_history, "CoreV1Api", Mock(return_value=k8s))
    app = FastAPI()
    app.include_router(run_history.router)
    return TestClient(app), store, lease, settings, k8s


def test_history_config_pagination_filtering_and_disappearance(api):
    client, store, lease, _, _ = api
    store.snapshot("analytics", [record("two", phase="Succeeded"), record("three")], lease)
    store.snapshot("outside", [record("unconfigured", "outside")], lease)
    config = client.get("/runs/v1/config")
    assert config.json() == {
        "schema_version": 1,
        "enabled": True,
        "namespaces": ["analytics"],
        "retention_days": 30,
        "max_rows": 10000,
    }
    result = client.get("/runs/v1", params={"limit": 1})
    assert result.status_code == 200
    assert result.headers["cache-control"] == "no-store"
    payload = result.json()
    seen = [payload["items"][0]["uid"]]
    while payload["next_cursor"]:
        payload = client.get("/runs/v1", params={"limit": 1, "cursor": payload["next_cursor"]}).json()
        seen.extend(item["uid"] for item in payload["items"])
    assert set(seen) == {"one", "two", "three"}
    assert len(seen) == len(set(seen))
    missing = client.get("/runs/v1", params={"presence": "missing"}).json()["items"]
    assert len(missing) == 1 and missing[0]["uid"] == "one" and missing[0]["status"] == "Running"
    succeeded = client.get("/runs/v1", params={"status": "Succeeded", "namespace": "analytics"}).json()[
        "items"
    ]
    assert [row["uid"] for row in succeeded] == ["two"]
    assert client.get("/runs/v1", params={"namespace": "outside"}).status_code == 403
    assert client.get("/runs/v1", params={"cursor": "bad"}).status_code == 400
    assert client.get("/runs/v1", params={"status": "Deleted"}).status_code == 422
    assert client.get("/runs/v1", params={"limit": 1000}).status_code == 422


def test_liveness_probe_auth_exception_is_extended_only_to_ready_and_not_metrics(api, monkeypatch):
    _, _, _, _, k8s = api
    monkeypatch.setattr(
        remote_auth, "get_submission_settings", lambda: SubmissionSettings(True, b"operator-token")
    )
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(run_history.router)
    client = TestClient(app)
    assert client.get("/ready").json() == {"status": "ready"}
    assert client.head("/ready").status_code == 200
    assert client.post("/ready").status_code == 401
    assert client.get("/metrics").status_code == 401
    assert client.get("/runs/v1").status_code == 401
    assert client.get("/metrics", headers={"Authorization": "Bearer operator-token"}).status_code == 200
    assert k8s.list_namespaced_pod.call_args.kwargs == {
        "namespace": "analytics",
        "label_selector": "spark-role=driver",
        "limit": 1,
        "_request_timeout": 2,
    }


def test_readiness_requires_real_namespace_access_and_recent_collection(api):
    client, store, _, settings, k8s = api
    k8s.list_namespaced_pod.side_effect = ApiException(status=403, reason="sensitive credential details")
    denied = client.get("/ready")
    assert denied.status_code == 503 and denied.json() == {"status": "not_ready"}
    assert "sensitive" not in denied.text
    k8s.list_namespaced_pod.side_effect = None
    with sqlite3.connect(store.path) as db:
        db.execute("UPDATE collection SET last_success=last_success-?", (settings.freshness_seconds + 1,))
    assert client.get("/ready").status_code == 503
    with sqlite3.connect(store.path) as db:
        db.execute("DELETE FROM collection")
    assert client.get("/ready").status_code == 503


def test_readiness_timeout_cancels_stalled_access_and_returns_generic_error(api, monkeypatch):
    client, _, _, _, k8s = api
    cancelled = []

    async def blocked(**_kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.append(True)

    k8s.list_namespaced_pod.side_effect = blocked
    monkeypatch.setattr(run_history, "READY_TIMEOUT", 0.02)
    assert client.get("/ready").json() == {"status": "not_ready"}
    assert cancelled == [True]


def test_metrics_are_low_cardinality_operational_counts_only(api):
    client, store, lease, _, _ = api
    store.snapshot("analytics", [record("completed", phase="Succeeded")], lease)
    store.failure("analytics", lease)
    result = client.get("/metrics")
    assert result.status_code == 200
    assert result.headers["content-type"].startswith("text/plain; version=0.0.4")
    assert 'spark_on_k8s_runs_recorded{namespace="analytics",status="Succeeded"} 1' in result.text
    assert 'spark_on_k8s_collection_successes_total{namespace="analytics"} 2' in result.text
    assert 'spark_on_k8s_collection_failures_total{namespace="analytics"} 1' in result.text
    assert "collection_age_seconds" in result.text
    assert (
        "report-driver" not in result.text and "completed" not in result.text and "spark:4" not in result.text
    )
    with sqlite3.connect(store.path) as db:
        db.execute("DELETE FROM collection")
    assert 'spark_on_k8s_collection_age_seconds{namespace="analytics"} -1.000' in client.get("/metrics").text


def test_unavailable_database_returns_generic_history_and_metrics_errors(api):
    client, store, _, _, _ = api
    with sqlite3.connect(store.path) as db:
        db.execute("DROP TABLE runs")
    response = client.get("/runs/v1")
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "history_unavailable"
    assert str(store.path) not in response.text
    assert client.get("/metrics").status_code == 503


def test_disabled_history_and_metrics_do_not_create_a_store(monkeypatch):
    monkeypatch.setattr(run_history, "get_history_settings", HistorySettings)
    monkeypatch.setattr(run_history, "_collector", None)
    provider = AsyncMock(return_value=object())
    monkeypatch.setattr(run_history.KubernetesClientSingleton, "client", provider)
    k8s = Mock(list_namespaced_pod=AsyncMock())
    monkeypatch.setattr(run_history, "CoreV1Api", Mock(return_value=k8s))
    app = FastAPI()
    app.include_router(run_history.router)
    client = TestClient(app)
    assert client.get("/runs/v1").status_code == 404
    assert client.get("/runs/v1/config").status_code == 404
    assert client.get("/metrics").status_code == 404
    assert client.get("/ready").status_code == 200
    asyncio.run(run_history.start_history())
    assert run_history._collector is None
    asyncio.run(run_history.stop_history())
    monkeypatch.setattr(run_history, "get_history_settings", lambda: HistorySettings(metrics_enabled=True))
    assert "spark_on_k8s_history_enabled 0" in client.get("/metrics").text


def test_enabled_store_not_yet_initialized_is_unavailable(api, monkeypatch):
    client, _, _, _, _ = api
    monkeypatch.setattr(run_history, "_collector", None)
    assert client.get("/runs/v1").status_code == 503
    assert client.get("/ready").status_code == 503


def test_real_api_history_lifecycle_creates_database_and_releases_lease(tmp_path, monkeypatch):
    from spark_on_k8s.history import collector as collector_module

    path = tmp_path / "runs.sqlite3"
    settings = HistorySettings(enabled=True, path=path, namespaces=("analytics",))
    monkeypatch.setattr(run_history, "get_history_settings", lambda: settings)
    monkeypatch.setattr(run_history, "_collector", None)
    api = Mock(list_namespaced_pod=AsyncMock(return_value=SimpleNamespace(items=[], metadata=None)))
    monkeypatch.setattr(collector_module, "CoreV1Api", Mock(return_value=api))
    monkeypatch.setattr(run_history.KubernetesClientSingleton, "client", AsyncMock(return_value=object()))

    async def scenario():
        await run_history.start_history()
        collector = run_history._collector

        async def wait_for_snapshot():
            while not await asyncio.to_thread(collector.store.health):
                await asyncio.sleep(0.001)

        await asyncio.wait_for(wait_for_snapshot(), 2)
        assert path.exists()
        await run_history.stop_history()
        assert run_history._collector is None
        assert collector.task is None
        assert RunStore(path).acquire("replacement", 30) is not None

    asyncio.run(scenario())


def test_runs_page_uses_packaged_console_and_is_separate_from_spark_history_server(api, monkeypatch):
    from spark_on_k8s.api import webserver

    monkeypatch.setattr(webserver, "get_history_settings", run_history.get_history_settings)
    app = FastAPI()
    app.include_router(webserver.router)
    from starlette.staticfiles import StaticFiles

    app.mount("/static", StaticFiles(directory=webserver.current_dir / "static"), name="static")
    client = TestClient(app)
    response = client.get("/webserver/runs")
    assert response.status_code == 200
    assert '"view": "runs"' in response.text and '"runsEnabled": true' in response.text
    assert client.get("/webserver/runs", params={"namespace": "outside"}).status_code == 400
    monkeypatch.setattr(webserver, "get_history_settings", HistorySettings)
    assert client.get("/webserver/runs").status_code == 404
