from __future__ import annotations

import asyncio
import base64
import dataclasses
import logging
import re
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


@pytest.fixture(autouse=True)
def readiness_log_state(monkeypatch):
    # Readiness logs transitions per worker process; each test starts as a new worker.
    monkeypatch.setattr(run_history, "_readiness", None)


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
    config = client.get("/runs/v1/config").json()
    collection = config.pop("collection")
    assert config == {
        "schema_version": 1,
        "enabled": True,
        "namespaces": ["analytics"],
        "retention_days": 30,
        "max_rows": 10000,
        "readiness": "degraded",
        "freshness_seconds": 85,
    }
    assert [(item["namespace"], item["fresh"], item["last_error"]) for item in collection] == [
        ("analytics", True, None)
    ]
    assert collection[0]["last_success"].endswith("Z") and collection[0]["last_attempt"].endswith("Z")
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


@pytest.mark.parametrize("first, status", [("1" + "0" * 400, 400), (str(2**70), 200)])
def test_out_of_range_cursor_is_not_reported_as_history_unavailability(api, first, status):
    client = api[0]
    payload = '{"first":' + first + ',"uid":"x","filters":[null,null,"all",["analytics"]]}'
    response = client.get("/runs/v1", params={"cursor": base64.urlsafe_b64encode(payload.encode()).decode()})
    assert response.status_code == status
    if status == 400:
        assert response.json()["detail"]["code"] == "invalid_cursor"


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


@pytest.fixture
def two_namespaces(api, monkeypatch):
    client, store, lease, settings, k8s = api
    configured = dataclasses.replace(settings, namespaces=("analytics", "batch"))
    monkeypatch.setattr(run_history, "get_history_settings", lambda: configured)
    store.snapshot("batch", [record("batch-run", "batch")], lease)

    def age(namespace, error=None):
        with sqlite3.connect(store.path) as db:
            db.execute(
                "UPDATE collection SET last_success=last_success-?, last_error=? WHERE namespace=?",
                (settings.freshness_seconds + 1, error, namespace),
            )

    return client, configured, k8s, age


def test_one_stale_namespace_degrades_readiness_without_removing_the_api(two_namespaces, monkeypatch):
    client, settings, _, age = two_namespaces
    assert client.get("/ready").json() == {"status": "ready"}
    age("batch", "forbidden")
    degraded = client.get("/ready")
    assert degraded.status_code == 200 and client.head("/ready").status_code == 200
    # The public probe body names no namespace or error; the authorized config does.
    assert degraded.json() == {"status": "degraded"}
    collection = {item["namespace"]: item for item in client.get("/runs/v1/config").json()["collection"]}
    assert collection["batch"]["fresh"] is False and collection["batch"]["last_error"] == "forbidden"
    assert collection["analytics"]["fresh"] is True
    strict = dataclasses.replace(settings, readiness="strict")
    monkeypatch.setattr(run_history, "get_history_settings", lambda: strict)
    assert client.get("/ready").json() == {"status": "not_ready"}
    monkeypatch.setattr(run_history, "get_history_settings", lambda: settings)
    age("analytics")
    every = client.get("/ready")
    assert every.status_code == 503 and every.json() == {"status": "not_ready"}


def test_kubernetes_failure_in_every_namespace_is_not_ready_but_one_is_tolerated(two_namespaces, monkeypatch):
    client, settings, k8s, _ = two_namespaces

    async def listing(**kwargs):
        if kwargs["namespace"] == "batch" or everywhere:
            raise ApiException(status=429, reason="sensitive throttling details")
        return object()

    everywhere = False
    k8s.list_namespaced_pod.side_effect = listing
    # Collection is still fresh; a persistent failure surfaces once it makes the namespace stale.
    assert client.get("/ready").json() == {"status": "ready"}
    strict = dataclasses.replace(settings, readiness="strict")
    monkeypatch.setattr(run_history, "get_history_settings", lambda: strict)
    assert client.get("/ready").status_code == 503
    monkeypatch.setattr(run_history, "get_history_settings", lambda: settings)
    everywhere = True
    response = client.get("/ready")
    assert response.status_code == 503 and "sensitive" not in response.text


def test_unusable_history_store_is_not_ready(two_namespaces):
    client, *_ = two_namespaces
    store = run_history._collector.store
    with sqlite3.connect(store.path) as db:
        db.execute("DROP TABLE collection")
    assert client.get("/ready").json() == {"status": "not_ready"}
    config = client.get("/runs/v1/config")
    assert config.status_code == 503 and config.json()["detail"]["code"] == "history_unavailable"
    assert str(store.path) not in config.text


def test_readiness_logs_status_changes_once_without_error_text(two_namespaces, caplog):
    client, _, k8s, age = two_namespaces
    caplog.set_level(logging.INFO, logger=run_history.__name__)
    for _ in range(3):
        assert client.get("/ready").status_code == 200
    assert caplog.messages == []
    age("batch", "throttled")
    for _ in range(3):
        client.get("/ready")
    age("analytics", "timeout")
    for _ in range(3):
        client.get("/ready")
    store = run_history._collector.store
    lease = store.acquire("collector", 300)
    store.snapshot("analytics", [], lease)
    store.snapshot("batch", [], lease)
    for _ in range(3):
        assert client.get("/ready").json() == {"status": "ready"}
    k8s.list_namespaced_pod.side_effect = ApiException(status=403, reason="Bearer secret-token")
    for _ in range(3):
        client.get("/ready")
    assert [(record.levelno, record.getMessage()) for record in caplog.records] == [
        (
            logging.WARNING,
            "API is ready but degraded: run-history collection is stale in batch (last error: throttled)",
        ),
        (
            logging.WARNING,
            "API is not ready: no run-history namespace has a fresh collection: "
            "analytics (last error: timeout), batch (last error: throttled)",
        ),
        (logging.INFO, "API readiness recovered"),
        (
            logging.WARNING,
            "API is not ready: Kubernetes pod listing failed in analytics (forbidden), batch (forbidden)",
        ),
    ]
    assert "secret-token" not in caplog.text


def test_readiness_timeout_cancels_stalled_access_and_returns_generic_error(api, monkeypatch, caplog):
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
    assert caplog.messages == ["API is not ready: the readiness check timed out"]


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
    assert 'spark_on_k8s_collection_stale{namespace="analytics"} 0' in result.text
    with sqlite3.connect(store.path) as db:
        db.execute("UPDATE collection SET last_success=last_success-?", (86,))
    assert 'spark_on_k8s_collection_stale{namespace="analytics"} 1' in client.get("/metrics").text
    with sqlite3.connect(store.path) as db:
        db.execute("DELETE FROM collection")
    never = client.get("/metrics").text
    assert 'spark_on_k8s_collection_age_seconds{namespace="analytics"} -1.000' in never
    assert 'spark_on_k8s_collection_stale{namespace="analytics"} 1' in never


def test_metric_families_are_single_groups_for_multiple_namespaces(api, monkeypatch):
    client, store, lease, settings, _ = api
    configured = dataclasses.replace(settings, namespaces=("analytics", "batch"))
    monkeypatch.setattr(run_history, "get_history_settings", lambda: configured)
    store.snapshot("batch", [record("batch-run", "batch")], lease)
    groups = []
    for line in client.get("/metrics").text.splitlines():
        name = line.split()[2] if line.startswith("# ") else re.split(r"[{ ]", line, maxsplit=1)[0]
        if not groups or groups[-1][0] != name:
            groups.append((name, []))
        groups[-1][1].append(line)
    names = [name for name, _ in groups]
    # The text exposition format requires each family's lines to be one group, metadata first.
    assert len(names) == len(set(names)) == 6
    for name, lines in groups:
        metadata = [line for line in lines if line.startswith("#")]
        assert lines[: len(metadata)] == metadata
        assert any(line.startswith(f"# TYPE {name} ") for line in metadata)
        assert len(lines) > len(metadata)
    assert (
        'spark_on_k8s_runs_recorded{namespace="batch",status="Running"} 1'
        in dict(groups)["spark_on_k8s_runs_recorded"]
    )
    assert dict(groups)["spark_on_k8s_collection_stale"][2:] == [
        'spark_on_k8s_collection_stale{namespace="analytics"} 0',
        'spark_on_k8s_collection_stale{namespace="batch"} 0',
    ]


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


def test_without_history_kubernetes_failure_is_not_ready_and_logged(monkeypatch, caplog):
    monkeypatch.setattr(run_history, "get_history_settings", HistorySettings)
    monkeypatch.setattr(run_history.KubernetesClientSingleton, "client", AsyncMock(return_value=object()))
    k8s = Mock(list_namespaced_pod=AsyncMock(side_effect=ApiException(status=401, reason="expired token")))
    monkeypatch.setattr(run_history, "CoreV1Api", Mock(return_value=k8s))
    app = FastAPI()
    app.include_router(run_history.router)
    client = TestClient(app)
    assert client.get("/ready").json() == {"status": "not_ready"}
    namespace = k8s.list_namespaced_pod.call_args.kwargs["namespace"]
    assert caplog.messages == [
        f"API is not ready: Kubernetes pod listing failed in {namespace} (unauthorized)"
    ]
    assert "expired token" not in caplog.text


def test_enabled_store_not_yet_initialized_is_unavailable(api, monkeypatch, caplog):
    client, _, _, _, _ = api
    monkeypatch.setattr(run_history, "_collector", None)
    assert client.get("/runs/v1").status_code == 503
    assert client.get("/ready").status_code == 503
    assert caplog.messages == ["API is not ready: the run-history store is unavailable"]


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


def test_readiness_remains_public_under_a_deployment_prefix_but_metrics_do_not(api, monkeypatch):
    monkeypatch.setattr(
        remote_auth, "get_submission_settings", lambda: SubmissionSettings(True, b"operator-token")
    )
    app = FastAPI(root_path="/platform/spark")
    app.add_middleware(RemoteAccessMiddleware)
    app.include_router(run_history.router)
    client = TestClient(app)
    assert client.get("/platform/spark/ready").status_code == 200
    assert client.head("/platform/spark/ready").status_code == 200
    assert client.get("/platform/spark/metrics").status_code == 401
    assert client.get("/platform/spark-extra/ready").status_code == 401
