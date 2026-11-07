from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock

import pytest
from kubernetes_asyncio import client as k8s
from spark_on_k8s.history import collector as module
from spark_on_k8s.history.collector import RunCollector, metadata_from_pod
from spark_on_k8s.history.settings import HistorySettings
from spark_on_k8s.history.store import RunStore


def pod(uid="one", namespace="analytics", phase="Running"):
    return k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            uid=uid,
            name="driver",
            namespace=namespace,
            labels={
                "spark-role": "driver",
                "spark-app-id": "report",
                "spark-app-name": "Report",
                "spark-version": "4.0.3",
            },
            annotations={"secret": "do-not-retain"},
            creation_timestamp=datetime(2026, 11, 7, tzinfo=UTC),
        ),
        spec=k8s.V1PodSpec(
            containers=[
                k8s.V1Container(
                    name="driver",
                    image="spark:4",
                    args=["secret-query"],
                    env=[k8s.V1EnvVar(name="TOKEN", value="secret-value")],
                )
            ]
        ),
        status=k8s.V1PodStatus(phase=phase),
    )


def page(items, cursor=None):
    return k8s.V1PodList(items=items, metadata=k8s.V1ListMeta(_continue=cursor))


@pytest.fixture
def setup(tmp_path, monkeypatch):
    settings = HistorySettings(
        enabled=True, path=tmp_path / "runs.sqlite3", namespaces=("analytics",), max_pages=2
    )
    store = RunStore(settings.path)
    store.initialize()
    api = Mock(list_namespaced_pod=AsyncMock())
    monkeypatch.setattr(module, "CoreV1Api", Mock(return_value=api))
    provider = AsyncMock(return_value=object())
    return RunCollector(settings, store, provider), api


def test_complete_paginated_collection_stores_only_operational_metadata(setup):
    collector, api = setup
    api.list_namespaced_pod.side_effect = [page([pod("a")], "second"), page([pod("b")])]
    assert asyncio.run(collector.collect_once())
    items = collector.store.list_runs()["items"]
    assert {item["uid"] for item in items} == {"a", "b"}
    assert "secret" not in str(items)
    assert api.list_namespaced_pod.call_args_list[0].kwargs == {
        "namespace": "analytics",
        "label_selector": "spark-role=driver",
        "limit": 200,
        "_request_timeout": 5,
    }
    assert api.list_namespaced_pod.call_args_list[1].kwargs["_continue"] == "second"
    assert collector.store.health()["analytics"]["successes"] == 1


@pytest.mark.parametrize("failure", ["api", "limit", "repeat", "invalid", "duplicate"])
def test_partial_failed_invalid_or_oversized_snapshot_never_disappears_existing_runs(setup, failure):
    collector, api = setup
    api.list_namespaced_pod.side_effect = [page([pod("existing")])]
    asyncio.run(collector.collect_once())
    failed_sequences = {
        "api": [page([pod("new")], "next"), k8s.ApiException(status=403, reason="sensitive")],
        "limit": [page([pod("new")], "next"), page([pod("other")], "third")],
        "repeat": [page([pod("new")], "next"), page([pod("other")], "next")],
        "invalid": [page([pod("new", namespace="other")])],
        "duplicate": [page([pod("new"), pod("new")])],
    }
    api.list_namespaced_pod.side_effect = failed_sequences[failure]
    asyncio.run(collector.collect_once())
    items = collector.store.list_runs()["items"]
    assert len(items) == 1 and items[0]["uid"] == "existing"
    assert items[0]["disappeared_at"] is None
    assert collector.store.health()["analytics"]["failures"] == 1


def test_active_other_worker_prevents_duplicate_polling(setup):
    collector, api = setup
    lease = collector.store.acquire("other-worker", 30)
    assert not asyncio.run(collector.collect_once())
    api.list_namespaced_pod.assert_not_called()
    collector.store.release(lease)


def test_overall_collection_deadline_cancels_a_stalled_api_call_without_deletion(setup):
    collector, api = setup
    api.list_namespaced_pod.return_value = page([pod()])
    asyncio.run(collector.collect_once())
    cancelled = []

    async def stalled(**_kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.append(True)

    api.list_namespaced_pod.side_effect = stalled
    object.__setattr__(collector.settings, "collection_timeout", 0.02)
    asyncio.run(collector.collect_once())
    assert cancelled == [True]
    assert collector.store.list_runs()["items"][0]["disappeared_at"] is None
    assert collector.store.health()["analytics"]["failures"] == 1


def test_collector_shutdown_cancels_pending_poll_and_releases_lease(setup):
    collector, api = setup

    async def scenario():
        started = asyncio.Event()
        cancelled = []

        async def stalled(**_kwargs):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.append(True)

        api.list_namespaced_pod.side_effect = stalled
        await collector.start()
        await asyncio.wait_for(started.wait(), 1)
        await collector.stop()
        await collector.stop()
        assert cancelled == [True]
        assert collector.task is None
        assert collector.store.acquire("new-process", 30) is not None

    asyncio.run(scenario())


def test_background_loop_recovers_after_store_error_and_stops_cleanly(setup):
    collector, _ = setup
    attempts = []

    async def collect():
        attempts.append(True)
        if len(attempts) == 1:
            raise RuntimeError("database temporarily unavailable")
        collector.stop_event.set()

    collector.collect_once = collect
    object.__setattr__(collector.settings, "interval", 0.001)
    asyncio.run(collector._run())
    assert len(attempts) == 2


def test_lost_lease_cannot_publish_an_inflight_snapshot(setup):
    collector, api = setup
    original = collector.store.snapshot

    def steal(namespace, records, lease):
        collector.store.release(lease)
        collector.store.acquire("other-process", 30)
        original(namespace, records, lease)

    collector.store.snapshot = steal
    api.list_namespaced_pod.return_value = page([pod()])
    assert not asyncio.run(collector.collect_once())
    assert collector.store.list_runs()["items"] == []


def test_metadata_selects_annotated_driver_without_retaining_pod_payload():
    source = pod()
    source.metadata.annotations["kubectl.kubernetes.io/default-container"] = "spark-main"
    source.spec.containers = [
        k8s.V1Container(name="sidecar", image="metrics"),
        k8s.V1Container(name="spark-main", image="approved"),
    ]
    result = metadata_from_pod(source, "analytics")
    assert result["image"] == "approved"
    assert result["created_at"] == "2026-11-07T00:00:00Z"
    assert "annotations" not in result
    source.metadata.labels = {"spark-role": "driver"}
    source.metadata.creation_timestamp = None
    source.spec, source.status = None, None
    result = metadata_from_pod(source, "analytics")
    assert result["image"] is None and result["app_id"] is None
    assert result["name"] == "driver" and result["status"] == "Unknown" and result["created_at"] is None


@pytest.mark.parametrize("field", ["uid", "name", "namespace", "labels"])
def test_incomplete_driver_records_are_not_silently_treated_as_absent(field):
    source = pod()
    setattr(source.metadata, field, None)
    with pytest.raises(ValueError):
        metadata_from_pod(source, "analytics")


def test_collection_budget_does_not_start_more_namespaces_after_expiry(setup):
    collector, api = setup
    object.__setattr__(collector.settings, "namespaces", ("analytics", "other"))
    object.__setattr__(collector.settings, "collection_timeout", 0.02)

    async def blocked(**_kwargs):
        await asyncio.Event().wait()

    api.list_namespaced_pod.side_effect = blocked
    asyncio.run(collector.collect_once())
    assert api.list_namespaced_pod.await_count == 1
    assert set(collector.store.health()) == {"analytics"}


def test_lost_lease_during_failure_recording_does_not_modify_new_owner_statistics(setup):
    collector, api = setup
    original = collector.store.failure

    def steal(namespace, lease):
        collector.store.release(lease)
        collector.store.acquire("replacement", 30)
        original(namespace, lease)

    collector.store.failure = steal
    api.list_namespaced_pod.side_effect = k8s.ApiException(status=503)
    assert not asyncio.run(collector.collect_once())
    assert collector.store.health() == {}


def test_namespace_rotation_prevents_a_slow_namespace_from_starving_others(setup):
    collector, api = setup
    object.__setattr__(collector.settings, "namespaces", ("analytics", "other"))
    object.__setattr__(collector.settings, "collection_timeout", 0.02)

    async def collect(**kwargs):
        if kwargs["namespace"] == "analytics":
            await asyncio.Event().wait()
        return page([pod("other-run", namespace="other")])

    api.list_namespaced_pod.side_effect = collect
    asyncio.run(collector.collect_once())
    assert "other" not in collector.store.health()
    asyncio.run(collector.collect_once())
    assert collector.store.health()["other"]["successes"] == 1
    assert collector.store.list_runs(namespace="other")["items"][0]["uid"] == "other-run"


def test_shutdown_release_failure_does_not_prevent_other_client_cleanup(setup, caplog):
    collector, _ = setup
    collector.lease = collector.store.acquire(collector.owner, 30)
    collector.store.release = Mock(side_effect=OSError("private database path"))
    asyncio.run(collector.stop())
    assert collector.lease is None
    assert "could not be released" in caplog.text
    assert "private database path" not in caplog.text
