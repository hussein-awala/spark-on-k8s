from __future__ import annotations

import asyncio
from unittest import mock

import pytest
from spark_on_k8s.airflow.ui.snapshots import SnapshotCache


class Clock:
    def __init__(self):
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def run(coroutine):
    return asyncio.run(coroutine)


def test_concurrent_reads_share_one_lookup_and_receive_independent_copies():
    calls = []

    async def scenario():
        cache = SnapshotCache(ttl=5)
        release = asyncio.Event()

        async def load():
            calls.append("lookup")
            await release.wait()
            return {"status": "Running"}

        readers = [asyncio.ensure_future(cache.get("task", load)) for _ in range(5)]
        await asyncio.sleep(0)
        release.set()
        results = await asyncio.gather(*readers)
        results[0]["status"] = "changed by one caller"
        return results, await cache.get("task", load)

    results, cached = run(scenario())
    assert calls == ["lookup"]
    assert [result["status"] for result in results[1:]] == ["Running"] * 4
    assert cached == {"status": "Running"}


def test_snapshot_expires_after_its_ttl():
    clock = Clock()
    lookups = mock.AsyncMock(side_effect=[{"status": "Running"}, {"status": "Succeeded"}])

    async def scenario():
        cache = SnapshotCache(ttl=5, clock=clock)
        first = await cache.get("task", lookups)
        clock.now += 4.9
        reused = await cache.get("task", lookups)
        clock.now += 0.1
        return first, reused, await cache.get("task", lookups)

    assert run(scenario()) == ({"status": "Running"}, {"status": "Running"}, {"status": "Succeeded"})
    assert lookups.await_count == 2


def test_failed_lookup_is_shared_by_concurrent_readers_but_not_retained():
    lookups = []

    async def scenario():
        cache = SnapshotCache(ttl=5)
        release = asyncio.Event()

        async def failing():
            lookups.append("failed")
            await release.wait()
            raise RuntimeError("service unavailable")

        readers = [asyncio.ensure_future(cache.get("task", failing)) for _ in range(3)]
        await asyncio.sleep(0)
        release.set()
        results = await asyncio.gather(*readers, return_exceptions=True)
        await asyncio.sleep(0)
        assert len(cache) == 0

        async def recovered():
            lookups.append("recovered")
            return "Running"

        return results, await cache.get("task", recovered)

    results, recovered = run(scenario())
    assert all(isinstance(result, RuntimeError) for result in results)
    assert recovered == "Running"
    assert lookups == ["failed", "recovered"]


def test_a_disconnected_reader_does_not_cancel_the_shared_lookup():
    async def scenario():
        cache = SnapshotCache(ttl=5)
        release = asyncio.Event()
        lookups = []

        async def load():
            lookups.append("lookup")
            await release.wait()
            return "Running"

        first = asyncio.ensure_future(cache.get("task", load))
        second = asyncio.ensure_future(cache.get("task", load))
        await asyncio.sleep(0)
        first.cancel()
        release.set()
        assert await second == "Running"
        with pytest.raises(asyncio.CancelledError):
            await first
        assert await cache.get("task", load) == "Running"
        return lookups

    assert run(scenario()) == ["lookup"]


def test_invalidation_discards_a_snapshot_and_an_inflight_result():
    async def scenario():
        cache = SnapshotCache(ttl=5)
        release = asyncio.Event()

        async def before_action():
            await release.wait()
            return "Running"

        reader = asyncio.ensure_future(cache.get("task", before_action))
        await asyncio.sleep(0)
        cache.invalidate("task")
        release.set()
        assert await reader == "Running"
        await asyncio.sleep(0)
        return await cache.get("task", mock.AsyncMock(return_value="Killed"))

    assert run(scenario()) == "Killed"


def test_cache_is_bounded_and_drops_expired_entries():
    clock = Clock()

    async def scenario():
        cache = SnapshotCache(ttl=5, max_entries=3, clock=clock)
        for key in range(5):
            await cache.get(key, mock.AsyncMock(return_value=key))
            await asyncio.sleep(0)
        assert len(cache) == 3
        clock.now += 5
        await cache.get("new", mock.AsyncMock(return_value="new"))
        return len(cache)

    assert run(scenario()) == 1


def test_zero_ttl_disables_sharing():
    lookups = mock.AsyncMock(return_value="Running")

    async def scenario():
        cache = SnapshotCache(ttl=0)
        await asyncio.gather(*(cache.get("task", lookups) for _ in range(3)))
        return len(cache)

    assert run(scenario()) == 0
    assert lookups.await_count == 3


def test_binding_without_task_context_reads_the_driver_without_the_cache(monkeypatch):
    from spark_on_k8s.airflow.ui import backend
    from spark_on_k8s.airflow.ui.task_binding import TaskBinding

    binding = TaskBinding(version=1, namespace="spark", driver_pod="app-driver", try_number=1)
    snapshots = mock.Mock(spec=SnapshotCache)
    connection = mock.Mock()
    connection.app.state.snapshots = snapshots
    application_for = mock.AsyncMock(return_value="current application")
    monkeypatch.setattr(backend, "application_for", application_for)

    assert backend.snapshot_key(binding) is None
    assert run(backend.snapshot_for(connection, binding)) == "current application"
    application_for.assert_awaited_once_with(connection, binding)
    snapshots.get.assert_not_called()
