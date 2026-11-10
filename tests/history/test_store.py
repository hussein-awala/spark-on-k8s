from __future__ import annotations

import base64
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest
from spark_on_k8s.history.store import LostLease, RunStore


def record(uid="one", namespace="analytics", status="Running"):
    return {
        "uid": uid,
        "namespace": namespace,
        "app_id": "report",
        "name": "Report",
        "driver_pod": "report-driver",
        "image": "spark:image",
        "spark_version": "4.0.3",
        "status": status,
        "created_at": "2026-11-07T12:00:00Z",
    }


@pytest.fixture
def database(tmp_path):
    clock = [1700000000.0]
    store = RunStore(tmp_path / "runs.sqlite3", clock=lambda: clock[0])
    store.initialize()
    lease = store.acquire("worker", 100)
    return store, clock, lease


def test_restart_preserves_metadata_and_reused_pod_names_have_distinct_identities(database):
    store, clock, lease = database
    store.snapshot("analytics", [record("old", status="Succeeded")], lease)
    clock[0] += 1
    store.snapshot("analytics", [record("new")], lease)
    reopened = RunStore(store.path)
    reopened.initialize()
    items = reopened.list_runs()["items"]
    assert [row["uid"] for row in items] == ["new", "old"]
    assert items[0]["driver_pod"] == items[1]["driver_pod"]
    assert items[0]["disappeared_at"] is None
    assert items[1]["disappeared_at"] is not None
    assert items[1]["status"] == "Succeeded"
    assert set(items[0]) == {
        "uid",
        "namespace",
        "app_id",
        "name",
        "driver_pod",
        "image",
        "spark_version",
        "status",
        "created_at",
        "first_seen",
        "last_seen",
        "disappeared_at",
    }
    with sqlite3.connect(store.path) as db:
        assert db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert db.execute("PRAGMA user_version").fetchone()[0] == 1


def test_missing_running_pod_is_not_failed_and_reappearance_clears_missing(database):
    store, clock, lease = database
    store.snapshot("analytics", [record()], lease)
    first = store.list_runs()["items"][0]
    clock[0] += 1
    store.snapshot("analytics", [], lease)
    missing = store.list_runs(presence="missing")["items"][0]
    assert missing["status"] == "Running"
    assert missing["last_seen"] == first["last_seen"]
    clock[0] += 1
    store.snapshot("analytics", [record(status="Succeeded")], lease)
    present = store.list_runs(presence="present")["items"][0]
    assert present["first_seen"] == first["first_seen"]
    assert present["last_seen"] != first["last_seen"]
    assert present["disappeared_at"] is None
    assert present["status"] == "Succeeded"
    store.snapshot("analytics", [record(status="Unknown")], lease)
    assert store.list_runs()["items"][0]["status"] == "Succeeded"


def test_failed_collection_never_marks_existing_records_missing(database):
    store, clock, lease = database
    store.snapshot("analytics", [record()], lease)
    clock[0] += 5
    store.failure("analytics", lease)
    assert store.list_runs()["items"][0]["disappeared_at"] is None
    health = store.health()["analytics"]
    assert health["successes"] == health["failures"] == 1
    assert health["last_success"] < health["last_attempt"]
    store.failure("other", lease)
    assert store.health()["other"]["last_success"] is None


def test_failure_category_is_kept_until_the_next_successful_snapshot(database):
    store, clock, lease = database
    store.failure("analytics", lease, "forbidden")
    assert store.health()["analytics"]["last_error"] == "forbidden"
    clock[0] += 1
    store.failure("analytics", lease, "timeout")
    assert store.health()["analytics"]["last_error"] == "timeout"
    store.snapshot("analytics", [record()], lease)
    health = store.health()["analytics"]
    assert health["last_error"] is None and health["failures"] == 2 and health["successes"] == 1


def test_earlier_version_one_database_gains_the_failure_category_column(tmp_path):
    path = tmp_path / "earlier.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("""CREATE TABLE collection (
            namespace TEXT PRIMARY KEY, last_attempt REAL, last_success REAL,
            successes INTEGER NOT NULL DEFAULT 0, failures INTEGER NOT NULL DEFAULT 0
        )""")
        db.execute("INSERT INTO collection VALUES('analytics', 1, 1, 3, 0)")
        db.execute("PRAGMA user_version=1")
    store = RunStore(path)
    store.initialize()
    store.initialize()
    assert store.health()["analytics"] == {
        "namespace": "analytics",
        "last_attempt": 1,
        "last_success": 1,
        "successes": 3,
        "failures": 0,
        "last_error": None,
    }
    store.failure("analytics", store.acquire("worker", 30), "throttled")
    assert store.health()["analytics"]["last_error"] == "throttled"
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 1


def test_namespace_snapshots_do_not_delete_other_namespaces(database):
    store, _, lease = database
    store.snapshot("analytics", [record()], lease)
    store.snapshot("other", [record("other-run", "other")], lease)
    store.snapshot("analytics", [], lease)
    assert store.list_runs(namespace="other", presence="present")["items"][0]["uid"] == "other-run"
    assert [row["uid"] for row in store.list_runs(allowed_namespaces=("analytics",))["items"]] == ["one"]


def test_partial_or_duplicate_snapshots_roll_back_all_writes(database):
    store, _, lease = database
    store.snapshot("analytics", [record("existing")], lease)
    with pytest.raises(sqlite3.IntegrityError):
        store.snapshot("analytics", [record("new"), record("new")], lease)
    assert [row["uid"] for row in store.list_runs()["items"]] == ["existing"]
    assert store.health()["analytics"]["successes"] == 1
    assert store.list_runs()["items"][0]["disappeared_at"] is None
    with pytest.raises(ValueError, match="Invalid run metadata"):
        store.snapshot("analytics", [record("new", "other")], lease)


def test_expired_or_replaced_lease_cannot_write_or_release_current_owner(database):
    store, clock, first = database
    assert store.acquire("other", 100) is None
    clock[0] += 101
    second = store.acquire("other", 100)
    with pytest.raises(LostLease):
        store.snapshot("analytics", [record()], first)
    with pytest.raises(LostLease):
        store.failure("analytics", first)
    store.release(first)
    assert store.acquire("third", 100) is None
    store.snapshot("analytics", [record()], second)
    store.release(second)
    assert store.acquire("third", 100) is not None


def test_same_owner_new_token_fences_previous_generation(database):
    store, _, old = database
    newer = store.acquire("worker", 100)
    assert newer.token > old.token
    with pytest.raises(LostLease):
        store.snapshot("analytics", [], old)
    store.snapshot("analytics", [record()], newer)


def test_lease_expiring_during_write_rolls_back_before_commit(database):
    store, clock, lease = database
    original = store._fence
    calls = []

    def fence(db, checked):
        calls.append(True)
        if len(calls) == 2:
            clock[0] += 101
        original(db, checked)

    store._fence = fence
    with pytest.raises(LostLease):
        store.snapshot("analytics", [record()], lease)
    assert store.list_runs()["items"] == []
    assert store.health() == {}


def test_concurrent_workers_have_only_one_lease_winner_and_no_stale_writes(database):
    store, clock, lease = database
    store.release(lease)
    with ThreadPoolExecutor(max_workers=8) as workers:
        leases = list(workers.map(lambda owner: store.acquire(owner, 30), [str(i) for i in range(8)]))
    winners = [value for value in leases if value is not None]
    assert len(winners) == 1
    store.snapshot("analytics", [record()], winners[0])
    assert store.list_runs()["items"][0]["uid"] == "one"


def test_retention_and_max_rows_bound_old_records(database):
    store, clock, lease = database
    store.retention_seconds, store.max_rows = 10, 2
    store.snapshot("analytics", [record("old")], lease)
    clock[0] += 11
    store.snapshot("analytics", [record("a"), record("b"), record("c")], lease)
    assert [row["uid"] for row in store.list_runs()["items"]] == ["c", "b"]
    assert store.counts() == [{"namespace": "analytics", "status": "Running", "count": 2}]


def test_keyset_pagination_remains_stable_when_older_runs_are_updated(database):
    store, clock, lease = database
    for uid in ("a", "b", "c"):
        store.snapshot("analytics", [record(uid)], lease)
        clock[0] += 1
    page = store.list_runs(limit=1)
    assert page["items"][0]["uid"] == "c"
    store.snapshot("analytics", [record("a", status="Succeeded"), record("new")], lease)
    next_page = store.list_runs(limit=1, cursor=page["next_cursor"])
    assert next_page["items"][0]["uid"] == "b"
    final_page = store.list_runs(limit=1, cursor=next_page["next_cursor"])
    assert final_page["items"][0]["uid"] == "a"
    assert final_page["next_cursor"] is None
    assert store.list_runs(status="Succeeded")["items"][0]["uid"] == "a"
    with pytest.raises(ValueError, match="cursor"):
        store.list_runs(status="Succeeded", cursor=page["next_cursor"])


@pytest.mark.parametrize(
    "cursor",
    [
        "bad base64",
        base64.urlsafe_b64encode(b"{}").decode(),
        base64.urlsafe_b64encode(b"[]").decode(),
        base64.urlsafe_b64encode(b'{"first":NaN,"uid":"x","filters":[null,null,"all",null]}').decode(),
        # An integer beyond float range cannot be a first-observed timestamp.
        base64.urlsafe_b64encode(
            b'{"first":1' + b"0" * 400 + b',"uid":"x","filters":[null,null,"all",null]}'
        ).decode(),
    ],
)
def test_invalid_cursors_are_rejected(database, cursor):
    with pytest.raises(ValueError, match="cursor"):
        database[0].list_runs(cursor=cursor)


def test_integer_cursor_position_beyond_sqlite_integer_range_is_bound_as_real(database):
    store, _, lease = database
    store.snapshot("analytics", [record("one")], lease)
    payload = b'{"first":1180591620717411303424,"uid":"x","filters":[null,null,"all",null]}'
    page = store.list_runs(cursor=base64.urlsafe_b64encode(payload).decode())
    assert [item["uid"] for item in page["items"]] == ["one"]


@pytest.mark.parametrize("options", [{"status": "Deleted"}, {"presence": "deleted"}, {"limit": 0}])
def test_invalid_filters_are_rejected(database, options):
    with pytest.raises(ValueError, match="filter"):
        database[0].list_runs(**options)


def test_future_schema_is_not_silently_rewritten(tmp_path):
    path = tmp_path / "future.sqlite3"
    with sqlite3.connect(path) as db:
        db.execute("PRAGMA user_version=9")
    with pytest.raises(ValueError, match="schema version"):
        RunStore(path).initialize()
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 9


def test_retention_is_enforced_even_when_kubernetes_remains_unavailable(database):
    store, clock, lease = database
    store.retention_seconds = 10
    store.snapshot("analytics", [record()], lease)
    clock[0] += 11
    store.failure("analytics", lease)
    assert store.list_runs()["items"] == []
    assert store.health()["analytics"]["failures"] == 1


def test_concurrent_worker_initialization_is_safe(tmp_path):
    import threading

    path = tmp_path / "shared.sqlite3"
    barrier = threading.Barrier(8)

    def initialize(_):
        barrier.wait(timeout=3)
        RunStore(path).initialize()

    with ThreadPoolExecutor(max_workers=8) as workers:
        list(workers.map(initialize, range(8)))
    assert RunStore(path).list_runs()["items"] == []


def test_initialization_recovers_from_an_existing_reader_without_corrupting_database(tmp_path, monkeypatch):
    import threading
    import time

    from spark_on_k8s.history import store as store_module

    path = tmp_path / "busy.sqlite3"
    reader = sqlite3.connect(path)
    reader.execute("CREATE TABLE existing(value TEXT)")
    reader.commit()
    reader.execute("BEGIN")
    reader.execute("SELECT * FROM existing").fetchall()
    retrying = threading.Event()
    real_sleep = time.sleep

    def sleep(seconds):
        retrying.set()
        real_sleep(seconds)

    monkeypatch.setattr(store_module.time, "sleep", sleep)
    try:
        with ThreadPoolExecutor(max_workers=1) as workers:
            future = workers.submit(RunStore(path).initialize)
            assert retrying.wait(timeout=2)
            reader.rollback()
            future.result(timeout=3)
    finally:
        reader.close()
    assert RunStore(path).list_runs()["items"] == []


def test_unavailable_database_directory_fails_without_retrying(tmp_path):
    with pytest.raises(sqlite3.OperationalError):
        RunStore(tmp_path / "missing" / "runs.sqlite3").initialize()
