from __future__ import annotations

import json
import signal
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from click.testing import CliRunner
from spark_on_k8s_dbt.cli import main
from spark_on_k8s_dbt.config import ComputeConfig
from spark_on_k8s_dbt.lifecycle import Endpoint, InvocationOwner, get_invocation_owner, invocation_scope


def compute(tmp_path):
    kube = tmp_path / "kubeconfig"
    kube.touch()
    return {
        "kube_config_file": str(kube),
        "kube_context": "test",
        "image": "spark:test",
        "server_jar": "local:///opt/spark/server.jar",
        "namespace": "test",
        "service_account": "spark",
        "spark_conf": {
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.warehouse.dir": "hdfs://h/warehouse",
            "spark.hadoop.hive.metastore.uris": "thrift://h:9083",
        },
    }


@pytest.mark.parametrize(
    "field,value",
    [
        ("startup_timeout", True),
        ("cleanup_timeout", float("inf")),
        ("poll_interval", 0),
        ("cleanup_timeout", 3601),
        ("startup_timeout", 10**20),
        ("in_cluster", "false"),
        ("max_runtime", 0),
        ("max_runtime", 1.5),
    ],
)
def test_reject_invalid_runtime_before_submission(tmp_path, field, value):
    config = compute(tmp_path)
    config[field] = value
    with pytest.raises(ValueError):
        ComputeConfig.parse("connect", config)


POD_SETTINGS = {
    "image_pull_secrets": ["private-registry"],
    "driver_env_from_secrets": {"API_TOKEN": "dbt-credentials:api-token"},
    "executor_env_from_secrets": {"API_TOKEN": "dbt-credentials:api-token"},
    "executor_node_selector": {"pool": "batch"},
    "driver_tolerations": [{"key": "dedicated", "value": "dbt", "effect": "NoSchedule"}],
    "executor_tolerations": [{"key": "spot", "operator": "Exists"}],
}


def test_connect_compute_passes_pod_settings_to_the_server_config(tmp_path):
    server = ComputeConfig.parse("connect", {**compute(tmp_path), **POD_SETTINGS}).server
    for name, value in POD_SETTINGS.items():
        assert getattr(server, name) == value


@pytest.mark.parametrize("name", sorted(POD_SETTINGS))
def test_thrift_compute_rejects_unmapped_pod_settings_before_cluster_access(tmp_path, name):
    with pytest.raises(ValueError, match=f"does not support.*{name}"):
        ComputeConfig.parse("thrift", {**compute(tmp_path), name: POD_SETTINGS[name]})


@pytest.mark.parametrize("method", ["connect", "thrift"])
def test_max_runtime_is_an_optional_server_lifetime(tmp_path, method):
    values = compute(tmp_path)
    assert ComputeConfig.parse(method, values).server.max_runtime is None
    values["max_runtime"] = 7200
    assert ComputeConfig.parse(method, values).server.max_runtime == 7200


def test_explicit_cluster_and_persistent_catalog_required(tmp_path):
    values = compute(tmp_path)
    for field in ("kube_context", "kube_config_file"):
        bad = dict(values)
        del bad[field]
        with pytest.raises(ValueError, match="explicit"):
            ComputeConfig.parse("thrift", bad)
    for field in ("spark.sql.warehouse.dir", "spark.hadoop.hive.metastore.uris"):
        bad = {**values, "spark_conf": {k: v for k, v in values["spark_conf"].items() if k != field}}
        with pytest.raises(ValueError):
            ComputeConfig.parse("connect", bad)


def test_scope_is_visible_to_workers_and_cleared_after_failure():
    owner = object()
    with pytest.raises(RuntimeError, match="boom"), invocation_scope(owner):
        with ThreadPoolExecutor(2) as pool:
            assert list(pool.map(lambda _: get_invocation_owner(), range(2))) == [owner, owner]
        with pytest.raises(RuntimeError, match="separate processes"), invocation_scope(object()):
            pass
        raise RuntimeError("boom")
    assert get_invocation_owner() is None
    with invocation_scope(object()):
        assert get_invocation_owner() is not owner


def test_receipt_reserved_exclusively_with_private_permissions(tmp_path):
    owner = InvocationOwner(tmp_path / "receipt")
    assert owner.directory.stat().st_mode & 0o777 == 0o700
    assert owner.receipt.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        InvocationOwner(owner.directory)
    owner.close(dbt_exit_code=0)
    assert json.loads(owner.receipt.read_text())["cleanup"]["status"] == "not_started"


def test_compute_identity_cannot_change_or_retry_uncertain_creation(tmp_path):
    owner = InvocationOwner(tmp_path / "receipt")
    owner._inputs = ("connect", {"key": "one"})
    with pytest.raises(RuntimeError, match="already failed"):
        owner.ensure_endpoint("connect", {"key": "one"})
    with pytest.raises(ValueError, match="cannot mix"):
        owner.ensure_endpoint("thrift", {"key": "one"})
    owner.endpoint = Endpoint("127.0.0.1", 100)
    assert owner.ensure_endpoint("connect", {"key": "one"}) == owner.endpoint


def test_same_owner_serializes_worker_startup(tmp_path):
    owner = InvocationOwner(tmp_path / "receipt")
    owner._inputs = ("connect", {})
    owner.endpoint = Endpoint("host", 100)
    with ThreadPoolExecutor(4) as pool:
        endpoints = list(pool.map(lambda _: owner.ensure_endpoint("connect", {}), range(16)))
    assert all(endpoint is owner.endpoint for endpoint in endpoints)


def test_partial_submission_is_retained_and_tunnel_closes(tmp_path):
    owner = InvocationOwner(tmp_path / "receipt")
    owner.state["run"] = {"driver_uid": "confirmed"}
    owner.forward = Mock()
    with pytest.raises(RuntimeError, match="cleanup failed"):
        owner.close(dbt_exit_code=1)
    owner.forward.close.assert_called_once()
    state = json.loads(owner.receipt.read_text())
    assert state["stage"] == "cleanup_failed" and state["dbt_exit_code"] == 1
    assert state["run"]["driver_uid"] == "confirmed"
    with pytest.raises(ValueError, match="complete server identity"):
        InvocationOwner.recover(owner.receipt)


def test_namespace_replacement_prevents_deletion_and_preserves_original_failure(tmp_path):
    owner = InvocationOwner(tmp_path / "receipt")
    owner.state.update(method="connect", server={"run": {"driver_uid": "old"}})
    owner.sdk = Mock()
    owner._namespace = Mock(side_effect=RuntimeError("changed"))
    with pytest.raises(RuntimeError, match="cleanup failed"):
        owner.close(dbt_exit_code=1)
    owner.sdk.delete_connect_server.assert_not_called()
    assert json.loads(owner.receipt.read_text())["dbt_exit_code"] == 1


@pytest.mark.parametrize(
    "success,exception,expected", [(True, None, 0), (False, None, 1), (False, ValueError("invalid"), 2)]
)
def test_wrapper_returns_dbt_status_and_always_cleans(tmp_path, monkeypatch, success, exception, expected):
    import dbt.cli.main

    runner = Mock()
    runner.invoke.return_value = SimpleNamespace(success=success, exception=exception)
    monkeypatch.setattr(dbt.cli.main, "dbtRunner", lambda: runner)
    result = CliRunner().invoke(main, ["run", "--receipt-dir", str(tmp_path / "run"), "--", "build"])
    assert result.exit_code == expected
    assert runner.invoke.call_args.args[0] == ["build"]
    assert json.loads((tmp_path / "run/receipt.json").read_text())["dbt_exit_code"] == expected
    assert get_invocation_owner() is None


def test_unusable_receipt_directory_is_an_invocation_error_not_a_dbt_failure(tmp_path, monkeypatch):
    import dbt.cli.main

    runner = Mock()
    monkeypatch.setattr(dbt.cli.main, "dbtRunner", lambda: runner)
    existing = tmp_path / "existing"
    existing.mkdir()
    result = CliRunner().invoke(main, ["run", "--receipt-dir", str(existing), "--", "build"])
    assert result.exit_code == 2, result.exception
    assert "Managed dbt failed" in result.output
    runner.invoke.assert_not_called()
    assert list(existing.iterdir()) == []


def test_wrapper_reports_cleanup_failure_and_keeps_dbt_failure(tmp_path, monkeypatch):
    import dbt.cli.main

    def invoke(args):
        get_invocation_owner().state["run"] = {"driver_uid": None}
        return SimpleNamespace(success=False, exception=None)

    monkeypatch.setattr(dbt.cli.main, "dbtRunner", lambda: SimpleNamespace(invoke=invoke))
    result = CliRunner().invoke(main, ["run", "--receipt-dir", str(tmp_path / "run"), "--", "build"])
    assert result.exit_code == 1
    assert "cleanup failed" in result.output


def test_wrapper_interrupt_restores_signals_and_runs_cleanup(tmp_path, monkeypatch):
    import dbt.cli.main

    previous = signal.getsignal(signal.SIGINT)

    def invoke(args):
        signal.raise_signal(signal.SIGINT)

    monkeypatch.setattr(dbt.cli.main, "dbtRunner", lambda: SimpleNamespace(invoke=invoke))
    result = CliRunner().invoke(main, ["run", "--receipt-dir", str(tmp_path / "run"), "--", "build"])
    assert result.exit_code == 130
    assert signal.getsignal(signal.SIGINT) == previous
    assert json.loads((tmp_path / "run/receipt.json").read_text())["dbt_exit_code"] == 130


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
def test_interrupt_stops_startup_waits_while_dbt_still_joins_its_workers(tmp_path, monkeypatch, signum):
    import time

    import dbt.cli.main

    observed = []

    def invoke(args):
        owner = get_invocation_owner()
        try:
            signal.raise_signal(signum)
        except KeyboardInterrupt:
            # dbt now cancels connections and joins workers before returning. A
            # worker still provisioning must see cancellation, not wait for readiness.
            with pytest.raises(RuntimeError, match="startup was cancelled"):
                owner._remaining(time.monotonic() + 60)
            observed.append(signum)
            raise

    monkeypatch.setattr(dbt.cli.main, "dbtRunner", lambda: SimpleNamespace(invoke=invoke))
    result = CliRunner().invoke(main, ["run", "--receipt-dir", str(tmp_path / "run"), "--", "build"])
    assert observed == [signum]
    assert result.exit_code == 128 + signum


def test_transitive_configmap_ownership_survives_executor_disappearance(tmp_path, monkeypatch):
    from contextlib import nullcontext

    from spark_on_k8s.connect_server import ConnectServerHandle
    from spark_on_k8s.utils.run import RunHandle
    from spark_on_k8s_dbt import lifecycle

    owner = InvocationOwner(tmp_path / "receipt")
    handle = ConnectServerHandle(
        run=RunHandle(namespace="test", app_id="app", driver_pod="app-driver", driver_uid="driver"),
        service_name="app-connect",
        service_uid="service",
        driver_service_uid="headless",
        port=15002,
    )

    def obj(name, uid, parent):
        return SimpleNamespace(
            metadata=SimpleNamespace(
                name=name, uid=uid, labels={}, owner_references=[SimpleNamespace(uid=parent)]
            )
        )

    api = Mock()
    executor = obj("executor", "executor-uid", "driver")
    configmap = obj("config", "config-uid", "executor-uid")
    api.list_namespaced_pod.return_value.items = [executor]
    api.list_namespaced_service.return_value.items = []
    api.list_namespaced_config_map.return_value.items = [configmap]
    owner.sdk = Mock()
    owner.sdk.k8s_client_manager.client.side_effect = lambda: nullcontext(object())
    monkeypatch.setattr(lifecycle.k8s, "CoreV1Api", lambda raw: api)
    assert set(owner._connect_remaining(handle)) == {"executor", "config"}
    api.list_namespaced_pod.return_value.items = []
    assert owner._connect_remaining(handle) == ["config"]
    assert "executor-uid" in json.loads(owner.receipt.read_text())["owned_uids"]


def test_invalid_receipt_cannot_claim_absence_in_different_namespace(tmp_path):
    owner = InvocationOwner(tmp_path / "receipt")
    owner.state.update(method="connect", namespace="absent", server={"run": {"namespace": "real"}})
    owner._namespace = Mock(return_value=False)
    with pytest.raises(RuntimeError, match="cleanup failed"):
        owner.close()
    owner._namespace.assert_not_called()
    assert owner.state["stage"] == "cleanup_failed"


@pytest.mark.parametrize(
    "warehouse", ["", "/tmp/warehouse", "file:///tmp/warehouse", "local:///tmp/warehouse", "hdfs://host"]
)
def test_reject_driver_local_warehouse_before_provisioning(tmp_path, warehouse):
    owner = InvocationOwner(tmp_path / "receipt")
    settings = compute(tmp_path)
    settings["spark_conf"]["spark.sql.warehouse.dir"] = warehouse
    with pytest.raises(ValueError, match="remote filesystem URI"):
        owner.ensure_endpoint("connect", settings)
    assert owner.sdk is None and "run" not in owner.state
    owner.close()


def test_stalled_startup_does_not_block_cleanup_indefinitely(tmp_path):
    import threading

    owner = InvocationOwner(tmp_path / "receipt")
    owner.state["cleanup_timeout"] = 0.02
    acquired, release = threading.Event(), threading.Event()

    def hold_startup_lock():
        with owner._lock:
            acquired.set()
            release.wait(5)

    thread = threading.Thread(target=hold_startup_lock)
    thread.start()
    assert acquired.wait(2)
    try:
        with pytest.raises(RuntimeError, match="still busy"):
            owner.close(dbt_exit_code=143)
        assert owner.state["cleanup"] == {"status": "failed", "error": "owner_busy"}
        assert owner.state["dbt_exit_code"] == 143
    finally:
        release.set()
        thread.join(2)
    assert not thread.is_alive()


def test_cleanup_stops_a_pending_driver_wait_instead_of_leaking_busy_compute(tmp_path, monkeypatch):
    import threading
    import time
    from contextlib import nullcontext

    from spark_on_k8s_dbt import lifecycle

    owner = InvocationOwner(tmp_path / "receipt")
    owner.state.update(cleanup_timeout=2, server={"run": {"driver_uid": "driver"}})
    owner.config = SimpleNamespace(poll_interval=0.01, server=SimpleNamespace(namespace="test"))
    owner.sdk = Mock()
    owner.sdk.k8s_client_manager.client.side_effect = lambda: nullcontext(object())
    api = Mock()
    polled = threading.Event()

    def pending(*args, **kwargs):
        polled.set()
        return SimpleNamespace(
            metadata=SimpleNamespace(uid="driver", deletion_timestamp=None),
            status=SimpleNamespace(phase="Pending"),
        )

    api.read_namespaced_pod.side_effect = pending
    monkeypatch.setattr(lifecycle.k8s, "CoreV1Api", lambda raw: api)
    owner._validate_identity = Mock()
    owner._namespace = Mock(return_value=False)
    failures = []

    def startup():
        # A worker still waiting for an image pull holds the owner lock.
        with owner._lock:
            try:
                owner._wait_running("app-driver", time.monotonic() + 30)
            except BaseException as error:
                failures.append(error)

    thread = threading.Thread(target=startup, daemon=True)
    thread.start()
    try:
        assert polled.wait(2)
        started = time.monotonic()
        owner.close(dbt_exit_code=130)
        assert time.monotonic() - started < 1
    finally:
        thread.join(5)
    assert not thread.is_alive()
    assert len(failures) == 1 and "cancelled" in str(failures[0])
    state = json.loads(owner.receipt.read_text())
    assert state["stage"] == "cleaned" and state["dbt_exit_code"] == 130
    owner._namespace.assert_called_once_with()


def test_thrift_cleanup_polls_at_the_configured_interval(tmp_path):
    owner = InvocationOwner(tmp_path / "receipt")
    server = {"run": {"namespace": "test", "driver_uid": "driver"}}
    owner.state.update(method="thrift", server=server, cleanup_timeout=30, poll_interval=7)
    owner.sdk = Mock()
    owner.backend = Mock()
    owner._validate_identity = Mock()
    owner._namespace = Mock(return_value=True)
    owner.close(dbt_exit_code=0)
    owner.backend.delete.assert_called_once_with(server)
    # Connect cleanup already honours compute.poll_interval; Thrift must too.
    owner.backend.wait_deleted.assert_called_once_with(server, timeout=30, poll_interval=7)
    assert json.loads(owner.receipt.read_text())["stage"] == "cleaned"
