from __future__ import annotations

import json
import os
import sys
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from spark_on_k8s_dbt.forward import PortForward
from spark_on_k8s_dbt.lifecycle import Endpoint, InvocationOwner


@pytest.fixture
def fake_kubectl(tmp_path, monkeypatch):
    """A real subprocess whose first tunnel disappears after announcing its local listener."""
    executable = tmp_path / "kubectl"
    executable.write_text(
        f"#!{sys.executable}\n"
        "import json, os, pathlib, sys, time\n"
        "directory = pathlib.Path(os.environ['FAKE_KUBECTL_STATE'])\n"
        "counter = directory / 'count'\n"
        "number = int(counter.read_text()) + 1 if counter.exists() else 1\n"
        "counter.write_text(str(number))\n"
        "(directory / f'argv-{number}.json').write_text(json.dumps(sys.argv[1:]))\n"
        "print(f'Forwarding from 127.0.0.1:{41000 + number} -> 10000', flush=True)\n"
        "if number == 1:\n"
        "    time.sleep(0.2)\n"
        "    sys.exit(1)\n"
        "while True:\n"
        "    time.sleep(0.1)\n"
    )
    executable.chmod(0o700)
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("FAKE_KUBECTL_STATE", str(tmp_path))
    return tmp_path


def test_refresh_restarts_exited_real_tunnel_and_closes_each_process(tmp_path, fake_kubectl):
    config = SimpleNamespace(kube_config_file="/explicit/kubeconfig", kube_context="explicit-context")
    forward = PortForward(config, "owned-namespace", "owned-driver", 10000)
    owner = InvocationOwner(tmp_path / "receipt")
    owner.forward = forward
    first = second = None
    try:
        first_port = forward.start(timeout=3)
        first = forward.process
        assert first_port == 41001
        assert first.wait(timeout=3) == 1
        second_port = owner._refresh_tunnel(first_port, time.monotonic() + 3)
        second = forward.process
        assert second_port == 41002 and second.pid != first.pid
        assert first.stdout.closed and second.poll() is None
        argv = json.loads((fake_kubectl / "argv-2.json").read_text())
        assert argv == [
            "--kubeconfig",
            "/explicit/kubeconfig",
            "--context",
            "explicit-context",
            "--namespace",
            "owned-namespace",
            "port-forward",
            "--address",
            "127.0.0.1",
            "pod/owned-driver",
            ":10000",
        ]
    finally:
        forward.close()
    assert first.poll() is not None
    assert second is not None and second.poll() is not None
    assert second.stdout.closed and not forward.reader.is_alive()


def test_terminal_interrupt_to_the_wrapper_group_does_not_reach_the_tunnel(tmp_path, fake_kubectl):
    (fake_kubectl / "count").write_text("1")  # Skip the fixture's self-exiting first tunnel.
    config = SimpleNamespace(kube_config_file="/explicit/kubeconfig", kube_context="explicit-context")
    forward = PortForward(config, "owned-namespace", "owned-driver", 10000)
    try:
        assert forward.start(timeout=3) == 41002
        # Ctrl-C signals the terminal's whole foreground process group. dbt's query
        # cancellation and session cleanup still use this tunnel afterwards.
        assert os.getpgid(forward.process.pid) != os.getpgid(0)
        assert forward.process.poll() is None
    finally:
        forward.close()
    assert forward.process.poll() is not None and forward.process.stdout.closed


def test_refresh_keeps_a_running_tunnel(tmp_path):
    owner = InvocationOwner(tmp_path / "receipt")
    owner.forward = Mock()
    owner.forward.process.poll.return_value = None
    assert owner._refresh_tunnel(41000, time.monotonic() + 3) == 41000
    owner.forward.start.assert_not_called()
    owner.forward.close.assert_not_called()


def test_refresh_without_local_tunnel_preserves_private_service_port(tmp_path):
    owner = InvocationOwner(tmp_path / "receipt")
    assert owner._refresh_tunnel(10000, time.monotonic() + 3) == 10000


def ready_owner(tmp_path, method):
    """An owner whose server became ready through a tunnel that kubectl later left."""
    owner = InvocationOwner(tmp_path / "receipt")
    owner._inputs = (method, {"compute": "fixture"})
    owner.state.update(method=method, server={"run": {"driver_pod": "owned-driver", "driver_uid": "uid"}})
    owner.config = SimpleNamespace(
        startup_timeout=30, poll_interval=0.01, in_cluster=False, server=SimpleNamespace(namespace="owned")
    )
    config = SimpleNamespace(kube_config_file="/explicit/kubeconfig", kube_context="explicit-context")
    owner.forward = PortForward(config, "owned", "owned-driver", 10000)
    owner.endpoint = Endpoint("127.0.0.1", owner.forward.start(timeout=3))
    owner.forward.process.terminate()
    owner.forward.process.wait(timeout=3)
    owner._wait_running = Mock()
    owner._wait_thrift = Mock(side_effect=lambda host, port, deadline: port)
    owner.backend = Mock()
    owner.backend.status.return_value = {"status": "running"}
    owner.sdk = Mock()
    return owner


@pytest.mark.parametrize("method", ["thrift", "connect"])
def test_ready_endpoint_reopens_an_exited_tunnel_after_reverifying_the_server(tmp_path, fake_kubectl, method):
    (fake_kubectl / "count").write_text("1")  # Skip the fixture's self-exiting first tunnel.
    owner = ready_owner(tmp_path, method)
    dead = owner.forward.process
    try:
        # kubectl can exit mid-run; later dbt connections must not get a dead port.
        endpoint = owner.ensure_endpoint(method, {"compute": "fixture"})
        assert endpoint.port == 41003 and owner.endpoint is endpoint
        assert owner.forward.process is not dead and owner.forward.process.poll() is None
        assert dead.stdout.closed
        owner._wait_running.assert_called_once()
        assert owner._wait_running.call_args.args[0] == "owned-driver"
        if method == "thrift":
            # The probe checks spark.app.id through the new tunnel.
            assert owner._wait_thrift.call_args.args[:2] == ("127.0.0.1", 41003)
            assert endpoint.connect_profile is None
        else:
            ready = owner.sdk.wait_connect_server_ready.call_args.kwargs
            assert ready["profile"].endpoint == endpoint.connect_profile.endpoint == "sc://127.0.0.1:41003"
        # A live tunnel is reused without new verification.
        assert owner.ensure_endpoint(method, {"compute": "fixture"}) is endpoint
        owner._wait_running.assert_called_once()
    finally:
        owner.forward.close()


def test_exited_tunnel_is_not_reopened_to_a_changed_driver(tmp_path, fake_kubectl):
    (fake_kubectl / "count").write_text("1")
    owner = ready_owner(tmp_path, "thrift")
    owner._wait_running.side_effect = RuntimeError("Driver identity changed during startup")
    try:
        with pytest.raises(RuntimeError, match="identity changed"):
            owner.ensure_endpoint("thrift", {"compute": "fixture"})
        assert (fake_kubectl / "count").read_text() == "2"  # No new kubectl process.
        owner._wait_thrift.assert_not_called()
    finally:
        owner.forward.close()


def test_unverified_reopened_tunnel_is_closed_so_the_next_open_retries(tmp_path, fake_kubectl):
    (fake_kubectl / "count").write_text("1")
    owner = ready_owner(tmp_path, "thrift")
    stale = owner.endpoint
    owner._wait_thrift.side_effect = TimeoutError("Managed Spark startup timed out")
    try:
        with pytest.raises(TimeoutError):
            owner.ensure_endpoint("thrift", {"compute": "fixture"})
        # Keeping this tunnel alive would hand out the stale port without retrying.
        assert owner.forward.process.poll() is not None and owner.endpoint is stale
        owner._wait_thrift.side_effect = lambda host, port, deadline: port
        assert owner.ensure_endpoint("thrift", {"compute": "fixture"}).port == 41004
    finally:
        owner.forward.close()


def test_expired_startup_budget_does_not_restart_dead_tunnel(tmp_path):
    owner = InvocationOwner(tmp_path / "receipt")
    owner.forward = Mock()
    owner.forward.process.poll.return_value = 1
    with pytest.raises(TimeoutError, match="startup timed out"):
        owner._refresh_tunnel(41000, time.monotonic() - 1)
    owner.forward.start.assert_not_called()
