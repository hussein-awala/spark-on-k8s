from __future__ import annotations

import json
import os
import sys
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from spark_on_k8s_dbt.forward import PortForward
from spark_on_k8s_dbt.lifecycle import InvocationOwner


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


def test_expired_startup_budget_does_not_restart_dead_tunnel(tmp_path):
    owner = InvocationOwner(tmp_path / "receipt")
    owner.forward = Mock()
    owner.forward.process.poll.return_value = 1
    with pytest.raises(TimeoutError, match="startup timed out"):
        owner._refresh_tunnel(41000, time.monotonic() - 1)
    owner.forward.start.assert_not_called()
