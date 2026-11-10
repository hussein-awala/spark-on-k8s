"""Readiness gives each query check the budget `connect check` uses and reports the last failure."""

from __future__ import annotations

import importlib.util
import json
import subprocess
from pathlib import Path

import pytest
import spark_on_k8s
from spark_on_k8s.connect import ConnectProfile
from spark_on_k8s.connect_server import ConnectServerTimeout

_SPEC = importlib.util.spec_from_file_location(
    "server_tests", Path(spark_on_k8s.__file__).resolve().parents[1] / "tests" / "test_connect_server.py"
)
server_tests = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(server_tests)
cluster = server_tests.cluster  # The repository's stateful Kubernetes API fixture.


@pytest.fixture
def clock(monkeypatch):
    now = [1000.0]
    monkeypatch.setattr("spark_on_k8s.utils.run.time.monotonic", lambda: now[0])
    monkeypatch.setattr(
        "spark_on_k8s.utils.run.time.sleep", lambda seconds: now.__setitem__(0, now[0] + seconds)
    )
    return now


def child(monkeypatch, clock, *, needs, answer):
    """Stand in for the probe child: it needs `needs` seconds, then prints `answer`."""
    attempts = []

    def run(command, *, input, timeout, **kwargs):
        attempts.append(timeout)
        clock[0] += min(needs, timeout)
        if timeout < needs:
            raise subprocess.TimeoutExpired(command, timeout)
        code, payload = answer(json.loads(input))
        return subprocess.CompletedProcess(command, code, stdout=json.dumps(payload), stderr="")

    monkeypatch.setattr("spark_on_k8s.connect.subprocess.run", run)
    return attempts


def ok(request):
    return 0, {
        "schema_version": 1,
        "status": "ok",
        "spark_version": "4.3.0",
        "client_version": "4.3.0",
        "app_id": request["expected_app_id"],
    }


def test_wait_succeeds_when_the_default_check_would(cluster, monkeypatch, clock):
    sdk, _, _ = cluster
    handle = sdk.create_connect_server(server_tests.config())
    profile = ConnectProfile(endpoint="sc://localhost:15002", tls=False)
    attempts = child(monkeypatch, clock, needs=6, answer=ok)

    # `connect check` (15 seconds by default) passes with this client.
    assert profile.probe(expected_app_id=handle.run.app_id)["status"] == "ok"
    attempts.clear()

    # Ten minutes is not enough for `connect server wait`: every attempt is cut at 5 seconds.
    try:
        result = sdk.wait_connect_server_ready(handle, profile=profile, timeout=600)
    except ConnectServerTimeout:
        pytest.fail(
            f"{len(attempts)} attempts, longest {max(attempts)}s, none allowed the 6s the client needs"
        )
    assert result["query_ready"] is True


def test_wait_timeout_says_that_another_application_answered(cluster, monkeypatch, clock):
    sdk, _, _ = cluster
    handle = sdk.create_connect_server(server_tests.config())
    profile = ConnectProfile(endpoint="sc://localhost:15002", tls=False)
    wrong = {
        "schema_version": 1,
        "status": "error",
        "error": "The endpoint is not the expected Spark application",
    }
    attempts = child(monkeypatch, clock, needs=1, answer=lambda request: (1, wrong))

    with pytest.raises(Exception) as error:
        sdk.wait_connect_server_ready(handle, profile=profile, timeout=60)

    # Today: ConnectServerTimeout after the whole budget, with no trace of the definitive answer
    # that every attempt received (a tunnel or profile pointing at a different server).
    assert "not the expected Spark application" in str(error.value), (
        f"{len(attempts)} attempts all got a definitive answer; reported only: {error.value}"
    )
