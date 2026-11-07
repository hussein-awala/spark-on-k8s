from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "configured_waiter, flags, expected_waiter",
    [("log", [], "log"), ("wait", [], "wait"), ("no_wait", [], "no_wait"), ("no_wait", ["--logs"], "log")],
)
def test_submit_uses_configured_waiter(monkeypatch, configured_waiter, flags, expected_waiter):
    # Unrelated parent settings must not affect the child CLI's startup.
    monkeypatch.setenv("SPARK_ON_K8S_SPARK_CONF", "unrelated invalid JSON")
    child_environment = {
        key: value for key, value in os.environ.items() if not key.startswith("SPARK_ON_K8S_")
    }
    child_environment["SPARK_ON_K8S_APP_WAITER"] = configured_waiter
    # Configuration and Click defaults are captured at import time. A fresh
    # process exercises the same environment-loading path as the installed CLI.
    script = """
import json
import sys
from unittest import mock
from click.testing import CliRunner
from spark_on_k8s.cli.app import submit
from spark_on_k8s.utils.run import RunHandle
from spark_on_k8s.utils.spark_app_status import SparkAppStatus

with mock.patch("spark_on_k8s.client.SparkOnK8S") as client_class:
    instance = client_class.return_value
    instance.last_submitted_run = RunHandle(
        namespace="default", app_id="example", driver_pod="example-driver"
    )
    instance.app_manager.app_status.return_value = SparkAppStatus.Succeeded
    result = CliRunner().invoke(
        submit, ["--image", "spark:test", "--path", "local:///app.py", *sys.argv[1:]]
    )
    assert result.exit_code == 0, result.output
    assert instance.submit_app.call_args.kwargs["app_waiter"] == "no_wait"
    actual = "no_wait"
    if instance.app_manager.wait_for_app.called:
        actual = "wait"
    if instance.app_manager.stream_logs.called:
        actual = "log"
    print(json.dumps(actual))
"""

    result = subprocess.run(
        [sys.executable, "-c", script, *flags], capture_output=True, text=True, env=child_environment
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == expected_waiter
