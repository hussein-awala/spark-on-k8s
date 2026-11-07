from __future__ import annotations

import json
import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "configured_waiter, flags, expected_waiter",
    [("log", [], "log"), ("wait", [], "wait"), ("no_wait", [], "no_wait"), ("no_wait", ["--logs"], "log")],
)
def test_submit_uses_configured_waiter(monkeypatch, configured_waiter, flags, expected_waiter):
    monkeypatch.setenv("SPARK_ON_K8S_APP_WAITER", configured_waiter)
    # Configuration and Click defaults are captured at import time. A fresh
    # process exercises the same environment-loading path as the installed CLI.
    script = """
import json
import sys
from unittest import mock
from click.testing import CliRunner
from spark_on_k8s.cli.app import submit

with mock.patch("spark_on_k8s.client.SparkOnK8S") as client_class:
    result = CliRunner().invoke(
        submit, ["--image", "spark:test", "--path", "local:///app.py", *sys.argv[1:]]
    )
    assert result.exit_code == 0, result.output
    print(json.dumps(client_class.return_value.submit_app.call_args.kwargs["app_waiter"]))
"""

    result = subprocess.run([sys.executable, "-c", script, *flags], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == expected_waiter
