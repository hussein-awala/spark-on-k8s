from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "configured_template, flags, expected_template, expected_path",
    [
        (None, [], None, None),
        (None, ["--executor-template", "./executor.yaml"], "./executor.yaml", None),
        (
            None,
            ["--executor-template", "apiVersion: v1\nkind: Pod\nspec:\n  containers:\n  - name: executor"],
            "apiVersion: v1\nkind: Pod\nspec:\n  containers:\n  - name: executor",
            None,
        ),
        ("./configured.yaml", [], "./configured.yaml", None),
        ("./configured.yaml", ["--executor-template", "./override.yaml"], "./override.yaml", None),
        (
            None,
            ["--executor-pod-template-path", "/opt/spark/executor.yaml"],
            None,
            "/opt/spark/executor.yaml",
        ),
    ],
)
def test_app_submit_forwards_executor_template(configured_template, flags, expected_template, expected_path):
    # Import-time Click defaults must be loaded from only this test's settings.
    environment = {key: value for key, value in os.environ.items() if not key.startswith("SPARK_ON_K8S_")}
    if configured_template is not None:
        environment["SPARK_ON_K8S_EXECUTOR_TEMPLATE"] = configured_template
    script = """
import json
import sys
from unittest import mock
from spark_on_k8s.cli import main

flags = sys.argv[1:]
sys.argv = ["spark-on-k8s", "app", "submit", "--image", "spark:test", "--path", "local:///app.py", *flags]
with mock.patch("spark_on_k8s.client.SparkOnK8S", autospec=True) as client_class:
    try:
        main()
    except SystemExit as error:
        assert error.code == 0, error.code
    client_class.return_value.submit_app.assert_called_once()
    arguments = client_class.return_value.submit_app.call_args.kwargs
    print(json.dumps({
        "template": arguments["executor_pod_template"],
        "path": arguments["executor_pod_template_path"],
    }))
"""
    result = subprocess.run(
        [sys.executable, "-c", script, *flags], capture_output=True, text=True, env=environment
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout) == {"template": expected_template, "path": expected_path}
