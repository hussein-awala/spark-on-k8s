from __future__ import annotations

import json
import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "configured_arguments, arguments, expected_arguments",
    [
        (["from-env", "two words"], [], ["from-env", "two words"]),
        (["from-env"], ["--threshold", "0.5"], ["--threshold", "0.5"]),
        (None, [], []),
        (None, ["explicit"], ["explicit"]),
    ],
)
def test_submit_uses_configured_app_arguments(
    monkeypatch, configured_arguments, arguments, expected_arguments
):
    if configured_arguments is None:
        monkeypatch.delenv("SPARK_ON_K8S_APP_ARGUMENTS", raising=False)
    else:
        monkeypatch.setenv("SPARK_ON_K8S_APP_ARGUMENTS", json.dumps(configured_arguments))
    # Exercise configuration loading, CLI parsing, and the real pod builder in
    # a fresh process. Only Kubernetes interactions are replaced with mocks.
    script = """
import json
import sys
from unittest import mock
from click.testing import CliRunner
from spark_on_k8s.cli.app import submit

def create_pod(namespace, body):
    body.metadata.uid = "test-uid"
    return body

with (
    mock.patch("spark_on_k8s.client.KubernetesClientManager"),
    mock.patch("spark_on_k8s.client.k8s.CoreV1Api") as api_class,
):
    api_class.return_value.create_namespaced_pod.side_effect = create_pod
    result = CliRunner().invoke(
        submit,
        ["--image", "spark:test", "--path", "local:///app.py", "--name", "test-app", "--", *sys.argv[1:]],
    )
    assert result.exit_code == 0, (result.output, result.exception)
    pod = api_class.return_value.create_namespaced_pod.call_args.kwargs["body"]
    command = pod.spec.containers[0].args
    print(json.dumps(command[command.index("local:///app.py") + 1:]))
"""

    result = subprocess.run([sys.executable, "-c", script, *arguments], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == expected_arguments
