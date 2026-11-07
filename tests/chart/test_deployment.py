from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

HELM = shutil.which("helm")
CHART = Path(__file__).resolve().parents[2] / "chart"
pytestmark = pytest.mark.skipif(HELM is None, reason="Helm is required to render the chart")


def render_deployment(*settings):
    result = subprocess.run(
        [HELM, "template", "test", str(CHART), "--show-only", "templates/deployment.yaml", *settings],
        check=True,
        capture_output=True,
        text=True,
    )
    return yaml.safe_load(result.stdout)["spec"]["template"]["spec"]["containers"][0]


@pytest.mark.parametrize("history_enabled", ["false", "true"])
def test_api_runtime_defaults_do_not_depend_on_history(history_enabled):
    container = render_deployment("--set", f"sparkHistory.enabled={history_enabled}")
    env = {entry["name"]: entry.get("value") for entry in container["env"]}
    assert env["SPARK_ON_K8S_IN_CLUSTER"] == "true"
    assert env["SPARK_ON_K8S_API_HOST"] == "0.0.0.0"
    assert env["SPARK_ON_K8S_API_PORT"] == "8000"
    assert ("SPARK_ON_K8S_API_SPARK_HISTORY_HOST" in env) == (history_enabled == "true")


def test_custom_service_port_reaches_the_listening_api():
    container = render_deployment("--set", "service.port=9000")
    env = {entry["name"]: entry.get("value") for entry in container["env"]}
    assert container["ports"][0]["containerPort"] == 9000
    assert env["SPARK_ON_K8S_API_PORT"] == "9000"


def test_explicit_environment_overrides_are_not_duplicated():
    container = render_deployment(
        "--set",
        "env[0].name=SPARK_ON_K8S_API_HOST",
        "--set-string",
        "env[0].value=::",
        "--set",
        "env[1].name=SPARK_ON_K8S_IN_CLUSTER",
        "--set",
        "env[1].valueFrom.configMapKeyRef.name=auth-mode",
        "--set",
        "env[1].valueFrom.configMapKeyRef.key=in-cluster",
    )
    entries = container["env"]
    assert len({entry["name"] for entry in entries}) == len(entries)
    env = {entry["name"]: entry for entry in entries}
    assert env["SPARK_ON_K8S_API_HOST"]["value"] == "::"
    assert env["SPARK_ON_K8S_IN_CLUSTER"]["valueFrom"] == {
        "configMapKeyRef": {"name": "auth-mode", "key": "in-cluster"}
    }


@pytest.mark.parametrize("read_only", ["true", "false"])
def test_namespace_role_can_read_diagnostic_events_without_mutating_them(read_only):
    result = subprocess.run(
        [
            HELM,
            "template",
            "test",
            str(CHART),
            "--show-only",
            "templates/serviceaccount.yaml",
            "--set",
            "namespaces=analytics",
            "--set",
            f"readOnly={read_only}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    documents = list(yaml.safe_load_all(result.stdout))
    role = next(document for document in documents if document["kind"] == "Role")
    assert role["metadata"]["namespace"] == "analytics"
    assert [rule for rule in role["rules"] if "events" in rule["resources"]] == [
        {"apiGroups": [""], "resources": ["events"], "verbs": ["list"]}
    ]
    assert not any(document["kind"].startswith("Cluster") for document in documents)
