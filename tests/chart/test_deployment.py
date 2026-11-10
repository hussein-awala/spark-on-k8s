from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

HELM = shutil.which("helm")
CHART = Path(__file__).resolve().parents[2] / "chart"
DOCKERFILE = Path(__file__).resolve().parents[2] / "docker" / "Dockerfile"
pytestmark = pytest.mark.skipif(HELM is None, reason="Helm is required to render the chart")


def render_pod(*settings):
    result = subprocess.run(
        [HELM, "template", "test", str(CHART), "--show-only", "templates/deployment.yaml", *settings],
        check=True,
        capture_output=True,
        text=True,
    )
    return yaml.safe_load(result.stdout)["spec"]["template"]["spec"]


def render_deployment(*settings):
    return render_pod(*settings)["containers"][0]


def image_identity():
    users = re.findall(r"^USER (\S+)$", DOCKERFILE.read_text(), re.MULTILINE)
    uid, gid = users[-1].split(":")
    return int(uid), int(gid)


def test_api_pod_defaults_run_the_image_user_under_restricted_pod_security():
    uid, gid = image_identity()
    pod = render_pod()
    # A mismatched UID/GID would leave the image's run-history directory or fsGroup grant unusable.
    assert pod["securityContext"] == {
        "runAsNonRoot": True,
        "runAsUser": uid,
        "runAsGroup": gid,
        "fsGroup": gid,
        "fsGroupChangePolicy": "OnRootMismatch",
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    assert pod["containers"][0]["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
    }


def test_api_security_context_defaults_can_be_overridden_or_removed():
    pod = render_pod(
        "--set",
        "podSecurityContext.runAsUser=20000",
        "--set",
        "podSecurityContext.fsGroup=null",
        "--set",
        "securityContext.readOnlyRootFilesystem=true",
        "--set",
        "securityContext.capabilities.add[0]=NET_BIND_SERVICE",
    )
    assert pod["securityContext"]["runAsUser"] == 20000
    assert pod["securityContext"]["runAsNonRoot"] is True
    assert "fsGroup" not in pod["securityContext"]
    assert pod["containers"][0]["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"], "add": ["NET_BIND_SERVICE"]},
        "readOnlyRootFilesystem": True,
    }


@pytest.mark.parametrize("history_enabled", ["false", "true"])
def test_api_runtime_defaults_do_not_depend_on_history(history_enabled):
    container = render_deployment(
        "--set", f"sparkHistory.enabled={history_enabled}", "--set", "sparkHistory.image.tag=render-test"
    )
    env = {entry["name"]: entry.get("value") for entry in container["env"]}
    assert env["SPARK_ON_K8S_IN_CLUSTER"] == "true"
    assert env["SPARK_ON_K8S_API_HOST"] == "0.0.0.0"
    assert env["SPARK_ON_K8S_API_PORT"] == "8000"
    assert ("SPARK_ON_K8S_API_SPARK_HISTORY_HOST" in env) == (history_enabled == "true")


def test_a_kubeconfig_selected_in_env_is_not_overridden_by_in_cluster_mode():
    # Chart 0.3.0 left in-cluster mode off; enabling it would make the API ignore the file and
    # act on its own cluster with the pod's service account.
    container = render_deployment(
        "--set",
        "env[0].name=SPARK_ON_K8S_CONFIG_FILE",
        "--set-string",
        "env[0].value=/etc/remote/kubeconfig",
    )
    env = {entry["name"]: entry.get("value") for entry in container["env"]}
    assert env["SPARK_ON_K8S_CONFIG_FILE"] == "/etc/remote/kubeconfig"
    assert "SPARK_ON_K8S_IN_CLUSTER" not in env


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
