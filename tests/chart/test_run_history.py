from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml
from spark_on_k8s.api.run_history import READY_TIMEOUT

HELM = shutil.which("helm")
CHART = Path(__file__).resolve().parents[2] / "chart"
pytestmark = pytest.mark.skipif(HELM is None, reason="Helm is required to render the chart")


def render(tmp_path, values):
    path = tmp_path / "values.yaml"
    path.write_text(yaml.safe_dump(values))
    return subprocess.run(
        [HELM, "template", "test", str(CHART), "-f", str(path), "--show-only", "templates/deployment.yaml"],
        capture_output=True,
        text=True,
    )


def test_default_deployment_uses_separate_liveness_and_readiness_without_database(tmp_path):
    result = render(tmp_path, {})
    assert result.returncode == 0, result.stderr
    spec = yaml.safe_load(result.stdout)["spec"]["template"]["spec"]
    container = spec["containers"][0]
    assert container["livenessProbe"]["httpGet"]["path"] == "/health"
    assert container["readinessProbe"]["httpGet"]["path"] == "/ready"
    # The kubelet default (1s) would mark the pod unready inside the API's own check budget.
    assert container["readinessProbe"]["timeoutSeconds"] >= READY_TIMEOUT
    assert "volumes" not in spec
    assert all(entry["name"] != "SPARK_ON_K8S_RUN_HISTORY_ENABLED" for entry in container["env"])


def test_history_uses_existing_claim_explicit_namespaces_and_recreate_strategy(tmp_path):
    result = render(
        tmp_path,
        {
            "namespaces": "analytics,batch",
            "runHistory": {
                "enabled": True,
                "namespaces": ["analytics", "batch"],
                "existingClaim": "run-metadata",
                "retentionDays": 7,
                "maxRows": 500,
            },
            "metrics": {"enabled": True},
            "volumes": [{"name": "other", "emptyDir": {}}],
            "volumeMounts": [{"name": "other", "mountPath": "/tmp/other"}],
        },
    )
    assert result.returncode == 0, result.stderr
    deployment = yaml.safe_load(result.stdout)
    assert deployment["spec"]["strategy"] == {"type": "Recreate"}
    spec = deployment["spec"]["template"]["spec"]
    container = spec["containers"][0]
    env = {item["name"]: item["value"] for item in container["env"]}
    assert env["SPARK_ON_K8S_RUN_HISTORY_PATH"] == "/var/lib/spark-on-k8s/runs.sqlite3"
    assert env["SPARK_ON_K8S_RUN_HISTORY_NAMESPACES"] == "analytics,batch"
    assert env["SPARK_ON_K8S_RUN_HISTORY_RETENTION_DAYS"] == "7"
    assert env["SPARK_ON_K8S_RUN_HISTORY_MAX_ROWS"] == "500"
    assert env["SPARK_ON_K8S_API_DEFAULT_NAMESPACE"] == "analytics"
    assert env["SPARK_ON_K8S_METRICS_ENABLED"] == "true"
    assert spec["volumes"] == [
        {"name": "run-history", "persistentVolumeClaim": {"claimName": "run-metadata"}},
        {"name": "other", "emptyDir": {}},
    ]
    assert container["volumeMounts"][0] == {"name": "run-history", "mountPath": "/var/lib/spark-on-k8s"}
    # The kubelet regroups a root-written claim for the non-root API on volume types honoring fsGroup.
    assert spec["securityContext"]["fsGroup"] == spec["securityContext"]["runAsGroup"]
    assert spec["securityContext"]["fsGroupChangePolicy"] == "OnRootMismatch"


@pytest.mark.parametrize(
    "extra,message",
    [
        ({"runHistory": {"enabled": True}}, "namespaces"),
        ({"runHistory": {"enabled": True, "namespaces": ["analytics"]}}, "existingClaim"),
        ({"replicaCount": 2}, "replicaCount"),
        ({"autoscaling": {"enabled": True}}, "autoscaling"),
        ({"volumes": [{"name": "run-history", "emptyDir": {}}]}, "reserved volume name"),
    ],
)
def test_unsafe_or_ephemeral_chart_history_configuration_is_rejected(tmp_path, extra, message):
    values = {
        "namespaces": "analytics",
        "runHistory": {"enabled": True, "namespaces": ["analytics"], "existingClaim": "history"},
        **extra,
    }
    result = render(tmp_path, values)
    assert result.returncode != 0
    assert message in result.stderr


def test_history_namespaces_need_chart_managed_access_when_chart_manages_rbac(tmp_path):
    history = {"enabled": True, "namespaces": ["analytics", "ml"], "existingClaim": "history"}
    # Chart-managed Roles cover only `namespaces`; the recorder would never become ready in "ml".
    result = render(tmp_path, {"namespaces": "analytics, batch", "runHistory": history})
    assert result.returncode != 0
    assert '"ml" is not in namespaces' in result.stderr
    covered = {"namespaces": "analytics,ml", "runHistory": history}
    assert render(tmp_path, covered).returncode == 0
    assert render(tmp_path, {"namespaces": "*", "runHistory": history}).returncode == 0
    external = {"namespaces": "analytics", "rbac": {"create": False}, "runHistory": history}
    assert render(tmp_path, external).returncode == 0
    # The default scope is the release namespace ("default" for this render).
    assert render(tmp_path, {"runHistory": history}).returncode != 0
    release = {"enabled": True, "namespaces": ["default"], "existingClaim": "history"}
    assert render(tmp_path, {"runHistory": release}).returncode == 0


def test_explicit_environment_overrides_are_preserved_without_duplicates(tmp_path):
    result = render(
        tmp_path,
        {
            "namespaces": "analytics,batch",
            "runHistory": {"enabled": True, "namespaces": ["analytics"], "existingClaim": "history"},
            "metrics": {"enabled": True},
            "env": [
                {"name": "SPARK_ON_K8S_METRICS_ENABLED", "value": "false"},
                {"name": "SPARK_ON_K8S_RUN_HISTORY_MAX_ROWS", "value": "100"},
                {"name": "SPARK_ON_K8S_API_DEFAULT_NAMESPACE", "value": "batch"},
            ],
        },
    )
    assert result.returncode == 0, result.stderr
    env = yaml.safe_load(result.stdout)["spec"]["template"]["spec"]["containers"][0]["env"]
    assert len(env) == len({item["name"] for item in env})
    values = {item["name"]: item["value"] for item in env}
    assert values["SPARK_ON_K8S_METRICS_ENABLED"] == "false"
    assert values["SPARK_ON_K8S_RUN_HISTORY_MAX_ROWS"] == "100"
    assert values["SPARK_ON_K8S_API_DEFAULT_NAMESPACE"] == "batch"
    without_history = render(tmp_path, {"namespaces": "analytics,batch"})
    env = yaml.safe_load(without_history.stdout)["spec"]["template"]["spec"]["containers"][0]["env"]
    assert (
        next(item["value"] for item in env if item["name"] == "SPARK_ON_K8S_API_DEFAULT_NAMESPACE")
        == "analytics"
    )
