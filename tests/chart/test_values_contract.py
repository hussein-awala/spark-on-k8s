from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

HELM = shutil.which("helm")
CHART = Path(__file__).resolve().parents[2] / "chart"
# Syntax-only fixtures: these values do not identify published images.
API_DIGEST = "sha256:" + "a" * 64
HISTORY_DIGEST = "sha256:" + "b" * 64
pytestmark = pytest.mark.skipif(HELM is None, reason="Helm is required to validate the chart")


def invoke(tmp_path, values, command="template", extra=()):
    path = tmp_path / "values.yaml"
    path.write_text(yaml.safe_dump(values))
    args = [HELM, command]
    if command == "template":
        args.extend(["test", str(CHART), "--namespace", "control"])
    else:
        args.append(str(CHART))
    return subprocess.run([*args, "-f", str(path), *extra], capture_output=True, text=True)


def resources(tmp_path, values):
    result = invoke(tmp_path, values)
    assert result.returncode == 0, result.stderr
    return [item for item in yaml.safe_load_all(result.stdout) if item]


def container(documents, name="spark-on-k8s"):
    return next(
        item["spec"]["template"]["spec"]["containers"][0]
        for item in documents
        if item["kind"] == "Deployment" and item["spec"]["template"]["spec"]["containers"][0]["name"] == name
    )


def test_development_default_retains_legacy_image_and_read_only_access(tmp_path):
    docs = resources(tmp_path, {})
    assert container(docs)["image"] == "ghcr.io/hussein-awala/spark-on-k8s:latest"
    binding = next(item for item in docs if item["kind"] == "ClusterRoleBinding")
    assert binding["roleRef"]["name"] == "view"
    assert invoke(tmp_path, {}, command="lint").returncode == 0


def test_empty_tag_explicitly_opts_into_chart_app_version(tmp_path):
    docs = resources(tmp_path, {"image": {"tag": ""}})
    metadata = yaml.safe_load((CHART / "Chart.yaml").read_text())
    assert container(docs)["image"] == "ghcr.io/hussein-awala/spark-on-k8s:" + metadata["appVersion"]


def test_explicit_tags_and_digests_select_both_images(tmp_path):
    docs = resources(
        tmp_path,
        {"image": {"tag": "api-test"}, "sparkHistory": {"enabled": True, "image": {"tag": "history-test"}}},
    )
    assert container(docs)["image"].endswith(":api-test")
    assert container(docs, "spark-history")["image"].endswith(":history-test")
    values = {
        "image": {"tag": "ignored", "digest": API_DIGEST},
        "sparkHistory": {"enabled": True, "image": {"tag": "ignored", "digest": HISTORY_DIGEST}},
    }
    docs = resources(tmp_path, values)
    assert container(docs)["image"] == "ghcr.io/hussein-awala/spark-on-k8s@" + API_DIGEST
    assert (
        container(docs, "spark-history")["image"] == "ghcr.io/hussein-awala/spark-history@" + HISTORY_DIGEST
    )


@pytest.mark.parametrize(
    "values,error",
    [
        ({"replicaCount": -1}, "replicaCount"),
        ({"replicaCount": "2"}, "replicaCount"),
        ({"readOnly": "false"}, "readOnly"),
        ({"namespaces": ["analytics"]}, "namespaces"),
        ({"namespaces": "analytics,,batch"}, "namespaces"),
        ({"service": {"port": 70000}}, "port"),
        ({"image": {"digest": "sha256:incomplete"}}, "digest"),
        ({"image": {"pullPolicy": "Sometimes"}}, "pullPolicy"),
        ({"sparkHistory": {"enabled": True}}, "sparkHistory"),
        ({"runHistory": {"namespaces": ["*"]}}, "namespaces"),
        ({"runHistory": {"namespaces": ["a", "a"]}}, "namespaces"),
        ({"runHistory": {"intervalSeconds": 4}}, "intervalSeconds"),
        ({"runHistory": {"retentionDays": 0}}, "retentionDays"),
        ({"runHistory": {"maxRows": 1000001}}, "maxRows"),
        ({"runHistory": {"enabled": True, "namespaces": ["analytics"]}}, "existingClaim"),
        (
            {
                "replicaCount": 2,
                "runHistory": {"enabled": True, "namespaces": ["analytics"], "existingClaim": "runs"},
            },
            "replicaCount",
        ),
        (
            {
                "autoscaling": {"enabled": True},
                "runHistory": {"enabled": True, "namespaces": ["analytics"], "existingClaim": "runs"},
            },
            "autoscaling",
        ),
        ({"autoscaling": {"enabled": True, "targetCPUUtilizationPercentage": 0}}, "autoscaling"),
        ({"rbac": {"create": "yes"}}, "create"),
        ({"rbac": {"create": True}, "serviceAccount": {"create": False}}, "serviceAccount"),
    ],
)
@pytest.mark.parametrize("command", ["lint", "template"])
def test_invalid_managed_values_are_rejected_before_install(tmp_path, values, error, command):
    result = invoke(tmp_path, values, command=command)
    assert result.returncode != 0, result.stdout
    assert error in result.stdout + result.stderr


@pytest.mark.parametrize(
    "create_account,create_rbac,expect_account,expect_roles",
    [
        (True, None, True, True),
        (False, None, False, False),
        (False, True, False, True),
        (True, False, True, False),
        (False, False, False, False),
    ],
)
def test_identity_and_rbac_ownership_are_independent_with_legacy_defaults(
    tmp_path, create_account, create_rbac, expect_account, expect_roles
):
    docs = resources(
        tmp_path,
        {
            "serviceAccount": {"create": create_account, "name": "existing-api"},
            "rbac": {"create": create_rbac},
            "namespaces": "analytics, batch,analytics",
        },
    )
    assert any(item["kind"] == "ServiceAccount" for item in docs) is expect_account
    roles = [item for item in docs if item["kind"] == "Role"]
    bindings = [item for item in docs if item["kind"] == "RoleBinding"]
    assert len(roles) == len(bindings) == (2 if expect_roles else 0)
    assert not any(item["kind"].startswith("ClusterRole") for item in docs)
    for role in roles:
        assert role["metadata"]["namespace"] in {"analytics", "batch"}
        assert all(set(rule["verbs"]) <= {"get", "list", "watch"} for rule in role["rules"])
    for binding in bindings:
        assert binding["subjects"] == [
            {"kind": "ServiceAccount", "name": "existing-api", "namespace": "control"}
        ]
    deployment = next(item for item in docs if item["kind"] == "Deployment")
    assert deployment["spec"]["template"]["spec"]["serviceAccountName"] == "existing-api"


def test_passthrough_kubernetes_fields_and_memory_only_scaling_remain_supported(tmp_path):
    values = {
        "resources": {"requests": {"example.com/device": "1"}},
        "securityContext": {"readOnlyRootFilesystem": True},
        "autoscaling": {
            "enabled": True,
            "targetCPUUtilizationPercentage": None,
            "targetMemoryUtilizationPercentage": 75,
        },
        "env": [{"name": "CUSTOM", "valueFrom": {"secretKeyRef": {"name": "settings", "key": "value"}}}],
        "podAnnotations": {"example.com/hint": "anything"},
    }
    docs = resources(tmp_path, values)
    assert container(docs)["resources"]["requests"]["example.com/device"] == "1"
    hpa = next(item for item in docs if item["kind"] == "HorizontalPodAutoscaler")
    assert [metric["resource"]["name"] for metric in hpa["spec"]["metrics"]] == ["memory"]


def test_production_example_requires_a_real_digest_parameter(tmp_path):
    example = CHART / "examples" / "production-values.yaml"
    result = invoke(tmp_path, {}, extra=["-f", str(example)])
    assert result.returncode != 0 and "digest" in result.stderr
    result = invoke(tmp_path, {}, extra=["-f", str(example), "--set-string", "image.digest=" + API_DIGEST])
    assert result.returncode == 0, result.stderr
    docs = [item for item in yaml.safe_load_all(result.stdout) if item]
    assert container(docs)["image"].endswith("@" + API_DIGEST)
    assert not any(
        item["kind"] in {"ServiceAccount", "Role", "RoleBinding", "ClusterRoleBinding"} for item in docs
    )


def test_autoscaler_cross_field_comparison_is_rejected_during_render(tmp_path):
    # Helm lint intentionally suppresses template fail(); always render as well.
    for values in (
        {"autoscaling": {"minReplicas": 5, "maxReplicas": 2}},
        {"sparkHistory": {"autoscaling": {"minReplicas": 5, "maxReplicas": 2}}},
    ):
        result = invoke(tmp_path, values)
        assert result.returncode != 0 and "minReplicas" in result.stderr
