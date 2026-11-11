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


def environment(documents):
    return {item["name"]: item.get("value") for item in container(documents)["env"]}


def test_release_default_uses_app_version_and_read_only_access_to_its_namespace(tmp_path):
    docs = resources(tmp_path, {})
    assert container(docs)["image"] == "ghcr.io/hussein-awala/spark-on-k8s:0.0.0-test"
    assert not any(item["kind"].startswith("ClusterRole") for item in docs)
    role = next(item for item in docs if item["kind"] == "Role")
    binding = next(item for item in docs if item["kind"] == "RoleBinding")
    assert role["metadata"]["namespace"] == binding["metadata"]["namespace"] == "control"
    assert all(set(rule["verbs"]) <= {"get", "list", "watch"} for rule in role["rules"])
    env = environment(docs)
    assert env["SPARK_ON_K8S_API_NAMESPACES"] == "control"
    assert env["SPARK_ON_K8S_API_DEFAULT_NAMESPACE"] == "control"
    assert invoke(tmp_path, {}, command="lint").returncode == 0


@pytest.mark.parametrize(
    "namespaces, scope, default, bindings",
    [
        ("*", "*", None, {("ClusterRoleBinding", None)}),
        (
            " analytics, batch ,analytics",
            "analytics,batch",
            "analytics",
            {("RoleBinding", "analytics"), ("RoleBinding", "batch")},
        ),
    ],
)
def test_api_namespace_scope_matches_chart_managed_rbac(tmp_path, namespaces, scope, default, bindings):
    docs = resources(tmp_path, {"namespaces": namespaces})
    env = environment(docs)
    assert env["SPARK_ON_K8S_API_NAMESPACES"] == scope
    # With every namespace in scope, the API opens its own namespace unless told otherwise.
    assert env.get("SPARK_ON_K8S_API_DEFAULT_NAMESPACE") == default
    assert {
        (item["kind"], item["metadata"].get("namespace")) for item in docs if item["kind"].endswith("Binding")
    } == bindings


@pytest.mark.parametrize("namespace", ["123", "0x10", "true", "null"])
def test_namespaces_that_read_as_yaml_scalars_stay_strings_in_chart_managed_rbac(tmp_path, namespace):
    # Valid Kubernetes namespace names. Unquoted, they render as an integer, a boolean or null,
    # and clients then reject the Role or default it into the release namespace.
    docs = resources(tmp_path, {"namespaces": f"{namespace},analytics"})
    for kind in ("Role", "RoleBinding"):
        placed = [item["metadata"].get("namespace") for item in docs if item["kind"] == kind]
        assert placed == [namespace, "analytics"], kind
    assert environment(docs)["SPARK_ON_K8S_API_NAMESPACES"] == f"{namespace},analytics"


# (resource, verb) pairs for the Kubernetes calls the API makes: listing and reading drivers,
# readiness and run history, driver log streams and diagnostic events; with readOnly false,
# also kill (exec), delete and remote submission's driver pod and headless service.
READ_ACCESS = {("pods", "list"), ("pods", "get"), ("pods/log", "get"), ("events", "list")}
WRITE_ACCESS = READ_ACCESS | {
    # A WebSocket exec starts as a GET; newer API servers also require create.
    ("pods/exec", "get"),
    ("pods/exec", "create"),
    ("pods", "delete"),
    ("pods", "create"),
    ("services", "create"),
}


@pytest.mark.parametrize("read_only", [True, False])
def test_cluster_wide_scope_binds_a_chart_owned_role_with_the_namespace_rules(tmp_path, read_only):
    rbac = {"allowClusterWideWrite": True}
    cluster = resources(tmp_path, {"namespaces": "*", "readOnly": read_only, "rbac": rbac})
    namespaced = resources(tmp_path, {"namespaces": "analytics", "readOnly": read_only})
    role = next(item for item in namespaced if item["kind"] == "Role")
    cluster_role = next(item for item in cluster if item["kind"] == "ClusterRole")
    binding = next(item for item in cluster if item["kind"] == "ClusterRoleBinding")
    assert cluster_role["rules"] == role["rules"]
    assert binding["roleRef"] == {
        "apiGroup": "rbac.authorization.k8s.io",
        "kind": "ClusterRole",
        "name": cluster_role["metadata"]["name"],
    }
    assert binding["subjects"] == [
        {"kind": "ServiceAccount", "name": "test-spark-on-k8s", "namespace": "control"}
    ]
    rules = cluster_role["rules"]
    granted = {(resource, verb) for rule in rules for resource in rule["resources"] for verb in rule["verbs"]}
    # Exactly these: least privilege, so an unused verb is a failure too.
    assert granted == (READ_ACCESS if read_only else WRITE_ACCESS)
    # Unlike the built-in view/edit roles: core API pod access only, never Secrets or ConfigMaps.
    assert all(rule["apiGroups"] == [""] for rule in rules)
    assert {resource for resource, _ in granted} <= {"pods", "pods/log", "pods/exec", "services", "events"}
    assert all(set(rule["verbs"]) <= {"get", "list", "watch"} for rule in rules) is read_only


def test_cluster_wide_write_requires_explicit_acknowledgement(tmp_path):
    result = invoke(tmp_path, {"namespaces": "*", "readOnly": False})
    assert result.returncode != 0
    assert "rbac.allowClusterWideWrite" in result.stderr
    # Read-only cluster-wide access, listed namespaces and external RBAC need no acknowledgement.
    for values in (
        {"namespaces": "*"},
        {"namespaces": "analytics", "readOnly": False},
        {"namespaces": "*", "readOnly": False, "rbac": {"create": False}},
        {"namespaces": "*", "readOnly": False, "rbac": {"allowClusterWideWrite": True}},
    ):
        result = invoke(tmp_path, values)
        assert result.returncode == 0, (values, result.stderr)


def test_explicit_namespace_scope_environment_is_preserved_without_duplicates(tmp_path):
    override = {"name": "SPARK_ON_K8S_API_NAMESPACES", "value": "analytics"}
    docs = resources(tmp_path, {"namespaces": "analytics,batch", "env": [override]})
    names = [item["name"] for item in container(docs)["env"]]
    assert names.count("SPARK_ON_K8S_API_NAMESPACES") == 1
    assert environment(docs)["SPARK_ON_K8S_API_NAMESPACES"] == "analytics"


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
        ({"image": {"repository": "registry.example/app:old"}}, "repository"),
        ({"ingress": {"hosts": [{"paths": [{"path": "/"}]}]}}, "pathType"),
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
        ({"rbac": {"allowClusterWideWrite": "yes"}}, "allowClusterWideWrite"),
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
