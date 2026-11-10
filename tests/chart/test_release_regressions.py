from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

CHART = Path(__file__).resolve().parents[2] / "chart"


def render(tmp_path, values, namespace="control"):
    path = tmp_path / "values.yaml"
    path.write_text(yaml.safe_dump(values))
    result = subprocess.run(
        ["helm", "template", "console", str(CHART), "--namespace", namespace, "-f", str(path)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return [item for item in yaml.safe_load_all(result.stdout) if item]


def history(docs):
    return next(
        d
        for d in docs
        if d["kind"] == "Deployment"
        and d["spec"]["template"]["spec"]["containers"][0]["name"] == "spark-history"
    )


def test_history_configuration_rolls_pods_and_preserves_scheduling(tmp_path):
    values = {
        "sparkHistory": {
            "enabled": True,
            "image": {"tag": "fixture"},
            "affinity": {
                "nodeAffinity": {
                    "requiredDuringSchedulingIgnoredDuringExecution": {
                        "nodeSelectorTerms": [
                            {"matchExpressions": [{"key": "pool", "operator": "In", "values": ["history"]}]}
                        ]
                    }
                }
            },
            "tolerations": [{"key": "history", "operator": "Exists", "effect": "NoSchedule"}],
        }
    }
    before = history(render(tmp_path, values))["spec"]["template"]
    values["sparkHistory"]["configuration"] = "spark.history.fs.logDirectory=s3a://events/new"
    after = history(render(tmp_path, values))["spec"]["template"]
    assert (
        before["metadata"]["annotations"]["checksum/config"]
        != after["metadata"]["annotations"]["checksum/config"]
    )
    for key in ("affinity", "tolerations"):
        assert after["spec"][key] == values["sparkHistory"][key]


def test_long_history_suffix_does_not_collide(tmp_path):
    for key in ("nameOverride", "fullnameOverride"):
        docs = render(
            tmp_path,
            {key: "a" * 55 + "-history", "sparkHistory": {"enabled": True, "image": {"tag": "fixture"}}},
        )
        identities = [(d["kind"], d["metadata"]["name"]) for d in docs]
        assert len(identities) == len(set(identities))
        selectors = [d["spec"]["selector"]["matchLabels"] for d in docs if d["kind"] == "Deployment"]
        assert selectors[0] != selectors[1]


def test_shared_rbac_is_unique_and_legacy_opt_in_is_preserved(tmp_path):
    for namespaces in ("*", "analytics"):

        def identities(namespace, legacy=False, namespaces=namespaces):
            docs = render(tmp_path, {"namespaces": namespaces, "rbac": {"legacyNames": legacy}}, namespace)
            return {
                (d["kind"], d["metadata"].get("namespace"), d["metadata"]["name"])
                for d in docs
                if d["kind"] in ("Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding")
            }

        assert identities("control-a").isdisjoint(identities("control-b"))
        assert identities("control-a", True) == identities("control-b", True)


@pytest.mark.parametrize("read_only", [True, False])
@pytest.mark.parametrize("legacy", [True, False])
def test_cluster_wide_rbac_names_never_reuse_built_in_role_bindings(tmp_path, read_only, legacy):
    values = {
        "namespaces": "*",
        "readOnly": read_only,
        "rbac": {"legacyNames": legacy, "allowClusterWideWrite": True},
    }
    fullname = "console-spark-on-k8s"
    digest = hashlib.sha256(f"control/{fullname}".encode()).hexdigest()
    name = fullname if legacy else f"{fullname}-{digest[:8]}"
    cluster = {d["kind"]: d for d in render(tmp_path, values) if d["kind"].startswith("ClusterRole")}
    # Earlier charts bound "*" installs to the built-in view/edit roles as <name>-view or <name>-edit.
    # roleRef is immutable, so reusing those names for the chart-owned role would fail `helm upgrade`.
    assert {kind: d["metadata"]["name"] for kind, d in cluster.items()} == {
        "ClusterRole": name,
        "ClusterRoleBinding": name,
    }
    assert cluster["ClusterRoleBinding"]["roleRef"]["name"] == name


@pytest.mark.parametrize("namespaces", ["", "*"])
def test_digit_only_release_name_namespace_and_secret_render_as_strings(tmp_path, namespaces):
    # Kubernetes accepts digit-only namespace, release and Secret names; unquoted they render as
    # YAML integers, which the API server refuses in labels, selectors, subjects and references.
    values = {
        "namespaces": namespaces,
        "nameOverride": "321",
        "metrics": {"enabled": True, "serviceMonitor": {"enabled": True}},
        "remote": {"enabled": True, "existingSecret": "789", "tokenKey": "1"},
    }
    path = tmp_path / "values.yaml"
    path.write_text(yaml.safe_dump(values))
    monitor_api = ["--api-versions", "monitoring.coreos.com/v1/ServiceMonitor"]
    result = subprocess.run(
        ["helm", "template", "456", str(CHART), "--namespace", "123", "-f", str(path), *monitor_api],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    docs = [item for item in yaml.safe_load_all(result.stdout) if item]
    bindings = [d for d in docs if d["kind"] in ("RoleBinding", "ClusterRoleBinding")]
    assert [subject["namespace"] for d in bindings for subject in d["subjects"]] == ["123"]
    if not namespaces:
        scoped = [d["metadata"]["namespace"] for d in docs if d["kind"] in ("Role", "RoleBinding")]
        assert scoped == ["123", "123"]
    for d in docs:
        labels = d["metadata"].get("labels", {})
        assert all(isinstance(value, str) for value in labels.values()), (d["kind"], labels)
    selectors = [d["spec"]["selector"] for d in docs if d["kind"] in ("Service", "Deployment")]
    assert selectors[0]["app.kubernetes.io/instance"] == "456"
    assert selectors[1]["matchLabels"]["app.kubernetes.io/instance"] == "456"
    monitor = next(d for d in docs if d["kind"] == "ServiceMonitor")["spec"]
    assert monitor["namespaceSelector"]["matchNames"] == ["123"]
    assert monitor["selector"]["matchLabels"]["app.kubernetes.io/instance"] == "456"
    assert monitor["endpoints"][0]["authorization"]["credentials"] == {"name": "789", "key": "1"}


def test_stop_permission_and_authenticated_health_hook(tmp_path):
    docs = render(tmp_path, {"namespaces": "analytics", "readOnly": False})
    role = next(d for d in docs if d["kind"] == "Role")
    assert next(r for r in role["rules"] if r["resources"] == ["pods/exec"])["verbs"] == ["get", "create"]
    hook = next(d for d in docs if d["kind"] == "Pod")
    assert "/ready" in hook["spec"]["containers"][0]["args"][-1]
    assert "wget -T 5" in hook["spec"]["containers"][0]["args"][-1]
    assert hook["spec"]["activeDeadlineSeconds"] == 90


def test_values_reused_from_published_chart_fail_schema_instead_of_rendering(tmp_path):
    # `helm upgrade --reuse-values` replaces this chart's defaults with the previous release's
    # coalesced values, so maps added after published chart 0.3.0 are absent, not defaulted.
    reused = tmp_path / "reused"
    shutil.copytree(CHART, reused)
    values = yaml.safe_load((reused / "values.yaml").read_text())
    for key in ("rbac", "runHistory", "metrics"):
        del values[key]
    (reused / "values.yaml").write_text(yaml.safe_dump(values))
    result = subprocess.run(["helm", "template", "console", str(reused)], capture_output=True, text=True)
    assert result.returncode != 0
    assert "nil pointer" not in result.stderr
    assert "missing properties" in result.stderr
    assert all(f"'{key}'" in result.stderr for key in ("rbac", "runHistory", "metrics"))


def test_notes_name_console_path_and_tls_ingress_origin_setting(tmp_path):
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text("")  # Client-side dry run; never read the caller's cluster context.

    def notes(*settings):
        result = subprocess.run(
            ["helm", "install", "console", str(CHART), "--dry-run=client", *settings],
            capture_output=True,
            text=True,
            env={**os.environ, "KUBECONFIG": str(kubeconfig)},
        )
        assert result.returncode == 0, result.stderr
        return result.stdout.partition("NOTES:")[2]

    tls = ["--set", "ingress.enabled=true", "--set", "ingress.tls[0].secretName=tls"]
    origins = ["--set", "env[0].name=SPARK_ON_K8S_API_WEBSOCKET_ALLOWED_ORIGINS"]
    assert "/webserver/apps" in notes()
    assert "SPARK_ON_K8S_API_WEBSOCKET_ALLOWED_ORIGINS" not in notes()
    assert "SPARK_ON_K8S_API_WEBSOCKET_ALLOWED_ORIGINS" in notes(*tls)
    assert "SPARK_ON_K8S_API_WEBSOCKET_ALLOWED_ORIGINS" not in notes(*tls, *origins)


def test_source_requires_explicit_qualified_image():
    source = Path(__file__).resolve().parents[2] / "chart"
    result = subprocess.run(["helm", "template", "source", str(source)], capture_output=True, text=True)
    assert result.returncode != 0
    assert "explicit qualified image" in result.stderr


def test_documented_quickstart_values_start_an_api_in_the_default_release_namespace():
    # README and getting started install with these values and no --namespace.
    example = Path(__file__).resolve().parents[2] / "examples" / "helm" / "values.yaml"
    result = subprocess.run(
        ["helm", "template", "spark-on-k8s-release", str(CHART), "--values", str(example)],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    deployment = next(
        item for item in yaml.safe_load_all(result.stdout) if item and item["kind"] == "Deployment"
    )
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    environment = {entry["name"]: entry["value"] for entry in container["env"] if "value" in entry}
    # The API checks its scope before serving (spark_on_k8s/api/main.py).
    started = subprocess.run(
        [
            sys.executable,
            "-c",
            "from spark_on_k8s.api.namespace_scope import namespace_scope; print(namespace_scope())",
        ],
        env={**os.environ, **environment},
        capture_output=True,
        text=True,
    )
    assert started.returncode == 0, started.stderr


def test_helm_test_pod_meets_pod_security_restricted(tmp_path):
    (hook,) = [item for item in render(tmp_path, {}) if item["metadata"]["name"].endswith("-test-connection")]
    spec = hook["spec"]
    (container,) = spec["containers"]
    # The four controls Pod Security "restricted" checks, and a pinned image.
    assert spec["securityContext"]["runAsNonRoot"] is True
    assert spec["securityContext"]["seccompProfile"] == {"type": "RuntimeDefault"}
    assert container["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
    }
    assert ":" in container["image"] and not container["image"].endswith(":latest")
