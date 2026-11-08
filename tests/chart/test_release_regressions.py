from __future__ import annotations

import subprocess
from pathlib import Path

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
                if d["kind"] in ("Role", "RoleBinding", "ClusterRoleBinding")
            }

        assert identities("control-a").isdisjoint(identities("control-b"))
        assert identities("control-a", True) == identities("control-b", True)


def test_stop_permission_and_authenticated_health_hook(tmp_path):
    docs = render(tmp_path, {"namespaces": "analytics", "readOnly": False})
    role = next(d for d in docs if d["kind"] == "Role")
    assert next(r for r in role["rules"] if r["resources"] == ["pods/exec"])["verbs"] == ["get", "create"]
    hook = next(d for d in docs if d["kind"] == "Pod")
    assert "/ready" in hook["spec"]["containers"][0]["args"][-1]
    assert "wget -T 5" in hook["spec"]["containers"][0]["args"][-1]
    assert hook["spec"]["activeDeadlineSeconds"] == 90


def test_source_requires_explicit_qualified_image():
    source = Path(__file__).resolve().parents[2] / "chart"
    result = subprocess.run(["helm", "template", "source", str(source)], capture_output=True, text=True)
    assert result.returncode != 0
    assert "explicit qualified image" in result.stderr
