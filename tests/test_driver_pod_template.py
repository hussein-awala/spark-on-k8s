from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import yaml
from click.testing import CliRunner
from kubernetes import client as k8s
from spark_on_k8s.client import PodResources, SparkOnK8S
from spark_on_k8s.utils.driver_pod_template import load_driver_pod_template


@pytest.fixture
def template():
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"labels": {"team": "data"}, "annotations": {"example.org/profile": "restricted"}},
        "spec": {
            "securityContext": {"runAsUser": 185, "runAsNonRoot": True, "fsGroup": 185},
            "affinity": {"nodeAffinity": {"preferredDuringSchedulingIgnoredDuringExecution": []}},
            "imagePullSecrets": [{"name": "private-registry"}],
            "hostNetwork": True,
            "dnsPolicy": "ClusterFirstWithHostNet",
            "volumes": [{"name": "scratch", "emptyDir": {}}],
            "containers": [
                {"name": "metrics", "image": "metrics:v1", "args": ["serve"]},
                {
                    "name": "spark-main",
                    "image": "ignored:v1",
                    "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
                    "volumeMounts": [{"name": "scratch", "mountPath": "/scratch"}],
                    "env": [{"name": "PROFILE", "value": "restricted"}],
                    "resources": {
                        "requests": {"cpu": "8", "ephemeral-storage": "1Gi"},
                        "limits": {"nvidia.com/gpu": "1"},
                    },
                },
            ],
        },
    }


@pytest.fixture
def submission(monkeypatch):
    api = MagicMock()
    api.create_namespaced_pod.return_value = SimpleNamespace(
        metadata=SimpleNamespace(name="report-driver", uid="pod-uid")
    )
    monkeypatch.setattr(k8s, "CoreV1Api", MagicMock(return_value=api))
    clients = MagicMock()
    spark = SparkOnK8S(k8s_client_manager=clients)

    def submit(**kwargs):
        return spark.submit_app(
            image="spark-app:v1",
            app_path="local:///opt/spark/job.py",
            app_name="report",
            app_id_suffix=lambda: "",
            namespace="analytics",
            service_account="spark",
            **kwargs,
        )

    return SimpleNamespace(api=api, clients=clients, submit=submit)


@pytest.mark.parametrize("input_kind", ["mapping", "yaml", "path", "path_string"])
def test_submission_preserves_security_sidecars_and_template_input(
    submission, template, tmp_path, input_kind
):
    before = deepcopy(template)
    value = template
    if input_kind == "yaml":
        value = yaml.safe_dump(template)
    elif input_kind in ("path", "path_string"):
        path = tmp_path / "driver.yaml"
        path.write_text(yaml.safe_dump(template))
        value = str(path) if input_kind == "path_string" else path

    assert submission.submit(driver_pod_template=value, driver_container_name="spark-main") == "report-driver"

    pod = submission.api.create_namespaced_pod.call_args.kwargs["body"]
    assert pod["apiVersion"] == "v1"
    assert pod["kind"] == "Pod"
    assert pod["metadata"]["name"] == "report-driver"
    assert pod["metadata"]["namespace"] == "analytics"
    assert pod["metadata"]["labels"]["team"] == "data"
    assert pod["metadata"]["annotations"]["kubectl.kubernetes.io/default-container"] == "spark-main"
    assert pod["spec"]["securityContext"] == before["spec"]["securityContext"]
    assert pod["spec"]["affinity"] == before["spec"]["affinity"]
    assert pod["spec"]["imagePullSecrets"] == [{"name": "private-registry"}]
    assert pod["spec"]["hostNetwork"] is True
    assert pod["spec"]["dnsPolicy"] == "ClusterFirstWithHostNet"
    driver, sidecar = pod["spec"]["containers"]
    assert driver["name"] == "spark-main"
    assert driver["image"] == "spark-app:v1"
    assert driver["securityContext"] == before["spec"]["containers"][1]["securityContext"]
    assert driver["volumeMounts"] == [{"name": "scratch", "mountPath": "/scratch"}]
    assert driver["resources"]["requests"] == {"cpu": "1", "memory": "1536Mi", "ephemeral-storage": "1Gi"}
    assert driver["resources"]["limits"]["nvidia.com/gpu"] == "1"
    assert sidecar == before["spec"]["containers"][0]
    assert template == before


def test_generated_identity_and_launch_fields_cannot_be_overridden(submission, template):
    template["metadata"].update(name="foreign-pod", namespace="other")
    template["metadata"]["labels"].update(
        {
            "spark-app-id": "foreign",
            "spark-app-name": "foreign",
            "spark-role": "executor",
            "spark-ui-proxy": "true",
        }
    )
    template["spec"].update(serviceAccount="admin", serviceAccountName="admin", restartPolicy="Always")
    driver = template["spec"]["containers"][1]
    driver.update(command=["sh", "-c"], args=["sleep forever"], imagePullPolicy="Always")
    driver["env"] = [{"name": "SPARK_DRIVER_BIND_ADDRESS", "value": "attacker.invalid"}]
    driver["ports"] = [{"name": "driver-port", "containerPort": 9999}]

    submission.submit(
        driver_pod_template=template,
        driver_container_name="spark-main",
        driver_resources=PodResources(cpu=2, memory=2048, memory_overhead=1024),
        driver_labels={"spark-app-id": "also-foreign", "spark-ui-proxy": "true", "team": "science"},
        driver_annotations={"spark-on-k8s.io/airflow-task": "trusted-context"},
    )

    pod = submission.api.create_namespaced_pod.call_args.kwargs["body"]
    assert pod["metadata"]["labels"] == {
        "team": "science",
        "spark-app-name": "report",
        "spark-app-id": "report",
        "spark-role": "driver",
    }
    assert pod["metadata"]["annotations"]["spark-on-k8s.io/airflow-task"] == "trusted-context"
    assert pod["spec"]["serviceAccountName"] == "spark"
    assert "serviceAccount" not in pod["spec"]
    assert pod["spec"]["restartPolicy"] == "Never"
    driver = pod["spec"]["containers"][0]
    assert "command" not in driver
    assert driver["args"][:3] == [
        "driver",
        "--master",
        "k8s://https://kubernetes.default.svc.cluster.local:443",
    ]
    assert driver["args"][-1] == "local:///opt/spark/job.py"
    assert "spark.app.id=report" in driver["args"]
    assert driver["imagePullPolicy"] == "IfNotPresent"
    assert driver["resources"]["requests"]["memory"] == "3072Mi"
    assert driver["env"] == [
        {"name": "SPARK_DRIVER_BIND_ADDRESS", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}}
    ]
    assert driver["ports"][0] == {"name": "driver-port", "containerPort": 7077}


def test_named_collections_merge_without_combining_incompatible_sources(submission, template):
    template["spec"]["nodeSelector"] = {"pool": "old", "disk": "ssd"}
    template["spec"]["initContainers"] = [{"name": "init", "image": "old:v1", "args": ["old"]}]
    template["spec"]["hostAliases"] = [{"ip": "192.0.2.1", "hostnames": ["old"]}]
    template["spec"]["tolerations"] = [{"key": "pool", "operator": "Exists"}]
    template["spec"]["containers"][1]["envFrom"] = [{"configMapRef": {"name": "settings"}}]

    submission.submit(
        driver_pod_template=template,
        driver_container_name="spark-main",
        volumes=[
            k8s.V1Volume(
                name="scratch",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="data"),
            )
        ],
        driver_volume_mounts=[k8s.V1VolumeMount(name="scratch", mount_path="/scratch", read_only=True)],
        driver_node_selector={"pool": "new"},
        driver_init_containers=[k8s.V1Container(name="init", image="new:v1")],
        driver_host_aliases=[k8s.V1HostAlias(ip="192.0.2.1", hostnames=["new"])],
        driver_tolerations=[k8s.V1Toleration(key="pool", operator="Exists")],
        driver_env_vars_from_secrets=["credentials"],
        driver_ephemeral_configmaps_volumes=[
            {
                "name": "queries",
                "mount_path": "/queries",
                "sources": [{"name": "query.sql", "text": "SELECT 1"}],
            }
        ],
    )

    pod = submission.api.create_namespaced_pod.call_args.kwargs["body"]
    spec = pod["spec"]
    assert spec["volumes"][0] == {"name": "scratch", "persistentVolumeClaim": {"claimName": "data"}}
    assert spec["nodeSelector"] == {"pool": "new", "disk": "ssd"}
    assert spec["initContainers"] == [{"name": "init", "image": "new:v1"}]
    assert spec["hostAliases"] == [{"ip": "192.0.2.1", "hostnames": ["new"]}]
    assert spec["tolerations"] == [{"key": "pool", "operator": "Exists"}]
    assert spec["containers"][0]["volumeMounts"][0] == {
        "name": "scratch",
        "mountPath": "/scratch",
        "readOnly": True,
    }
    assert spec["containers"][0]["envFrom"] == [
        {"configMapRef": {"name": "settings"}},
        {"secretRef": {"name": "credentials"}},
    ]
    configmap = submission.api.create_namespaced_config_map.call_args.kwargs["body"]
    assert spec["volumes"][1]["configMap"]["name"] == configmap.metadata.name
    assert spec["containers"][0]["volumeMounts"][1] == {
        "name": configmap.metadata.name,
        "mountPath": "/queries",
    }


@pytest.mark.parametrize("name", ["driver", "custom"])
def test_sole_driver_container_is_selected_without_an_override(submission, name):
    template = {"apiVersion": "v1", "kind": "Pod", "spec": {"containers": [{"name": name}]}}
    submission.submit(driver_pod_template=template)
    assert (
        submission.api.create_namespaced_pod.call_args.kwargs["body"]["spec"]["containers"][0]["name"] == name
    )


def test_named_driver_is_selected_in_multi_container_template(submission, template):
    template["spec"]["containers"][1]["name"] = "driver"
    submission.submit(driver_pod_template=template)
    assert (
        submission.api.create_namespaced_pod.call_args.kwargs["body"]["spec"]["containers"][0]["name"]
        == "driver"
    )


@pytest.mark.parametrize(
    "mutation, message",
    [
        (lambda p: p.update(kind="Deployment"), "kind: Pod"),
        (lambda p: p.update(apiVersion="apps/v1"), "apiVersion: v1"),
        (lambda p: p.update(status={}), "may contain only"),
        (lambda p: p["metadata"].update(ownerReferences=[]), "ownerReferences"),
        (lambda p: p["metadata"].update(uid="foreign"), "uid"),
        (
            lambda p: p["metadata"]["annotations"].update({"spark-on-k8s.io/airflow-task": "forged"}),
            "ownership",
        ),
        (lambda p: p["metadata"]["labels"].update(invalid=True), "string keys and values"),
        (lambda p: p["spec"].update(containers=[]), "at least one"),
        (lambda p: p["spec"]["containers"].append({"name": "spark-main"}), "duplicate"),
        (lambda p: p["spec"].update(volumes="invalid"), "list of objects"),
        (lambda p: p["spec"]["containers"][0].pop("image"), "requires an image"),
        (lambda p: p["spec"]["containers"][1].update(env=[{"name": "A"}, {"name": "A"}]), "duplicate"),
        (lambda p: p["spec"].update(securityContext="invalid"), "must be an object"),
    ],
)
def test_invalid_templates_fail_before_any_kubernetes_operations(submission, template, mutation, message):
    mutation(template)
    with pytest.raises(ValueError, match=message):
        submission.submit(
            driver_pod_template=template,
            driver_container_name="spark-main",
            secret_values={"TOKEN": "secret"},
        )
    submission.clients.client.assert_not_called()
    assert submission.api.mock_calls == []


@pytest.mark.parametrize("selected", [None, "absent", ""])
def test_missing_or_ambiguous_driver_selection_is_rejected_before_writes(submission, template, selected):
    with pytest.raises(ValueError, match="Select an existing driver container"):
        submission.submit(
            driver_pod_template=template, driver_container_name=selected, secret_values={"TOKEN": "secret"}
        )
    submission.clients.client.assert_not_called()


def test_container_selection_requires_a_template(submission):
    with pytest.raises(ValueError, match="requires driver_pod_template"):
        submission.submit(driver_container_name="driver")
    submission.clients.client.assert_not_called()


@pytest.mark.parametrize(
    "value",
    ["[]", "apiVersion: v1\nkind: Pod\nkind: Pod", "apiVersion: [", Path("/missing/driver-template.yaml")],
)
def test_loader_rejects_invalid_or_missing_templates(value):
    with pytest.raises(ValueError):
        load_driver_pod_template(value)


def test_cli_passes_template_path_and_container_selection(tmp_path, monkeypatch):
    from spark_on_k8s.cli.app import submit
    from spark_on_k8s.utils.run import RunHandle

    mocked_submit = MagicMock()
    client = SimpleNamespace(
        submit_app=mocked_submit,
        last_submitted_run=RunHandle(namespace="default", app_id="app", driver_pod="app-driver"),
    )
    monkeypatch.setattr("spark_on_k8s.client.SparkOnK8S", MagicMock(return_value=client))
    path = tmp_path / "driver.yaml"
    result = CliRunner().invoke(
        submit,
        [
            "--image",
            "spark:v1",
            "--path",
            "local:///job.py",
            "--driver-pod-template",
            str(path),
            "--driver-container-name",
            "spark-main",
        ],
    )
    assert result.exit_code == 0, result.output
    assert mocked_submit.call_args.kwargs["driver_pod_template"] == str(path)
    assert mocked_submit.call_args.kwargs["driver_container_name"] == "spark-main"


@pytest.mark.parametrize("value", [42, b"apiVersion: v1", "1: unsupported-key"])
def test_invalid_template_input_types_fail_before_secret_or_pod_creation(submission, value):
    with pytest.raises(ValueError):
        submission.submit(driver_pod_template=value, secret_values={"TOKEN": "private"})
    submission.clients.client.assert_not_called()
    assert submission.api.mock_calls == []


@pytest.mark.parametrize("string_path", [False, True])
def test_non_utf8_template_file_fails_before_any_kubernetes_operation(submission, tmp_path, string_path):
    path = tmp_path / "driver.yaml"
    path.write_bytes(b"apiVersion: v1\nkind: Pod\n# \xff")
    with pytest.raises(UnicodeDecodeError):
        submission.submit(driver_pod_template=str(path) if string_path else path)
    submission.clients.client.assert_not_called()
    assert submission.api.mock_calls == []


def test_template_file_that_becomes_unreadable_fails_before_any_kubernetes_operation(
    submission, tmp_path, monkeypatch
):
    path = tmp_path / "driver.yaml"
    path.write_text("apiVersion: v1", encoding="utf-8")

    def unreadable(self, *args, **kwargs):
        raise PermissionError("File permissions changed after the existence check")

    monkeypatch.setattr(Path, "read_text", unreadable)
    with pytest.raises(ValueError, match="Cannot read driver pod template file"):
        submission.submit(driver_pod_template=str(path))
    submission.clients.client.assert_not_called()
    assert submission.api.mock_calls == []


def test_yaml_text_survives_filesystem_probe_failure(monkeypatch):
    def cannot_probe(self):
        raise OSError("Input exceeds the filesystem filename limit")

    monkeypatch.setattr(Path, "is_file", cannot_probe)
    template = load_driver_pod_template("{apiVersion: v1, kind: Pod, spec: {containers: [{name: driver}]}}")
    assert template["spec"]["containers"] == [{"name": "driver"}]


@pytest.mark.parametrize(
    "field,entry",
    [
        ("volumes", {"name": ""}),
        ("initContainers", {"image": "spark:test"}),
        ("imagePullSecrets", {"name": 42}),
    ],
)
def test_missing_named_pod_entries_are_rejected_before_writes(submission, template, field, entry):
    template["spec"][field] = [entry]
    with pytest.raises(ValueError, match="require a nonempty name"):
        submission.submit(driver_pod_template=template, driver_container_name="spark-main")
    submission.clients.client.assert_not_called()
    assert submission.api.mock_calls == []


@pytest.mark.parametrize(
    "field,entry,message",
    [
        ("env", {"value": "example"}, "name"),
        ("volumeMounts", {"name": "scratch"}, "mountPath"),
    ],
)
def test_missing_named_container_entries_are_rejected_before_writes(
    submission, template, field, entry, message
):
    template["spec"]["containers"][1][field] = [entry]
    with pytest.raises(ValueError, match=f"require a nonempty {message}"):
        submission.submit(driver_pod_template=template, driver_container_name="spark-main")
    submission.clients.client.assert_not_called()
    assert submission.api.mock_calls == []


def test_explicit_tolerations_extend_template_without_duplicates_or_input_mutation(submission, template):
    template["spec"]["tolerations"] = [{"key": "batch", "operator": "Exists"}]
    before = deepcopy(template)
    submission.submit(
        driver_pod_template=template,
        driver_container_name="spark-main",
        driver_tolerations=[
            k8s.V1Toleration(key="batch", operator="Exists"),
            k8s.V1Toleration(key="dedicated", operator="Equal", value="spark", effect="NoSchedule"),
        ],
    )
    result = submission.api.create_namespaced_pod.call_args.kwargs["body"]
    assert result["spec"]["tolerations"] == [
        {"key": "batch", "operator": "Exists"},
        {"key": "dedicated", "operator": "Equal", "value": "spark", "effect": "NoSchedule"},
    ]
    assert template == before


@pytest.mark.parametrize(
    ("annotation", "reserved"),
    [
        ("other.spark-on-k8s.io/task", False),
        ("spark-on-k8s.io.example/task", False),
        ("example.org/spark-on-k8s.io", False),
        ("spark-on-k8s.io", False),
        ("spark-on-k8s.io/airflow-task", True),
        ("spark-on-k8s.io/custom", True),
        ("spark-on-k8s.io/", True),
    ],
)
def test_reserved_annotation_namespace_is_an_exact_qualified_name(template, annotation, reserved):
    template["metadata"]["annotations"][annotation] = "example"
    if reserved:
        with pytest.raises(ValueError, match="reserved spark-on-k8s.io/"):
            load_driver_pod_template(template)
    else:
        loaded = load_driver_pod_template(template)
        assert loaded["metadata"]["annotations"][annotation] == "example"
