from __future__ import annotations

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import yaml
from kubernetes import client as k8s
from kubernetes.client.exceptions import ApiException
from spark_on_k8s.client import ExecutorInstances, PodResources, SparkOnK8S
from spark_on_k8s.connect import ConnectClientUnavailable, ConnectError, ConnectProfile
from spark_on_k8s.connect_server import (
    ConnectServerConfig,
    ConnectServerCreateError,
    ConnectServerError,
    ConnectServerHandle,
    ConnectServerOwnershipError,
    ConnectServerTimeout,
)


@pytest.fixture
def cluster(monkeypatch):
    """Run the real submission builder with a stateful Kubernetes API fixture."""
    api = MagicMock()
    objects = {"pod": {}, "service": {}}
    counter = iter(range(1, 100))

    def create(kind, namespace, body, **kwargs):
        if isinstance(body, dict):  # A merged driver pod template is an API dictionary.
            body = k8s.ApiClient().deserialize(SimpleNamespace(data=json.dumps(body)), "V1Pod")
        value = deepcopy(body)
        if value.metadata.name in objects[kind]:
            raise ApiException(status=409)
        value.metadata.uid = f"uid-{next(counter)}"
        value.metadata.namespace = namespace
        if kind == "pod":
            value.status = k8s.V1PodStatus(phase="Running")
        else:
            value.spec.cluster_ip = value.spec.cluster_ip or "10.0.0.1"
        objects[kind][value.metadata.name] = value
        return deepcopy(value)

    def read(kind, name, namespace, **kwargs):
        try:
            return deepcopy(objects[kind][name])
        except KeyError:
            raise ApiException(status=404) from None

    def delete(kind, name, namespace, body, **kwargs):
        value = objects[kind].get(name)
        if value is None:
            raise ApiException(status=404)
        if value.metadata.uid != body.preconditions.uid:
            raise ApiException(status=409)
        del objects[kind][name]

    def list_pods(namespace, label_selector, **kwargs):
        wanted = dict(item.split("=", 1) for item in label_selector.split(","))
        return k8s.V1PodList(
            items=[
                deepcopy(pod)
                for pod in objects["pod"].values()
                if pod.metadata.namespace == namespace
                and all((pod.metadata.labels or {}).get(key) == value for key, value in wanted.items())
            ]
        )

    api.list_namespaced_pod.side_effect = list_pods
    for kind in objects:
        getattr(api, f"create_namespaced_{kind}").side_effect = lambda kind=kind, **kwargs: create(
            kind, **kwargs
        )
        getattr(api, f"read_namespaced_{kind}").side_effect = lambda kind=kind, **kwargs: read(kind, **kwargs)
        getattr(api, f"delete_namespaced_{kind}").side_effect = lambda kind=kind, **kwargs: delete(
            kind, **kwargs
        )
    monkeypatch.setattr(k8s, "CoreV1Api", lambda raw: api)
    sdk = SparkOnK8S(k8s_client_manager=MagicMock())
    return sdk, api, objects


def config(**kwargs):
    return ConnectServerConfig(
        **{
            "image": "apache/spark:4.0.5",
            "server_jar": "local:///opt/spark/jars/spark-connect_2.13-4.0.5.jar",
            "namespace": "connect-test",
            "service_account": "spark",
            **kwargs,
        }
    )


def test_create_uses_real_submission_builder_and_private_owned_service(cluster):
    sdk, api, objects = cluster
    cfg = config(
        port=16000,
        image_pull_policy="Never",
        driver_resources=PodResources(memory=768, memory_overhead=256),
        executor_resources=PodResources(cpu=1, memory=512, memory_overhead=256),
        executor_instances=ExecutorInstances(initial=1),
        spark_conf={"spark.sql.shuffle.partitions": "2", "spark.kubernetes.executor.request.cores": "250m"},
    )
    handle = sdk.create_connect_server(cfg)
    assert isinstance(handle, ConnectServerHandle)
    assert len(objects["pod"]) == 1
    assert len(objects["service"]) == 2
    pod = objects["pod"][handle.run.driver_pod]
    args = pod.spec.containers[0].args
    assert args[:3] == ["driver", "--master", "k8s://https://kubernetes.default.svc.cluster.local:443"]
    assert args[args.index("--class") + 1] == "org.apache.spark.sql.connect.service.SparkConnectServer"
    assert args[-1] == cfg.server_jar
    assert "spark.connect.grpc.binding.port=16000" in args
    assert "spark.connect.grpc.binding.address=0.0.0.0" in args
    assert pod.spec.containers[0].image_pull_policy == "Never"
    assert pod.spec.containers[0].resources.limits["memory"] == "1024Mi"
    service = objects["service"][handle.service_name]
    assert service.spec.type == "ClusterIP"
    assert service.spec.ports[0].port == 16000
    assert service.spec.ports[0].target_port == 16000
    assert service.spec.selector == {
        "spark-app-id": handle.run.app_id,
        "spark-role": "driver",
        "spark-on-k8s.io/connect-server": "true",
    }
    assert service.metadata.owner_references[0].uid == handle.run.driver_uid
    assert handle.endpoint == f"sc://{handle.service_name}.connect-test.svc:16000"
    assert ConnectServerHandle.from_dict(handle.to_dict()) == handle
    api.delete_namespaced_pod.assert_not_called()


def test_default_factories_and_collision_resistant_names(cluster):
    first, second = config(), config()
    first.driver_resources.memory = 600
    assert second.driver_resources.memory == 1024
    sdk, _, _ = cluster
    a = sdk.create_connect_server(config(app_name="a" * 100))
    b = sdk.create_connect_server(config(app_name="a" * 100))
    assert a.run.app_id != b.run.app_id
    assert len(a.service_name) == 63
    assert len(a.run.driver_pod) <= 63


@pytest.mark.parametrize(
    "phase, expected",
    [
        ("Pending", "starting"),
        ("Running", "running"),
        ("Failed", "terminated"),
        ("Succeeded", "terminated"),
        ("Unknown", "unknown"),
    ],
)
def test_status_reports_kubernetes_phase_not_query_readiness(cluster, phase, expected):
    sdk, _, objects = cluster
    handle = sdk.create_connect_server(config())
    objects["pod"][handle.run.driver_pod].status.phase = phase
    status = sdk.connect_server_status(handle)
    assert status["status"] == expected
    assert status["query_ready"] is None


def test_status_only_404_is_missing_and_rbac_or_transport_error_propagates(cluster):
    sdk, api, objects = cluster
    handle = sdk.create_connect_server(config())
    del objects["service"][handle.service_name]
    assert sdk.connect_server_status(handle)["missing"] == ["connect_service"]
    for error in (ApiException(status=403), ApiException(status=503), OSError("unavailable")):
        api.read_namespaced_pod.side_effect = error
        with pytest.raises(type(error)):
            sdk.connect_server_status(handle)
    api.delete_namespaced_pod.assert_not_called()


@pytest.mark.parametrize("aware", [True, False])
def test_status_reports_driver_age_and_optional_version_label(cluster, aware):
    from datetime import UTC, datetime, timedelta

    sdk, _, objects = cluster
    handle = sdk.create_connect_server(config())
    initial = sdk.connect_server_status(handle)
    assert (initial["created_at"], initial["age_seconds"], initial["spark_version"]) == (None, None, None)
    created = datetime.now(UTC).replace(microsecond=0) - timedelta(hours=2)
    pod = objects["pod"][handle.run.driver_pod]
    pod.metadata.creation_timestamp = created if aware else created.replace(tzinfo=None)
    pod.metadata.labels["spark-version"] = "4.0.5"
    status = sdk.connect_server_status(handle)
    assert status["created_at"] == created.isoformat().replace("+00:00", "Z")
    assert 7200 <= status["age_seconds"] < 7260
    assert status["spark_version"] == "4.0.5"
    assert status["query_ready"] is None
    pod.metadata.creation_timestamp = datetime.now(UTC) + timedelta(minutes=5)
    assert sdk.connect_server_status(handle)["age_seconds"] == 0, "Clock skew is not a negative age"
    del objects["pod"][handle.run.driver_pod]
    missing = sdk.connect_server_status(handle)
    assert (missing["status"], missing["created_at"], missing["age_seconds"], missing["spark_version"]) == (
        "missing",
        None,
        None,
        None,
    )


def test_list_reports_only_helper_labeled_drivers_oldest_first_without_mutation(cluster):
    from datetime import UTC, datetime, timedelta

    sdk, api, objects = cluster
    newer = sdk.create_connect_server(config(app_name="newer"))
    older = sdk.create_connect_server(config(app_name="older"))
    elsewhere = sdk.create_connect_server(config(namespace="other-team"))
    now = datetime.now(UTC).replace(microsecond=0)
    objects["pod"][older.run.driver_pod].metadata.creation_timestamp = now - timedelta(days=3, hours=4)
    objects["pod"][older.run.driver_pod].metadata.labels["spark-version"] = "4.0.5"
    objects["pod"][newer.run.driver_pod].metadata.creation_timestamp = now - timedelta(minutes=10)
    objects["pod"][newer.run.driver_pod].metadata.deletion_timestamp = now
    objects["pod"][newer.run.driver_pod].status.phase = "Running"
    # An ordinary batch driver and a labeled pod without the helper's application ID are not servers.
    objects["pod"]["batch-driver"] = k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            name="batch-driver",
            namespace="connect-test",
            uid="batch-uid",
            labels={"spark-role": "driver", "spark-app-id": "batch"},
        ),
        status=k8s.V1PodStatus(phase="Running"),
    )
    objects["pod"]["unidentified"] = k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            name="unidentified",
            namespace="connect-test",
            uid="unidentified-uid",
            labels={"spark-role": "driver", "spark-on-k8s.io/connect-server": "true"},
        ),
    )
    servers = sdk.list_connect_servers("connect-test", timeout=7)
    assert [server["app_id"] for server in servers] == [older.run.app_id, newer.run.app_id]
    assert servers[0] == {
        "app_id": older.run.app_id,
        "name": "older",
        "namespace": "connect-test",
        "driver_pod": older.run.driver_pod,
        "driver_uid": older.run.driver_uid,
        "status": "running",
        "driver_phase": "Running",
        "spark_version": "4.0.5",
        "image": "apache/spark:4.0.5",
        "created_at": (now - timedelta(days=3, hours=4)).isoformat().replace("+00:00", "Z"),
        "age_seconds": servers[0]["age_seconds"],
    }
    assert 273600 <= servers[0]["age_seconds"] < 273660
    assert (servers[1]["status"], servers[1]["driver_phase"], servers[1]["spark_version"]) == (
        "deleting",
        "Running",
        None,
    )
    call = api.list_namespaced_pod.call_args.kwargs
    assert call["namespace"] == "connect-test"
    assert call["label_selector"] == "spark-role=driver,spark-on-k8s.io/connect-server=true"
    assert 0 < call["_request_timeout"] <= 7
    assert [server["app_id"] for server in sdk.list_connect_servers("other-team")] == [elsewhere.run.app_id]
    assert sdk.list_connect_servers("empty") == []
    api.delete_namespaced_pod.assert_not_called()
    api.delete_namespaced_service.assert_not_called()
    api.patch_namespaced_pod.assert_not_called()


def test_list_validates_before_requests_and_propagates_kubernetes_errors(cluster):
    sdk, api, _ = cluster
    for namespace, timeout in (("Not/A-Namespace", 10), ("spark", 0), ("spark", float("nan"))):
        with pytest.raises(ValueError):
            sdk.list_connect_servers(namespace, timeout=timeout)
    api.list_namespaced_pod.assert_not_called()
    for error in (ApiException(status=403), ApiException(status=503), OSError("unavailable")):
        api.list_namespaced_pod.side_effect = error
        with pytest.raises(type(error)):
            sdk.list_connect_servers("connect-test")


def test_readiness_requires_query_identity_then_revalidates_resources(cluster, monkeypatch):
    sdk, api, _ = cluster
    handle = sdk.create_connect_server(config())
    profile = ConnectProfile(endpoint="sc://localhost:15002", tls=False)
    probe = MagicMock(
        return_value={
            "schema_version": 1,
            "status": "ok",
            "spark_version": "4.0.5",
            "app_id": handle.run.app_id,
        }
    )
    monkeypatch.setattr(ConnectProfile, "probe", probe)
    before = api.read_namespaced_pod.call_count
    result = sdk.wait_connect_server_ready(handle, profile=profile)
    assert result["status"] == "ready" and result["query_ready"] is True
    assert probe.call_args.kwargs["expected_app_id"] == handle.run.app_id
    assert api.read_namespaced_pod.call_count == before + 2


def test_readiness_reports_queried_versions_and_skew_without_failing(cluster, monkeypatch):
    from spark_on_k8s.connect import version_warnings

    sdk, api, objects = cluster
    handle = sdk.create_connect_server(config())
    objects["pod"][handle.run.driver_pod].metadata.labels["spark-version"] = "label-is-not-authoritative"
    warnings = version_warnings("4.3.0", "4.0.5")
    probe = MagicMock(
        return_value={
            "schema_version": 1,
            "status": "ok",
            "spark_version": "4.0.5",
            "client_version": "4.3.0",
            "warnings": warnings,
            "app_id": handle.run.app_id,
        }
    )
    monkeypatch.setattr(ConnectProfile, "probe", probe)
    result = sdk.wait_connect_server_ready(
        handle, profile=ConnectProfile(endpoint="sc://localhost", tls=False)
    )
    assert result["query_ready"] is True
    assert (result["spark_version"], result["client_version"]) == ("4.0.5", "4.3.0")
    assert result["warnings"] == warnings and warnings[0]["code"] == "version_skew"
    api.delete_namespaced_pod.assert_not_called()


def test_readiness_detects_resource_replacement_during_query(cluster, monkeypatch):
    sdk, _, objects = cluster
    handle = sdk.create_connect_server(config())

    def probe(self, **kwargs):
        objects["service"][handle.service_name].metadata.uid = "replacement"
        return {"app_id": handle.run.app_id}

    monkeypatch.setattr(ConnectProfile, "probe", probe)
    with pytest.raises(ConnectServerOwnershipError):
        sdk.wait_connect_server_ready(handle, profile=ConnectProfile(endpoint="sc://localhost", tls=False))
    assert handle.run.driver_pod in objects["pod"]


@pytest.mark.parametrize("pending", [True, False])
def test_readiness_timeout_never_deletes(cluster, monkeypatch, pending):
    sdk, api, objects = cluster
    handle = sdk.create_connect_server(config())
    if pending:
        objects["pod"][handle.run.driver_pod].status.phase = "Pending"
    monkeypatch.setattr(ConnectProfile, "probe", MagicMock(side_effect=ConnectError("not ready")))
    with pytest.raises(ConnectServerTimeout) as error:
        sdk.wait_connect_server_ready(
            handle,
            profile=ConnectProfile(endpoint="sc://localhost", tls=False),
            timeout=0.02,
            poll_interval=0.001,
        )
    assert error.value.handle == handle
    assert error.value.cleanup["status"] == "not_attempted"
    api.delete_namespaced_pod.assert_not_called()
    api.delete_namespaced_service.assert_not_called()


def test_readiness_without_a_local_client_fails_without_polling_or_deleting(cluster, monkeypatch):
    sdk, api, objects = cluster
    handle = sdk.create_connect_server(config())
    probe = MagicMock(side_effect=ConnectClientUnavailable("Install spark-on-k8s[connect]"))
    monkeypatch.setattr(ConnectProfile, "probe", probe)
    with pytest.raises(ConnectClientUnavailable, match="Install spark-on-k8s"):
        sdk.wait_connect_server_ready(
            handle, profile=ConnectProfile(endpoint="sc://localhost", tls=False), timeout=60
        )
    probe.assert_called_once()
    api.delete_namespaced_pod.assert_not_called()
    assert handle.run.driver_pod in objects["pod"]


def test_terminal_server_is_not_reported_ready(cluster, monkeypatch):
    sdk, _, objects = cluster
    handle = sdk.create_connect_server(config())
    objects["pod"][handle.run.driver_pod].status.phase = "Failed"
    probe = MagicMock()
    monkeypatch.setattr(ConnectProfile, "probe", probe)
    with pytest.raises(ConnectServerError, match="terminated"):
        sdk.wait_connect_server_ready(handle, profile=ConnectProfile(endpoint="sc://localhost", tls=False))
    probe.assert_not_called()


def test_delete_checks_every_identity_before_any_mutation(cluster):
    sdk, api, objects = cluster
    handle = sdk.create_connect_server(config())
    objects["service"][handle.service_name].metadata.owner_references[0].uid = "someone-else"
    with pytest.raises(ConnectServerOwnershipError):
        sdk.delete_connect_server(handle)
    api.delete_namespaced_pod.assert_not_called()
    api.delete_namespaced_service.assert_not_called()


def test_delete_uses_uid_preconditions_and_missing_resources_are_idempotent(cluster):
    sdk, api, objects = cluster
    handle = sdk.create_connect_server(config())
    del objects["service"][handle.service_name]
    result = sdk.delete_connect_server(handle)
    assert result["status"] == "deletion_requested"
    assert len(result["requested"]) == 2
    assert api.delete_namespaced_pod.call_args.kwargs["body"].preconditions.uid == handle.run.driver_uid
    assert (
        api.delete_namespaced_service.call_args.kwargs["body"].preconditions.uid == handle.driver_service_uid
    )
    assert sdk.delete_connect_server(handle)["status"] == "absent"


def test_service_creation_failure_retains_identity_and_requests_scoped_rollback(cluster):
    sdk, api, _ = cluster
    original = api.create_namespaced_service.side_effect
    failure = ApiException(status=403)

    def create(**kwargs):
        if kwargs["body"].metadata.name.endswith("-connect"):
            raise failure
        return original(**kwargs)

    api.create_namespaced_service.side_effect = create
    with pytest.raises(ConnectServerCreateError) as error:
        sdk.create_connect_server(config())
    assert error.value.__cause__ is failure
    assert error.value.phase == "grpc_service_creation"
    assert error.value.run.driver_uid
    assert error.value.handle is None
    assert error.value.cleanup["status"] == "requested"
    assert api.delete_namespaced_pod.call_args.kwargs["body"].preconditions.uid == error.value.run.driver_uid


def test_service_failure_rolls_back_driver_when_submission_normalizes_the_name(cluster):
    sdk, api, objects = cluster
    original = api.create_namespaced_service.side_effect

    def create(**kwargs):
        if kwargs["body"].metadata.name.endswith("-connect"):
            raise ApiException(status=403)
        return original(**kwargs)

    api.create_namespaced_service.side_effect = create
    with pytest.raises(ConnectServerCreateError) as error:
        # Valid configuration; submission collapses repeated separators in the application ID.
        sdk.create_connect_server(config(app_name="team--notebook"))
    assert error.value.cleanup["status"] == "requested"
    assert error.value.run.driver_uid
    assert error.value.run.app_id.startswith("team-notebook-")
    assert objects["pod"] == {}
    assert api.delete_namespaced_pod.call_args.kwargs["name"] == error.value.run.driver_pod


@pytest.mark.parametrize("value", [True, 0, -1, float("nan"), float("inf"), None])
def test_timeout_validation_happens_before_requests(cluster, value):
    sdk, api, _ = cluster
    handle = sdk.create_connect_server(config())
    api.reset_mock()
    with pytest.raises(ValueError):
        sdk.connect_server_status(handle, timeout=value)
    with pytest.raises(ValueError):
        sdk.delete_connect_server(handle, timeout=value)
    api.read_namespaced_pod.assert_not_called()


@pytest.mark.parametrize(
    "settings",
    [
        {"namespace": "not.a.namespace"},
        {"image": ""},
        {"server_jar": "--packages"},
        {"app_name": "123"},
        {"image_pull_policy": "sometimes"},
        {"driver_resources": {}},
        {"executor_instances": {}},
        {"executor_instances": ExecutorInstances(min=3, max=2)},
        {"executor_instances": ExecutorInstances(min=2, initial=1)},
        {"executor_instances": ExecutorInstances(initial=3, max=2)},
        {"executor_instances": ExecutorInstances(executor_allocation_ratio=float("nan"))},
        {"executor_instances": ExecutorInstances(scheduler_backlog_timeout="0s")},
        {"driver_node_selector": {"node": 1}},
        {"driver_node_selector": {"pool name": "ml"}},
        {"executor_node_selector": {"pool": "-gpu"}},
        {"executor_node_selector": {"Example.com/pool": "ml"}},
        {"executor_node_selector": {"pool": "x" * 64}},
        {"executor_node_selector": ["pool=ml"]},
        {"image_pull_secrets": "registry"},
        {"image_pull_secrets": ["Registry"]},
        {"image_pull_secrets": ["registry", "registry"]},
        {"driver_env_from_secrets": {"API_TOKEN": "credentials"}},
        {"driver_env_from_secrets": {"API_TOKEN": "Credentials:token"}},
        {"driver_env_from_secrets": {"API_TOKEN": "credentials:to/ken"}},
        {"driver_env_from_secrets": {"1TOKEN": "credentials:token"}},
        {"driver_env_from_secrets": {"API-TOKEN": "credentials:token"}},
        {"executor_env_from_secrets": {"SPARK_USER": "credentials:user"}},
        {"executor_env_from_secrets": {"TOKEN": ["credentials", "token"]}},
        {
            "executor_env_from_secrets": {"TOKEN": "credentials:token"},
            "spark_conf": {"spark.executorEnv.TOKEN": "literal"},
        },
        {"driver_tolerations": {"key": "pool"}},
        {"driver_tolerations": ["pool:NoSchedule"]},
        {"driver_tolerations": [{"key": "pool", "operator": "In"}]},
        {"driver_tolerations": [{"key": "pool", "operator": ["Exists"]}]},
        {"driver_tolerations": [{"value": "ml"}]},
        {"driver_tolerations": [{"key": "pool", "operator": "Exists", "value": "ml"}]},
        {"driver_tolerations": [{"key": "pool", "value": "not valid"}]},
        {"driver_tolerations": [{"key": "pool", "effect": "Never"}]},
        {"driver_tolerations": [{"key": "pool", "effect": "NoSchedule", "tolerationSeconds": 60}]},
        {"executor_tolerations": [{"key": "pool", "effect": "NoExecute", "tolerationSeconds": "60"}]},
        {"executor_tolerations": [{"key": "pool", "toleration_seconds": 60}]},
        {"spark_conf": {"spark.kubernetes.driver.podTemplateFile": "other.yaml"}},
        {"spark_conf": {"spark.kubernetes.container.image.pullSecrets": "registry"}},
        {"spark_conf": {"spark.kubernetes.executor.secretKeyRef.TOKEN": "credentials:token"}},
        {"spark_conf": {"spark.kubernetes.executor.request.cores": "invalid"}},
        {"spark_conf": {"spark.kubernetes.executor.request.cores": "0"}},
        {"spark_conf": {"spark.kubernetes.executor.request.cores": "2"}},
        {"packages": ["unversioned"]},
    ],
)
def test_invalid_configuration_is_rejected_without_kubernetes_access(cluster, settings):
    _, api, _ = cluster
    with pytest.raises(ValueError):
        config(**settings)
    api.create_namespaced_pod.assert_not_called()
    api.create_namespaced_service.assert_not_called()


def test_start_status_list_and_delete_do_not_import_pyspark(cluster, monkeypatch):
    import builtins

    sdk, _, _ = cluster
    original = builtins.__import__

    def import_without_pyspark(name, *args, **kwargs):
        if name == "pyspark" or name.startswith("pyspark."):
            raise AssertionError("Server lifecycle must not import PySpark")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_pyspark)
    handle = sdk.create_connect_server(config())
    assert sdk.connect_server_status(handle)["status"] == "running"
    assert sdk.list_connect_servers("connect-test")[0]["app_id"] == handle.run.app_id
    assert sdk.delete_connect_server(handle)["status"] == "deletion_requested"


def test_missing_driver_service_during_create_exposes_confirmed_run(cluster):
    sdk, api, _ = cluster
    api.read_namespaced_service.side_effect = ApiException(status=404)
    with pytest.raises(ConnectServerCreateError) as error:
        sdk.create_connect_server(config())
    assert error.value.phase == "driver_service_verification"
    assert error.value.run.driver_uid
    assert error.value.cleanup["status"] == "requested"
    assert api.create_namespaced_service.call_count == 1


def test_failed_readiness_query_can_recover_without_creating_a_second_server(cluster, monkeypatch):
    sdk, api, _ = cluster
    handle = sdk.create_connect_server(config())
    probe = MagicMock(side_effect=[ConnectError("starting"), {"app_id": handle.run.app_id}])
    monkeypatch.setattr(ConnectProfile, "probe", probe)
    result = sdk.wait_connect_server_ready(
        handle, profile=ConnectProfile(endpoint="sc://localhost", tls=False), poll_interval=0.001
    )
    assert result["query_ready"] is True
    assert probe.call_count == 2
    assert api.create_namespaced_pod.call_count == 1


def test_invalid_operation_types_fail_before_cluster_access(cluster):
    sdk, api, _ = cluster
    with pytest.raises(ValueError):
        sdk.create_connect_server({})
    with pytest.raises(ValueError):
        sdk.connect_server_status({})
    with pytest.raises(ValueError):
        sdk.delete_connect_server({})
    with pytest.raises(ValueError):
        sdk.wait_connect_server_ready({}, profile={})
    with pytest.raises(ValueError):
        sdk.wait_connect_server_ready(
            {}, profile=ConnectProfile(endpoint="sc://localhost"), poll_interval=True
        )
    api.read_namespaced_pod.assert_not_called()
    api.create_namespaced_pod.assert_not_called()


def test_interrupted_rollback_preserves_uncertain_deletion_and_does_not_retry(cluster):
    sdk, api, objects = cluster
    original_create = api.create_namespaced_service.side_effect
    original_delete = api.delete_namespaced_pod.side_effect
    interrupted = KeyboardInterrupt()

    def create(**kwargs):
        if kwargs["body"].metadata.name.endswith("-connect"):
            raise ApiException(status=403)
        return original_create(**kwargs)

    def delete(**kwargs):
        original_delete(**kwargs)  # The API accepted the deletion before the caller was interrupted.
        raise interrupted

    api.create_namespaced_service.side_effect = create
    api.delete_namespaced_pod.side_effect = delete
    with pytest.raises(KeyboardInterrupt) as error:
        sdk.create_connect_server(config())
    assert error.value is interrupted
    assert error.value.run.driver_uid
    assert error.value.phase == "rollback"
    assert error.value.cleanup["status"] == "unknown"
    assert error.value.run.driver_pod not in objects["pod"]
    assert api.delete_namespaced_pod.call_count == 1


@pytest.mark.parametrize("wrapped", [False, True])
@pytest.mark.parametrize("expired", [False, True])
@pytest.mark.parametrize("timeout_type", ["read", "connect"])
def test_kubernetes_transport_timeout_becomes_wait_timeout_only_after_deadline(
    cluster, monkeypatch, wrapped, expired, timeout_type
):
    from urllib3.exceptions import ConnectTimeoutError, MaxRetryError, ReadTimeoutError

    sdk, api, objects = cluster
    handle = sdk.create_connect_server(config())
    clock = [100.0]
    monkeypatch.setattr("spark_on_k8s.utils.run.time.monotonic", lambda: clock[0])
    reason = (
        ReadTimeoutError(None, "/api/v1/pods", "read timeout")
        if timeout_type == "read"
        else ConnectTimeoutError("connection timed out")
    )
    failure = MaxRetryError(None, "/api/v1/pods", reason=reason) if wrapped else reason

    def timed_out(**kwargs):
        assert kwargs["_request_timeout"] == 3
        clock[0] = 104.0 if expired else 101.0
        raise failure

    api.read_namespaced_pod.side_effect = timed_out
    with pytest.raises(ConnectServerTimeout if expired else type(failure)) as error:
        sdk.wait_connect_server_ready(
            handle, profile=ConnectProfile(endpoint="sc://localhost", tls=False), timeout=3
        )
    if expired:
        assert error.value.handle == handle
        assert error.value.cleanup["status"] == "not_attempted"
    else:
        assert error.value is failure
    assert handle.run.driver_pod in objects["pod"]
    assert handle.service_name in objects["service"]
    api.delete_namespaced_pod.assert_not_called()
    api.delete_namespaced_service.assert_not_called()


@pytest.mark.parametrize("kind", ["connection", "permission", "unavailable"])
def test_expired_wait_does_not_hide_unrelated_kubernetes_errors(cluster, monkeypatch, kind):
    from urllib3.exceptions import MaxRetryError, NewConnectionError

    sdk, api, _ = cluster
    handle = sdk.create_connect_server(config())
    clock = [100.0]
    monkeypatch.setattr("spark_on_k8s.utils.run.time.monotonic", lambda: clock[0])
    failure = (
        MaxRetryError(None, "/api/v1/pods", reason=NewConnectionError(None, "connection refused"))
        if kind == "connection"
        else ApiException(status=403 if kind == "permission" else 503)
    )

    def failed(**kwargs):
        clock[0] = 104.0
        raise failure

    api.read_namespaced_pod.side_effect = failed
    with pytest.raises(type(failure)) as error:
        sdk.wait_connect_server_ready(
            handle, profile=ConnectProfile(endpoint="sc://localhost", tls=False), timeout=3
        )
    assert error.value is failure
    api.delete_namespaced_pod.assert_not_called()


def test_bounded_helper_clients_disable_retries_without_changing_other_settings_or_cached_pools(
    cluster, monkeypatch
):
    from contextlib import contextmanager

    sdk, _, _ = cluster
    handle = sdk.create_connect_server(config())
    cfg = k8s.Configuration()
    cfg.host = "https://localhost:6443"
    cfg.api_key["authorization"] = "test-credential"
    seen = []

    @contextmanager
    def client():
        with k8s.ApiClient(configuration=cfg) as raw:
            pool_manager = raw.rest_client.pool_manager
            original_settings = dict(pool_manager.connection_pool_kw)
            cached = pool_manager.connection_from_url(cfg.host)
            assert cached.retries.total == 3
            yield raw
            current = pool_manager.connection_from_url(cfg.host)
            seen.append((cached, current))
            assert current is not cached
            assert current.retries == 0
            assert cached.retries.total == 3
            assert pool_manager.connection_pool_kw == {**original_settings, "retries": 0}
            assert raw.configuration.api_key["authorization"] == "test-credential"

    monkeypatch.setattr(sdk.k8s_client_manager, "client", client)
    assert sdk.connect_server_status(handle)["status"] == "running"
    assert sdk.delete_connect_server(handle)["status"] == "deletion_requested"
    assert len(seen) == 2
    assert cfg.retries is None
    with k8s.ApiClient(configuration=cfg) as unrelated:
        assert unrelated.rest_client.pool_manager.connection_from_url(cfg.host).retries.total == 3


POD_SETTINGS = {
    "image_pull_secrets": ["private-registry", "mirror-registry"],
    "driver_env_from_secrets": {"API_TOKEN": "notebook-credentials:api-token"},
    "executor_env_from_secrets": {
        "API_TOKEN": "notebook-credentials:api-token",
        "AWS_SECRET_ACCESS_KEY": "s3-credentials:secret.key",
    },
    "driver_node_selector": {"kubernetes.io/os": "linux"},
    "executor_node_selector": {"example.com/pool": "gpu", "nvidia.com/gpu.present": "true"},
    "driver_tolerations": [{"key": "dedicated", "operator": "Equal", "value": "ml", "effect": "NoSchedule"}],
    "executor_tolerations": [
        {"key": "nvidia.com/gpu", "operator": "Exists", "effect": "NoSchedule"},
        {"key": "spot", "operator": "Exists", "effect": "NoExecute", "tolerationSeconds": 300},
    ],
}


def test_pod_settings_use_existing_submission_mechanisms_without_reading_secrets(cluster):
    sdk, api, objects = cluster
    handle = sdk.create_connect_server(config(**deepcopy(POD_SETTINGS)))
    pod = objects["pod"][handle.run.driver_pod]
    assert [secret.name for secret in pod.spec.image_pull_secrets] == ["private-registry", "mirror-registry"]
    assert pod.spec.node_selector == {"kubernetes.io/os": "linux"}
    assert [(t.key, t.operator, t.value, t.effect) for t in pod.spec.tolerations] == [
        ("dedicated", "Equal", "ml", "NoSchedule")
    ]
    driver = pod.spec.containers[0]
    assert driver.name == "driver"
    reference = next(env for env in driver.env if env.name == "API_TOKEN")
    assert reference.value is None
    assert (reference.value_from.secret_key_ref.name, reference.value_from.secret_key_ref.key) == (
        "notebook-credentials",
        "api-token",
    )
    # The managed bind address and labels survive the driver template merge.
    assert any(env.name == "SPARK_DRIVER_BIND_ADDRESS" for env in driver.env)
    assert pod.metadata.labels["spark-on-k8s.io/connect-server"] == "true"
    for setting in (
        "spark.kubernetes.container.image.pullSecrets=private-registry,mirror-registry",
        "spark.kubernetes.executor.secretKeyRef.API_TOKEN=notebook-credentials:api-token",
        "spark.kubernetes.executor.secretKeyRef.AWS_SECRET_ACCESS_KEY=s3-credentials:secret.key",
        "spark.kubernetes.executor.node.selector.example.com/pool=gpu",
        "spark.kubernetes.executor.node.selector.nvidia.com/gpu.present=true",
        "spark.kubernetes.executor.podTemplateFile=/opt/spark/executor-template/executor-template.yaml",
    ):
        assert setting in driver.args
    configmap = api.create_namespaced_config_map.call_args.kwargs["body"]
    assert yaml.safe_load(configmap.data["executor-template.yaml"]) == {
        "apiVersion": "v1",
        "kind": "Pod",
        "spec": {"tolerations": POD_SETTINGS["executor_tolerations"]},
    }
    owner = api.patch_namespaced_config_map.call_args.kwargs["body"].metadata.owner_references[0]
    assert owner.uid == handle.run.driver_uid
    api.create_namespaced_secret.assert_not_called()
    api.read_namespaced_secret.assert_not_called()
    api.list_namespaced_secret.assert_not_called()
    assert sdk.connect_server_status(handle)["status"] == "running"


def test_default_server_keeps_the_generated_driver_pod_without_templates(cluster):
    sdk, api, _ = cluster
    sdk.create_connect_server(config())
    body = api.create_namespaced_pod.call_args.kwargs["body"]
    assert not isinstance(body, dict)
    assert body.spec.image_pull_secrets is None
    assert body.spec.tolerations == []
    api.create_namespaced_config_map.assert_not_called()
    assert not any(
        "secretKeyRef" in arg or "pullSecrets" in arg or "podTemplateFile" in arg
        for arg in body.spec.containers[0].args
    )


@pytest.mark.parametrize(
    "toleration",
    [
        {"operator": "Exists"},
        {"key": "pool", "operator": "Exists"},
        {"key": "pool"},
        {"key": "pool", "value": ""},
        {"key": "example.com/pool", "value": "ml.v2_a", "effect": "PreferNoSchedule"},
        {"key": "spot", "effect": "NoExecute", "tolerationSeconds": 0},
    ],
)
def test_valid_kubernetes_toleration_shapes_are_accepted(toleration):
    assert config(driver_tolerations=[toleration], executor_tolerations=[toleration])


def test_create_and_plan_revalidate_pod_settings_changed_after_construction(cluster):
    sdk, api, _ = cluster
    cfg = config(driver_tolerations=[{"key": "pool", "operator": "Exists"}])
    cfg.driver_tolerations.append({"key": "pool", "operator": "Never"})
    with pytest.raises(ValueError, match="operator"):
        sdk.create_connect_server(cfg)
    cfg = config(executor_env_from_secrets={"TOKEN": "credentials:token"})
    cfg.executor_env_from_secrets["SPARK_EXECUTOR_ID"] = "credentials:token"
    with pytest.raises(ValueError, match="SPARK_"):
        sdk.plan_connect_server(cfg)
    api.create_namespaced_pod.assert_not_called()
    api.create_namespaced_config_map.assert_not_called()


def test_plan_is_offline_and_shows_secret_references_but_no_values():
    manager = MagicMock()
    sdk = SparkOnK8S(k8s_client_manager=manager)
    cfg = config(app_name="notebook", port=16000, **deepcopy(POD_SETTINGS))
    plan = sdk.plan_connect_server(cfg)
    manager.client.assert_not_called()
    assert plan.app_id == "notebook-preview"
    assert [resource["kind"] for resource in plan.resources] == ["ConfigMap", "Pod", "Service", "Service"]
    grpc = plan.resources[-1]
    assert grpc["metadata"]["name"] == "notebook-preview-connect"
    assert grpc["spec"]["type"] == "ClusterIP"
    assert grpc["spec"]["ports"] == [{"name": "grpc", "port": 16000, "protocol": "TCP", "targetPort": 16000}]
    assert grpc["spec"]["selector"] == {
        "spark-app-id": "notebook-preview",
        "spark-role": "driver",
        "spark-on-k8s.io/connect-server": "true",
    }
    assert "ownerReferences" not in grpc["metadata"]
    pod = plan.resources[1]
    assert pod["spec"]["imagePullSecrets"] == [{"name": "private-registry"}, {"name": "mirror-registry"}]
    assert pod["spec"]["nodeSelector"] == {"kubernetes.io/os": "linux"}
    # No Secret object is planned or read: only Secret names and keys exist in the plan.
    assert all(resource["kind"] != "Secret" for resource in plan.resources)
    shown = plan.to_dict()
    assert shown["preview"] is True
    driver = next(item for item in shown["resources"] if item["kind"] == "Pod")["spec"]["containers"][0]
    assert {
        "name": "API_TOKEN",
        "valueFrom": {"secretKeyRef": {"name": "notebook-credentials", "key": "api-token"}},
    } in driver["env"]
    assert cfg.driver_env_from_secrets == POD_SETTINGS["driver_env_from_secrets"]


def test_plan_matches_the_created_driver_pod_settings(cluster):
    sdk, _, objects = cluster
    cfg = config(**deepcopy(POD_SETTINGS))
    planned = next(item for item in sdk.plan_connect_server(cfg).resources if item["kind"] == "Pod")
    handle = sdk.create_connect_server(cfg)
    with k8s.ApiClient() as serializer:
        created = serializer.sanitize_for_serialization(objects["pod"][handle.run.driver_pod])
    for field in ("imagePullSecrets", "nodeSelector", "tolerations"):
        assert planned["spec"][field] == created["spec"][field]
    assert planned["spec"]["containers"][0]["env"] == created["spec"]["containers"][0]["env"]
