from __future__ import annotations

from copy import deepcopy
from unittest.mock import MagicMock

import pytest
from kubernetes import client as k8s
from kubernetes.client.exceptions import ApiException
from spark_on_k8s.client import ExecutorInstances, PodResources, SparkOnK8S
from spark_on_k8s.connect_server import ConnectServerConfig
from spark_on_k8s.utils.configuration import Configuration
from spark_on_k8s_dbt.thrift_server import ThriftServer, ThriftServerError


@pytest.fixture
def cluster(monkeypatch):
    """Use the real SDK submission builder with stateful Kubernetes identity/preconditions."""
    api = MagicMock()
    objects = {"pod": {}, "service": {}, "config_map": {}}
    counter = iter(range(1, 100))
    namespace = k8s.V1Namespace(metadata=k8s.V1ObjectMeta(name="dbt-test", uid="namespace-uid"))
    api.read_namespace.side_effect = lambda **kwargs: deepcopy(namespace)

    def create(kind, namespace, body, **kwargs):
        value = deepcopy(body)
        if value.metadata.name in objects[kind]:
            raise ApiException(status=409)
        version = str(next(counter))
        value.metadata.uid = f"uid-{version}"
        value.metadata.resource_version = version
        value.metadata.namespace = namespace
        if kind == "pod":
            value.status = k8s.V1PodStatus(phase="Running")
        elif kind == "service":
            value.spec.cluster_ip = value.spec.cluster_ip or "10.0.0.10"
        objects[kind][value.metadata.name] = value
        return deepcopy(value)

    def read(kind, name, namespace, **kwargs):
        if name not in objects[kind]:
            raise ApiException(status=404)
        return deepcopy(objects[kind][name])

    def delete(kind, name, namespace, body, **kwargs):
        if name not in objects[kind]:
            raise ApiException(status=404)
        value = objects[kind][name]
        if (value.metadata.uid, value.metadata.resource_version) != (
            body.preconditions.uid,
            body.preconditions.resource_version,
        ):
            raise ApiException(status=409)
        del objects[kind][name]

    for kind in objects:
        getattr(api, f"create_namespaced_{kind}").side_effect = lambda kind=kind, **kwargs: create(
            kind, **kwargs
        )
        getattr(api, f"read_namespaced_{kind}").side_effect = lambda kind=kind, **kwargs: read(kind, **kwargs)
        getattr(api, f"delete_namespaced_{kind}").side_effect = lambda kind=kind, **kwargs: delete(
            kind, **kwargs
        )
        getattr(api, f"list_namespaced_{kind}").side_effect = lambda kind=kind, **kwargs: MagicMock(
            items=deepcopy(list(objects[kind].values()))
        )
    monkeypatch.setattr(k8s, "CoreV1Api", lambda raw: api)
    sdk = SparkOnK8S(k8s_client_manager=MagicMock())
    return sdk, api, objects, namespace


def configuration(**overrides):
    return ConnectServerConfig(
        **{
            "image": "spark-test:4.3.0",
            "server_jar": "local:///opt/spark/jars/spark-hive-thriftserver_2.13-4.3.0.jar",
            "namespace": "dbt-test",
            "service_account": "spark",
            "port": 10000,
            "app_name": "dbt-thrift",
            "driver_resources": PodResources(memory=768),
            "executor_resources": PodResources(memory=512),
            "executor_instances": ExecutorInstances(initial=1),
            "spark_conf": {"spark.sql.warehouse.dir": "hdfs://warehouse:8020/user/hive/warehouse"},
            **overrides,
        }
    )


def backend(cluster, config=None):
    records = []
    return ThriftServer(cluster[0], config or configuration(), records.append), records


def no_deletion(api):
    api.delete_namespaced_service.assert_not_called()
    api.delete_namespaced_pod.assert_not_called()
    api.delete_namespace.assert_not_called()
    api.delete_namespaced_service_account.assert_not_called()


def test_real_sdk_submission_is_private_and_neutralizes_ambient_defaults(cluster, monkeypatch):
    monkeypatch.setattr(
        Configuration, "SPARK_ON_K8S_SPARK_CONF", {"spark.hadoop.hive.server2.thrift.port": "22"}
    )
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_PACKAGES", "unexpected:dependency:1")
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_SECRET_ENV_VAR", {"UNEXPECTED_SECRET": "do-not-submit"})
    server, records = backend(cluster)
    handle = server.create()
    _, api, objects, _ = cluster
    assert handle["stage"] == "created"
    assert records[0]["stage"] == "creating" and "driver_uid" not in records[0]["run"]
    assert records[-1] == handle
    pod = objects["pod"][handle["run"]["driver_pod"]]
    args = pod.spec.containers[0].args
    assert args[args.index("--class") + 1] == "org.apache.spark.sql.hive.thriftserver.HiveThriftServer2"
    assert "spark.hadoop.hive.server2.thrift.port=10000" in args
    assert "spark.hadoop.hive.server2.transport.mode=binary" in args
    assert "spark.sql.warehouse.dir=hdfs://warehouse:8020/user/hive/warehouse" in args
    assert not any("unexpected" in arg or "do-not-submit" in arg or "spark.connect" in arg for arg in args)
    api.create_namespaced_secret.assert_not_called()
    assert len(objects["service"]) == 2
    service = objects["service"][handle["service"]["name"]]
    assert service.spec.type == "ClusterIP" and service.spec.external_ips is None
    assert service.spec.selector == {
        "spark-app-id": handle["run"]["app_id"],
        "spark-role": "driver",
        "spark-on-k8s.io/dbt-thrift-server": "true",
    }
    assert service.metadata.owner_references[0].uid == handle["run"]["driver_uid"]
    assert ThriftServer(cluster[0], None, records.append).status(handle) == {
        "status": "running",
        "driver_phase": "Running",
        "missing": [],
        "query_ready": None,
    }
    no_deletion(api)


@pytest.mark.parametrize(
    "conf",
    [
        {"spark.hadoop.hive.server2.thrift.port": "10001"},
        {"spark.hadoop.hive.server2.transport.mode": "http"},
        {"spark.hadoop.hive.server2.authentication": "CUSTOM"},
        {"spark.hadoop.hive.server2.enable.doAs": "true"},
        {"hive.server2.thrift.bind.host": "127.0.0.1"},
        {"spark.sql.hive.thriftServer.singleSession": "true"},
        {"spark.sql.catalogImplementation": "in-memory"},
    ],
)
def test_managed_thrift_configuration_cannot_be_overridden(cluster, conf):
    with pytest.raises(ValueError, match="managed Thrift"):
        backend(cluster, configuration(spark_conf=conf))
    cluster[1].create_namespaced_pod.assert_not_called()


def test_requires_thrift_port_and_creation_config(cluster):
    with pytest.raises(ValueError, match="10000"):
        backend(cluster, configuration(port=15002))
    with pytest.raises(ValueError, match="explicit config"):
        ThriftServer(cluster[0], None, lambda value: None).create()


@pytest.mark.parametrize("phase", ["pod", "driver_service", "thrift_service"])
def test_lost_creation_response_records_partial_identity_without_retry_or_adoption(cluster, phase):
    server, records = backend(cluster)
    _, api, objects, _ = cluster
    method = api.create_namespaced_pod if phase == "pod" else api.create_namespaced_service
    original = method.side_effect

    def uncertain(**kwargs):
        value = original(**kwargs)
        if phase == "pod" or (kwargs["body"].metadata.name.endswith("-thrift")) == (
            phase == "thrift_service"
        ):
            raise TimeoutError("response lost after resource creation")
        return value

    method.side_effect = uncertain
    with pytest.raises(ThriftServerError) as caught:
        server.create()
    handle = caught.value.handle
    assert handle["stage"] == "creation_uncertain" and records[-1] == handle
    assert len(objects["pod"]) == 1
    assert len(objects["service"]) == {"pod": 0, "driver_service": 1, "thrift_service": 2}[phase]
    assert ("driver_uid" in handle["run"]) == (phase != "pod")
    assert ("driver_service" in handle) == (phase == "thrift_service")
    assert "service" not in handle
    with pytest.raises(ThriftServerError):
        server.delete(handle)
    with pytest.raises(RuntimeError, match="already attempted"):
        server.create()
    assert api.create_namespaced_pod.call_count == 1
    no_deletion(api)


def test_receipt_publication_failure_prevents_submission(cluster):
    def fail(_):
        raise OSError("disk unavailable")

    server = ThriftServer(cluster[0], configuration(), fail)
    with pytest.raises(ThriftServerError) as caught:
        server.create()
    assert caught.value.handle["stage"] == "creation_uncertain"
    assert "driver_uid" not in caught.value.handle["run"]
    cluster[1].create_namespaced_pod.assert_not_called()
    no_deletion(cluster[1])


def test_confirmed_driver_identity_survives_receipt_write_failure(cluster):
    def fail_after_submission(value):
        if "driver_uid" in value["run"]:
            raise OSError("disk became unavailable")

    server = ThriftServer(cluster[0], configuration(), fail_after_submission)
    with pytest.raises(ThriftServerError) as caught:
        server.create()
    assert caught.value.handle["run"]["driver_uid"] == next(iter(cluster[2]["pod"].values())).metadata.uid
    no_deletion(cluster[1])


@pytest.mark.parametrize(
    "target,change",
    [
        ("driver", lambda obj: setattr(obj.metadata, "uid", "replacement")),
        ("driver_service", lambda obj: setattr(obj.metadata, "uid", "replacement")),
        ("service", lambda obj: setattr(obj.metadata, "uid", "replacement")),
        ("service", lambda obj: setattr(obj.metadata.owner_references[0], "uid", "another-driver")),
        ("service", lambda obj: obj.spec.selector.update({"extra": "selector"})),
        ("driver_service", lambda obj: obj.spec.selector.pop("spark-app-name")),
        ("service", lambda obj: setattr(obj.spec, "type", "NodePort")),
        ("service", lambda obj: setattr(obj.spec, "external_ips", ["1.2.3.4"])),
        ("service", lambda obj: setattr(obj.spec.ports[0], "node_port", 30000)),
        ("service", lambda obj: setattr(obj.spec.ports[0], "target_port", 9999)),
        ("driver_service", lambda obj: setattr(obj.spec, "cluster_ip", "10.0.0.1")),
        ("driver", lambda obj: setattr(obj.metadata, "resource_version", None)),
    ],
)
def test_all_resource_identity_and_routing_checks_precede_any_deletion(cluster, target, change):
    server, _ = backend(cluster)
    handle = server.create()
    _, api, objects, _ = cluster
    obj = (
        objects["pod"][handle["run"]["driver_pod"]]
        if target == "driver"
        else objects["service"][handle[target]["name"]]
    )
    change(obj)
    with pytest.raises(ThriftServerError):
        server.delete(handle)
    no_deletion(api)


def test_namespace_replacement_is_never_adopted(cluster):
    server, _ = backend(cluster)
    handle = server.create()
    cluster[3].metadata.uid = "replacement-namespace"
    with pytest.raises(ThriftServerError):
        server.delete(handle)
    no_deletion(cluster[1])


@pytest.mark.parametrize("status", [401, 403, 500])
def test_api_errors_are_not_absence(cluster, status):
    server, _ = backend(cluster)
    handle = server.create()
    cluster[1].read_namespaced_service.side_effect = ApiException(status=status)
    with pytest.raises(ThriftServerError, match="uncertain"):
        server.status(handle)
    with pytest.raises(ThriftServerError):
        server.delete(handle)
    no_deletion(cluster[1])


def test_deletion_uses_uid_and_resource_version_for_every_resource(cluster):
    server, records = backend(cluster)
    handle = server.create()
    _, api, objects, _ = cluster
    expected = {
        obj.metadata.name: (obj.metadata.uid, obj.metadata.resource_version)
        for items in objects.values()
        for obj in items.values()
    }
    server.delete(handle)
    assert records[-1]["stage"] == "deletion_requested"
    calls = [*api.delete_namespaced_service.call_args_list, *api.delete_namespaced_pod.call_args_list]
    assert len(calls) == 3
    for call in calls:
        body = call.kwargs["body"]
        assert (body.preconditions.uid, body.preconditions.resource_version) == expected[call.kwargs["name"]]
    assert not objects["pod"] and not objects["service"]
    assert server.wait_deleted(handle, timeout=1) == {"status": "deleted", "owned_resources_remaining": []}
    assert records[-1]["stage"] == "deleted"
    server.delete(records[-1])
    assert server.status(records[-1])["status"] == "absent"
    api.delete_namespace.assert_not_called()
    api.delete_namespaced_service_account.assert_not_called()


def test_race_after_preflight_preserves_changed_driver_and_records_partial_deletion(cluster):
    server, records = backend(cluster)
    handle = server.create()
    _, api, objects, _ = cluster
    original = api.delete_namespaced_pod.side_effect

    def race(**kwargs):
        objects["pod"][kwargs["name"]].metadata.resource_version = "new-version"
        return original(**kwargs)

    api.delete_namespaced_pod.side_effect = race
    with pytest.raises(ThriftServerError) as caught:
        server.delete(handle)
    assert records[-1]["stage"] == "deletion_uncertain"
    assert isinstance(caught.value.__cause__, ApiException) and caught.value.__cause__.status == 409
    assert handle["run"]["driver_pod"] in objects["pod"]
    assert not objects["service"]


def test_executor_owned_configmap_is_observed_after_driver_and_executor_are_gone(cluster):
    server, records = backend(cluster)
    handle = server.create()
    _, _, objects, _ = cluster
    executor = k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            name="executor",
            namespace="dbt-test",
            uid="executor-uid",
            resource_version="11",
            owner_references=[
                k8s.V1OwnerReference(
                    api_version="v1",
                    kind="Pod",
                    name=handle["run"]["driver_pod"],
                    uid=handle["run"]["driver_uid"],
                )
            ],
        )
    )
    configmap = k8s.V1ConfigMap(
        metadata=k8s.V1ObjectMeta(
            name="executor-map",
            namespace="dbt-test",
            uid="map-uid",
            resource_version="12",
            owner_references=[
                k8s.V1OwnerReference(api_version="v1", kind="Pod", name="executor", uid="executor-uid")
            ],
        )
    )
    objects["pod"]["executor"] = executor
    objects["config_map"]["executor-map"] = configmap
    server.delete(handle)
    durable = records[-1]
    assert {"executor-uid", "map-uid"} <= set(durable["owned_uids"])
    objects["pod"].pop("executor")
    resumed = ThriftServer(cluster[0], None, records.append)
    with pytest.raises(ThriftServerError, match="not observed"):
        resumed.wait_deleted(durable, timeout=0.02, poll_interval=0.001)
    assert "executor-map" in objects["config_map"]
    objects["config_map"].clear()
    assert resumed.wait_deleted(durable, timeout=1)["status"] == "deleted"


def test_missing_namespace_is_absence_but_is_never_deleted(cluster):
    server, records = backend(cluster)
    handle = server.create()
    cluster[1].read_namespace.side_effect = ApiException(status=404)
    assert server.status(handle)["status"] == "absent"
    server.delete(handle)
    assert server.wait_deleted(handle, timeout=1)["status"] == "deleted"
    no_deletion(cluster[1])
