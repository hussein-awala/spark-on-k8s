from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from kubernetes import client as k8s
from kubernetes.client.exceptions import ApiException
from spark_on_k8s.client import ExecutorInstances, PodResources, SparkOnK8S
from spark_on_k8s.connect_server import (
    ConnectServerConfig,
    ConnectServerCreateError,
    ConnectServerHandle,
    ConnectServerOwnershipError,
    create_connect_server,
    delete_connect_server,
)
from spark_on_k8s.utils.configuration import Configuration


@pytest.fixture
def cluster(monkeypatch):
    """Exercise the real submission and cancellation paths against Kubernetes responses."""
    api = MagicMock()
    pods = {}
    services = {}

    def create_pod(*, namespace, body, **kwargs):
        pod = deepcopy(body)
        pod.metadata.uid = "original-driver-uid"
        pod.status = k8s.V1PodStatus(phase="Running")
        pods[pod.metadata.name] = pod
        return deepcopy(pod)

    def create_service(*, namespace, body, **kwargs):
        if body.metadata.name in services:
            raise ApiException(status=409, reason="AlreadyExists")
        service = deepcopy(body)
        service.metadata.uid = f"service-uid-{len(services)}"
        if service.spec.cluster_ip is None:
            service.spec.cluster_ip = "10.96.1.20"
        services[service.metadata.name] = service
        return deepcopy(service)

    def read(objects, name):
        if name not in objects:
            raise ApiException(status=404, reason="NotFound")
        return deepcopy(objects[name])

    def delete(objects, *, name, body, **kwargs):
        existing = read(objects, name)
        if body.preconditions.uid != existing.metadata.uid:
            raise ApiException(status=409, reason="UID precondition failed")
        del objects[name]

    api.create_namespaced_pod.side_effect = create_pod
    api.create_namespaced_service.side_effect = create_service
    api.read_namespaced_pod.side_effect = lambda *, name, **kw: read(pods, name)
    api.read_namespaced_service.side_effect = lambda *, name, **kw: read(services, name)
    api.delete_namespaced_pod.side_effect = lambda **kw: delete(pods, **kw)
    api.delete_namespaced_service.side_effect = lambda **kw: delete(services, **kw)
    monkeypatch.setattr(k8s, "CoreV1Api", lambda raw: api)
    client = SparkOnK8S(k8s_client_manager=MagicMock())
    config = ConnectServerConfig(
        image="spark:test",
        server_jar="local:///opt/spark/jars/spark-connect.jar",
        namespace="connect-tests",
        service_account="spark",
        driver_resources=PodResources(cpu=1, memory=512, memory_overhead=128),
        executor_resources=PodResources(cpu=1, memory=512, memory_overhead=128),
        executor_instances=ExecutorInstances(initial=1),
    )
    return SimpleNamespace(
        client=client,
        config=config,
        api=api,
        pods=pods,
        services=services,
        create_pod=create_pod,
        create_service=create_service,
    )


def test_server_submission_does_not_inherit_unrelated_application_defaults(cluster, monkeypatch):
    ambient = {
        "SPARK_ON_K8S_DOCKER_IMAGE": "ambient-image",
        "SPARK_ON_K8S_NAMESPACE": "ambient-namespace",
        "SPARK_ON_K8S_SERVICE_ACCOUNT": "ambient-account",
        "SPARK_ON_K8S_APP_NAME": "ambient-app",
        "SPARK_ON_K8S_APP_PATH": "local:///ambient.py",
        "SPARK_ON_K8S_CLASS_NAME": "ambient.Main",
        "SPARK_ON_K8S_SPARK_CONF": {"spark.master": "local[*]", "spark.ambient.secret": "private"},
        "SPARK_ON_K8S_PACKAGES": "ambient:package:1",
        "SPARK_ON_K8S_APP_ARGUMENTS": ["ambient-argument"],
        "SPARK_ON_K8S_APP_WAITER": "wait",
        "SPARK_ON_K8S_IMAGE_PULL_POLICY": "Always",
        "SPARK_ON_K8S_UI_REVERSE_PROXY": True,
        "SPARK_ON_K8S_SECRET_ENV_VAR": {"AMBIENT_SECRET": "private"},
        "SPARK_ON_K8S_DRIVER_ENV_VARS_FROM_SECRET": ["ambient-secret"],
        "SPARK_ON_K8S_EXECUTOR_POD_TEMPLATE_PATH": "/ambient/template.yaml",
        "SPARK_ON_K8S_EXECUTOR_TEMPLATE": "invalid ambient template",
        "SPARK_ON_K8S_DRIVER_MEMORY": 99999,
        "SPARK_ON_K8S_EXECUTOR_MEMORY": 99999,
        "SPARK_ON_K8S_EXECUTOR_INITIAL_INSTANCES": 100,
    }
    for key, value in ambient.items():
        monkeypatch.setattr(Configuration, key, value)

    handle = create_connect_server(cluster.client, cluster.config)
    pod = cluster.pods[handle.run.driver_pod]
    container = pod.spec.containers[0]
    assert pod.metadata.namespace == "connect-tests"
    assert pod.spec.service_account_name == "spark"
    assert container.image == "spark:test"
    assert container.image_pull_policy == "IfNotPresent"
    assert container.resources.requests == {"cpu": "1", "memory": "640Mi"}
    assert container.env_from == []
    assert pod.spec.volumes == []
    assert "ambient" not in repr(container.args)
    assert "private" not in repr(container.args)
    assert "org.apache.spark.sql.connect.service.SparkConnectServer" in container.args
    assert container.args[-1] == cluster.config.server_jar
    cluster.api.create_namespaced_secret.assert_not_called()
    cluster.api.create_namespaced_config_map.assert_not_called()
    grpc = cluster.services[handle.service_name]
    assert grpc.spec.type == "ClusterIP"
    assert grpc.spec.external_ips is None
    assert grpc.spec.selector == {
        "spark-app-id": handle.run.app_id,
        "spark-role": "driver",
        "spark-on-k8s.io/connect-server": "true",
    }
    assert grpc.metadata.owner_references[0].uid == handle.run.driver_uid


def test_uncertain_driver_create_is_not_retried_adopted_or_deleted(cluster):
    def accepted_but_response_lost(**kwargs):
        cluster.create_pod(**kwargs)
        raise TimeoutError("response lost after admission")

    cluster.api.create_namespaced_pod.side_effect = accepted_but_response_lost
    with pytest.raises(ConnectServerCreateError) as raised:
        create_connect_server(cluster.client, cluster.config)
    error = raised.value
    assert len(cluster.pods) == 1
    assert error.run.driver_pod in cluster.pods
    assert error.run.driver_uid is None
    assert error.cleanup["status"] == "not_attempted"
    assert isinstance(error.__cause__, TimeoutError)
    cluster.api.create_namespaced_pod.assert_called_once()
    cluster.api.create_namespaced_service.assert_not_called()
    cluster.api.read_namespaced_pod.assert_not_called()
    cluster.api.delete_namespaced_pod.assert_not_called()


@pytest.mark.parametrize("replacement", [False, True], ids=["own-driver", "replacement-driver"])
def test_conflicting_grpc_service_is_never_adopted_or_deleted(cluster, replacement):
    conflict = ApiException(status=409, reason="AlreadyExists")
    foreign = None

    def conflicting_service(**kwargs):
        nonlocal foreign
        body = kwargs["body"]
        if not body.metadata.name.endswith("-connect"):
            return cluster.create_service(**kwargs)
        foreign = deepcopy(body)
        foreign.metadata.uid = "foreign-service-uid"
        foreign.metadata.owner_references = []
        cluster.services[foreign.metadata.name] = foreign
        if replacement:
            next(iter(cluster.pods.values())).metadata.uid = "replacement-driver-uid"
        raise conflict

    cluster.api.create_namespaced_service.side_effect = conflicting_service
    with pytest.raises(ConnectServerCreateError) as raised:
        create_connect_server(cluster.client, cluster.config)
    error = raised.value
    assert error.__cause__ is conflict
    assert error.run.driver_uid == "original-driver-uid"
    assert error.phase == "grpc_service_creation"
    assert cluster.services[foreign.metadata.name].metadata.uid == "foreign-service-uid"
    cluster.api.delete_namespaced_service.assert_not_called()
    if replacement:
        assert error.cleanup["status"] == "failed"
        assert cluster.pods[error.run.driver_pod].metadata.uid == "replacement-driver-uid"
        cluster.api.delete_namespaced_pod.assert_not_called()
    else:
        assert error.cleanup["status"] == "requested"
        deletion = cluster.api.delete_namespaced_pod.call_args.kwargs
        assert deletion["body"].preconditions.uid == error.run.driver_uid
        assert error.run.driver_pod not in cluster.pods


@pytest.mark.parametrize("resource", ["driver", "driver_service", "connect_service"])
def test_stale_receipt_preflights_every_identity_before_any_delete(cluster, resource):
    handle = create_connect_server(cluster.client, cluster.config)
    target = {
        "driver": cluster.pods[handle.run.driver_pod],
        "driver_service": cluster.services[handle.run.app_id],
        "connect_service": cluster.services[handle.service_name],
    }[resource]
    target.metadata.uid = "foreign-replacement-uid"
    with pytest.raises(ConnectServerOwnershipError):
        delete_connect_server(cluster.client, handle)
    cluster.api.delete_namespaced_service.assert_not_called()
    cluster.api.delete_namespaced_pod.assert_not_called()
    assert len(cluster.pods) == 1
    assert len(cluster.services) == 2


@pytest.mark.parametrize("resource", ["driver_service", "connect_service"])
def test_receipt_does_not_authorize_mutated_public_service(cluster, resource):
    handle = create_connect_server(cluster.client, cluster.config)
    name = handle.run.app_id if resource == "driver_service" else handle.service_name
    cluster.services[name].spec.type = "LoadBalancer"
    with pytest.raises(ConnectServerOwnershipError):
        delete_connect_server(cluster.client, handle)
    cluster.api.delete_namespaced_service.assert_not_called()
    cluster.api.delete_namespaced_pod.assert_not_called()


@pytest.mark.parametrize(
    "change",
    [
        lambda value: value["run"].pop("driver_uid"),
        lambda value: value["run"].update(driver_uid=""),
        lambda value: value.update(service_uid=None),
        lambda value: value.update(service_name="unrelated-connect"),
        lambda value: value.update(port=True),
        lambda value: value.update(token="private-token"),
        lambda value: value["run"].update(namespace="wrong.namespace"),
    ],
)
def test_corrupt_or_cross_resource_receipts_are_rejected_before_cluster_access(cluster, change):
    handle = create_connect_server(cluster.client, cluster.config)
    value = handle.to_dict()
    change(value)
    cluster.api.reset_mock()
    with pytest.raises(ValueError):
        ConnectServerHandle.from_dict(value)
    assert not cluster.api.mock_calls


@pytest.mark.parametrize("mutation", ["protected-conf", "negative-memory", "executor-bounds"])
def test_mutated_nested_config_is_revalidated_before_creating_resources(cluster, mutation):
    if mutation == "protected-conf":
        cluster.config.spark_conf["spark.master"] = "local[*]"
    elif mutation == "negative-memory":
        cluster.config.driver_resources.memory = -1
    else:
        cluster.config.executor_instances.min = 5
        cluster.config.executor_instances.max = 2
    with pytest.raises(ValueError):
        create_connect_server(cluster.client, cluster.config)
    assert not cluster.api.mock_calls


def test_delete_uid_precondition_protects_replacement_created_after_preflight(cluster):
    handle = create_connect_server(cluster.client, cluster.config)
    delete_service = cluster.api.delete_namespaced_service.side_effect

    def replace_driver_during_deletion(**kwargs):
        cluster.pods[handle.run.driver_pod].metadata.uid = "replacement-after-preflight"
        return delete_service(**kwargs)

    cluster.api.delete_namespaced_service.side_effect = replace_driver_during_deletion
    with pytest.raises(ApiException) as raised:
        delete_connect_server(cluster.client, handle)
    assert raised.value.status == 409
    assert cluster.pods[handle.run.driver_pod].metadata.uid == "replacement-after-preflight"
    assert (
        cluster.api.delete_namespaced_pod.call_args.kwargs["body"].preconditions.uid == handle.run.driver_uid
    )


def test_readiness_rechecks_uid_after_successful_query(cluster, monkeypatch):
    from spark_on_k8s.connect import ConnectProfile
    from spark_on_k8s.connect_server import wait_connect_server_ready

    handle = create_connect_server(cluster.client, cluster.config)

    def successful_query_then_replacement(profile, *, expected_app_id, timeout):
        assert expected_app_id == handle.run.app_id
        cluster.pods[handle.run.driver_pod].metadata.uid = "replacement-during-query"
        return {"schema_version": 1, "status": "ok", "spark_version": "4.0.5", "app_id": expected_app_id}

    monkeypatch.setattr(ConnectProfile, "probe", successful_query_then_replacement)
    with pytest.raises(ConnectServerOwnershipError):
        wait_connect_server_ready(
            cluster.client,
            handle,
            profile=ConnectProfile(endpoint="sc://127.0.0.1:15002", tls=False),
            timeout=2,
        )
    cluster.api.delete_namespaced_pod.assert_not_called()
    cluster.api.delete_namespaced_service.assert_not_called()


@pytest.mark.parametrize("returned_app_id", [None, "some-other-server"])
def test_parent_probe_rejects_success_for_wrong_or_missing_application_id(monkeypatch, returned_app_id):
    import json

    from spark_on_k8s import connect as module
    from spark_on_k8s.connect import ConnectError, ConnectProfile

    payload = {"schema_version": 1, "status": "ok", "spark_version": "4.0.5"}
    if returned_app_id is not None:
        payload["app_id"] = returned_app_id
    monkeypatch.setattr(
        module.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps(payload)),
    )
    with pytest.raises(ConnectError):
        ConnectProfile(endpoint="sc://127.0.0.1:15002", tls=False).probe(expected_app_id="owned-server")


def test_wrong_endpoint_is_rejected_before_query_and_only_its_session_is_released(monkeypatch, capsys):
    import io
    import json
    import sys

    from spark_on_k8s import connect as module

    session = MagicMock()
    session.conf.get.return_value = "another-server"
    builder = MagicMock()
    builder.remote.return_value.create.return_value = session
    monkeypatch.setitem(
        sys.modules,
        "pyspark.sql.connect.session",
        SimpleNamespace(SparkSession=SimpleNamespace(builder=builder)),
    )
    request = {"endpoint": "sc://127.0.0.1:15002", "tls": False, "expected_app_id": "owned-server"}
    monkeypatch.setattr(module.sys, "stdin", io.StringIO(json.dumps(request)))
    assert module._probe_worker() == 1
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "error"
    assert "another-server" not in json.dumps(result)
    session.conf.get.assert_called_once_with("spark.app.id")
    session.range.assert_not_called()
    session.stop.assert_called_once()
