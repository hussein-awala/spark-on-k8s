from __future__ import annotations

from types import SimpleNamespace
from unittest import mock

import pytest
from kubernetes import client as k8s
from kubernetes.client.exceptions import ApiException
from spark_on_k8s.k8s.sync_client import KubernetesClientManager
from spark_on_k8s.utils.setup_namespace import SparkOnK8SNamespaceSetup


def _resources(*names):
    return SimpleNamespace(items=[SimpleNamespace(metadata=k8s.V1ObjectMeta(name=name)) for name in names])


@pytest.fixture
def namespace_setup(monkeypatch):
    client = mock.Mock(spec=k8s.ApiClient)
    manager = KubernetesClientManager()
    monkeypatch.setattr(manager, "create_client", lambda: client)
    core = mock.Mock(spec=k8s.CoreV1Api)
    rbac = mock.Mock(spec=k8s.RbacAuthorizationV1Api)
    core.list_namespace.return_value = _resources()
    core.list_namespaced_service_account.return_value = _resources()
    rbac.list_cluster_role_binding.return_value = _resources()
    monkeypatch.setattr(k8s, "CoreV1Api", lambda api_client: core)
    monkeypatch.setattr(k8s, "RbacAuthorizationV1Api", lambda api_client: rbac)
    return SparkOnK8SNamespaceSetup(k8s_client_manager=manager), core, rbac, client


@pytest.mark.parametrize(
    ("namespace_exists", "account_exists", "binding_exists"),
    [
        (False, False, False),
        (True, False, False),
        (True, True, False),
        (True, True, True),
        (True, False, True),
    ],
    ids=["new-namespace", "namespace-only", "missing-binding", "already-ready", "deleted-account"],
)
def test_setup_creates_only_missing_resources_for_the_requested_namespace(
    namespace_setup, namespace_exists, account_exists, binding_exists
):
    setup, core, rbac, client = namespace_setup
    namespace = "analytics"
    core.list_namespace.return_value = _resources("unrelated", *([namespace] if namespace_exists else []))
    core.list_namespaced_service_account.return_value = _resources(
        "default", *(["spark"] if account_exists else [])
    )
    rbac.list_cluster_role_binding.return_value = _resources(
        "spark-role-binding-unrelated", *([f"spark-role-binding-{namespace}"] if binding_exists else [])
    )

    setup.setup_namespace(namespace)

    core.list_namespaced_service_account.assert_called_once_with(namespace=namespace)
    if namespace_exists:
        core.create_namespace.assert_not_called()
    else:
        core.create_namespace.assert_called_once()
        assert core.create_namespace.call_args.kwargs["body"].metadata.name == namespace
    if account_exists:
        core.create_namespaced_service_account.assert_not_called()
    else:
        core.create_namespaced_service_account.assert_called_once()
        request = core.create_namespaced_service_account.call_args.kwargs
        assert request["namespace"] == namespace
        assert request["body"].metadata.name == "spark"
    if binding_exists:
        rbac.create_cluster_role_binding.assert_not_called()
    else:
        rbac.create_cluster_role_binding.assert_called_once()
        binding = rbac.create_cluster_role_binding.call_args.kwargs["body"]
        assert binding.metadata.name == f"spark-role-binding-{namespace}"
        assert binding.role_ref.to_dict() == {
            "api_group": "rbac.authorization.k8s.io",
            "kind": "ClusterRole",
            "name": "edit",
        }
        assert len(binding.subjects) == 1
        subject = binding.subjects[0]
        assert (subject.kind, subject.name, subject.namespace) == ("ServiceAccount", "spark", namespace)
    client.close.assert_called_once()


@pytest.mark.parametrize("failure_phase", ["namespace-read", "account-create", "binding-create"])
def test_setup_propagates_permission_errors_and_closes_the_client(namespace_setup, failure_phase):
    setup, core, rbac, client = namespace_setup
    error = ApiException(status=403, reason="Forbidden")
    failing_call = {
        "namespace-read": core.list_namespace,
        "account-create": core.create_namespaced_service_account,
        "binding-create": rbac.create_cluster_role_binding,
    }[failure_phase]
    failing_call.side_effect = error

    with pytest.raises(ApiException) as raised:
        setup.setup_namespace("analytics")

    assert raised.value is error
    client.close.assert_called_once()
    if failure_phase == "namespace-read":
        core.create_namespace.assert_not_called()
        core.create_namespaced_service_account.assert_not_called()
    if failure_phase != "binding-create":
        rbac.create_cluster_role_binding.assert_not_called()
