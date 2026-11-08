from __future__ import annotations

import re
from unittest import mock

import pytest
from kubernetes import client as k8s
from kubernetes.client.exceptions import ApiException
from spark_on_k8s.client import SparkOnK8S
from urllib3.exceptions import MaxRetryError, ReadTimeoutError


def test_successful_submission_preserves_full_app_id_without_changing_return_value():
    client = SparkOnK8S(k8s_client_manager=mock.MagicMock())
    assert client.last_submitted_app_id is None
    with mock.patch("spark_on_k8s.client.k8s.CoreV1Api") as api:

        def created_pod(namespace, body):
            body.metadata.uid = "created-driver-uid"
            return body

        api.return_value.create_namespaced_pod.side_effect = created_pod
        pod_name = client.submit_app(
            image="spark:test", app_path="local:///app.py", app_name="a" * 63, app_id_suffix=lambda: ""
        )
    assert pod_name == "a" * 23 + "-7d3e74a05d7db15bce4ad9ec0658ea98-driver"
    assert client.last_submitted_app_id == "a" * 63


def test_unsuccessful_submission_does_not_publish_an_app_id():
    client = SparkOnK8S(k8s_client_manager=mock.MagicMock())
    with mock.patch("spark_on_k8s.client.k8s.CoreV1Api") as api:
        api.return_value.create_namespaced_pod.side_effect = RuntimeError("Kubernetes unavailable")
        with pytest.raises(RuntimeError, match="Kubernetes unavailable"):
            client.submit_app(image="spark:test", app_path="local:///app.py")
    assert client.last_submitted_app_id is None


def test_long_app_ids_keep_distinct_driver_names_and_matching_driver_references():
    # A 48-character name with the default timestamp suffix yields a 63-character app ID;
    # runs within ten days differ only after the 56th character.
    names = []
    for suffix in ("-20240114121231", "-20240115121231"):
        client = SparkOnK8S(k8s_client_manager=mock.MagicMock())
        with mock.patch("spark_on_k8s.client.k8s.CoreV1Api") as api:

            def created_pod(namespace, body):
                body.metadata.uid = "created-driver-uid"
                return body

            api.return_value.create_namespaced_pod.side_effect = created_pod
            pod_name = client.submit_app(
                image="spark:test",
                app_path="local:///app.py",
                app_name="a" * 48,
                app_id_suffix=lambda suffix=suffix: suffix,
            )
        app_id = "a" * 48 + suffix
        assert client.last_submitted_run.app_id == app_id
        assert client.last_submitted_run.driver_pod == pod_name
        assert len(pod_name) <= 63
        assert re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", pod_name)
        pod = api.return_value.create_namespaced_pod.call_args.kwargs["body"]
        assert f"spark.kubernetes.driver.pod.name={pod_name}" in pod.spec.containers[0].args
        service = api.return_value.create_namespaced_service.call_args.kwargs["body"]
        assert service.metadata.name == app_id
        assert [(owner.name, owner.uid) for owner in service.metadata.owner_references] == [
            (pod_name, "created-driver-uid")
        ]
        names.append(pod_name)

    assert names[0] != names[1]


def _submit_with_created_resources(api, client=None):
    """Submit with a Secret and two ConfigMaps whose create responses carry server UIDs."""
    api.create_namespaced_secret.side_effect = lambda namespace, body: k8s.V1Secret(
        metadata=k8s.V1ObjectMeta(name=body.metadata.name, uid="secret-uid")
    )
    if api.create_namespaced_config_map.side_effect is None:
        api.create_namespaced_config_map.side_effect = lambda namespace, body: k8s.V1ConfigMap(
            metadata=k8s.V1ObjectMeta(name=body.metadata.name, uid=f"{body.metadata.name}-uid")
        )
    client = client or SparkOnK8S(k8s_client_manager=mock.MagicMock())
    with mock.patch("spark_on_k8s.client.k8s.CoreV1Api", return_value=api):
        client.submit_app(
            image="spark:test",
            app_path="local:///app.py",
            namespace="spark",
            app_name="job",
            app_id_suffix=lambda: "",
            secret_values={"TOKEN": "private"},
            driver_ephemeral_configmaps_volumes=[
                {"mount_path": "/etc/first", "sources": [{"name": "a.conf", "text": "a"}]},
                {"mount_path": "/etc/second", "sources": [{"name": "b.conf", "text": "b"}]},
            ],
        )


def _uid_delete(uid):
    return k8s.V1DeleteOptions(preconditions=k8s.V1Preconditions(uid=uid))


@pytest.mark.parametrize("status", [400, 403, 409, 422, 429])
def test_rejected_driver_deletes_only_resources_created_by_the_same_submission(status):
    # Rejected Pods (for example by quota or admission) would otherwise leave unowned Secrets/ConfigMaps.
    api = mock.Mock()
    error = ApiException(status=status, reason="Rejected")
    api.create_namespaced_pod.side_effect = error
    client = SparkOnK8S(k8s_client_manager=mock.MagicMock())

    with pytest.raises(ApiException) as raised:
        _submit_with_created_resources(api, client)

    assert raised.value is error
    assert client.last_submitted_run is None
    api.delete_namespaced_secret.assert_called_once_with(
        name="job", namespace="spark", body=_uid_delete("secret-uid")
    )
    assert api.delete_namespaced_config_map.call_args_list == [
        mock.call(name="job-0", namespace="spark", body=_uid_delete("job-0-uid")),
        mock.call(name="job-1", namespace="spark", body=_uid_delete("job-1-uid")),
    ]
    api.delete_namespaced_pod.assert_not_called()
    api.create_namespaced_service.assert_not_called()


@pytest.mark.parametrize(
    "failure",
    [
        ApiException(status=500, reason="Internal error"),
        ApiException(status=504, reason="Timeout"),
        MaxRetryError(None, "/api/v1/namespaces/spark/pods", ReadTimeoutError(None, "/", "read timed out")),
        KeyboardInterrupt(),
    ],
    ids=["500", "504", "transport-timeout", "interrupted"],
)
def test_ambiguous_driver_creation_keeps_resources_the_driver_may_use(failure):
    api = mock.Mock()
    api.create_namespaced_pod.side_effect = failure

    with pytest.raises(type(failure)):
        _submit_with_created_resources(api)

    api.delete_namespaced_secret.assert_not_called()
    api.delete_namespaced_config_map.assert_not_called()
    api.delete_namespaced_pod.assert_not_called()


def test_rejected_configmap_keeps_the_existing_object_and_removes_earlier_creations():
    api = mock.Mock()
    conflict = ApiException(status=409, reason="AlreadyExists")
    api.create_namespaced_config_map.side_effect = [
        k8s.V1ConfigMap(metadata=k8s.V1ObjectMeta(name="job-0", uid="job-0-uid")),
        conflict,
    ]

    with pytest.raises(ApiException) as raised:
        _submit_with_created_resources(api)

    assert raised.value is conflict
    api.delete_namespaced_secret.assert_called_once_with(
        name="job", namespace="spark", body=_uid_delete("secret-uid")
    )
    api.delete_namespaced_config_map.assert_called_once_with(
        name="job-0", namespace="spark", body=_uid_delete("job-0-uid")
    )
    api.create_namespaced_pod.assert_not_called()


def test_failed_rollback_preserves_the_rejection_and_continues_with_other_resources():
    api = mock.Mock()
    rejection = ApiException(status=403, reason="Forbidden")
    api.create_namespaced_pod.side_effect = rejection
    api.delete_namespaced_secret.side_effect = ApiException(status=403, reason="Forbidden")
    api.delete_namespaced_config_map.side_effect = [ApiException(status=404, reason="NotFound"), None]

    with pytest.raises(ApiException) as raised:
        _submit_with_created_resources(api)

    assert raised.value is rejection
    assert api.delete_namespaced_config_map.call_count == 2
    assert any("Secret job" in note for note in getattr(raised.value, "__notes__", []))
    assert "private" not in " ".join(getattr(raised.value, "__notes__", []))


def test_resources_without_a_confirmed_uid_are_never_deleted():
    api = mock.Mock()
    api.create_namespaced_pod.side_effect = ApiException(status=403, reason="Forbidden")
    api.create_namespaced_config_map.side_effect = lambda namespace, body: k8s.V1ConfigMap(
        metadata=k8s.V1ObjectMeta(name=body.metadata.name)
    )

    with pytest.raises(ApiException):
        _submit_with_created_resources(api)

    api.delete_namespaced_secret.assert_called_once()
    api.delete_namespaced_config_map.assert_not_called()


@pytest.mark.parametrize("suffix", ["-v1.2", "-Run1", "-a_b", "-" + "x" * 63])
@pytest.mark.parametrize("dry_run", [False, True])
def test_application_ids_that_cannot_name_the_service_are_rejected_before_creation(suffix, dry_run):
    # The driver Service is named after the app ID and is created after the driver Pod, whose
    # name accepts dots; validating late left a driver without its Service.
    api = mock.Mock()
    client = SparkOnK8S(k8s_client_manager=mock.MagicMock())

    with (
        mock.patch("spark_on_k8s.client.k8s.CoreV1Api", return_value=api),
        pytest.raises(ValueError, match="DNS-1035 label"),
    ):
        client.submit_app(
            image="spark:test",
            app_path="local:///app.py",
            namespace="spark",
            app_name="job",
            app_id_suffix=lambda: suffix,
            secret_values={"TOKEN": "private"},
            dry_run=dry_run,
        )

    assert api.method_calls == []
    assert client.last_submitted_run is None
