from __future__ import annotations

from unittest import mock

import pytest
from spark_on_k8s.client import SparkOnK8S


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
    assert pod_name == "a" * 56 + "-driver"
    assert client.last_submitted_app_id == "a" * 63


def test_unsuccessful_submission_does_not_publish_an_app_id():
    client = SparkOnK8S(k8s_client_manager=mock.MagicMock())
    with mock.patch("spark_on_k8s.client.k8s.CoreV1Api") as api:
        api.return_value.create_namespaced_pod.side_effect = RuntimeError("Kubernetes unavailable")
        with pytest.raises(RuntimeError, match="Kubernetes unavailable"):
            client.submit_app(image="spark:test", app_path="local:///app.py")
    assert client.last_submitted_app_id is None
