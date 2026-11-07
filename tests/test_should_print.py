from __future__ import annotations

import logging
from unittest import mock

import pytest
from kubernetes import client as k8s
from spark_on_k8s import client as client_module
from spark_on_k8s.utils.types import NOTSET


@pytest.mark.parametrize("app_waiter", ["no_wait", "wait", "log"])
@pytest.mark.parametrize(
    "output_arguments, expected_print",
    [
        pytest.param({}, False, id="omitted"),
        pytest.param({"should_print": NOTSET}, False, id="notset"),
        pytest.param({"should_print": False}, False, id="false"),
        pytest.param({"should_print": True}, True, id="true"),
    ],
)
def test_submit_app_output_destination(
    app_waiter, output_arguments, expected_print, capsys, caplog, monkeypatch
):
    monkeypatch.setattr(client_module.Configuration, "SPARK_ON_K8S_SECRET_ENV_VAR", {})
    monkeypatch.setattr(client_module.Configuration, "SPARK_ON_K8S_EXECUTOR_TEMPLATE", None)
    monkeypatch.setattr(client_module.Configuration, "SPARK_ON_K8S_EXECUTOR_POD_TEMPLATE_PATH", None)
    caplog.set_level(logging.INFO)
    # Host frameworks may log to stdout; isolate logging from direct print assertions.
    monkeypatch.setattr(logging.getLogger(), "handlers", [caplog.handler])
    original_name = "spark-app-" + "x" * 63
    normalized_name = original_name[:63]
    expected_messages = [
        f"Application name {original_name!r} has been sanitized"
        f" to {normalized_name!r} to comply with Kubernetes naming conventions"
    ]

    with (
        mock.patch("spark_on_k8s.client.k8s.CoreV1Api") as api_class,
        mock.patch("spark_on_k8s.utils.app_manager.watch.Watch") as watcher_class,
    ):
        api = api_class.return_value
        api.create_namespaced_pod.side_effect = lambda namespace, body: body
        api.read_namespaced_pod.return_value = k8s.V1Pod(status=k8s.V1PodStatus(phase="Succeeded"))
        watcher_class.return_value.stream.return_value = iter(["application log line"])
        spark_client = client_module.SparkOnK8S(k8s_client_manager=mock.MagicMock())
        pod_name = spark_client.submit_app(
            image="spark:test",
            app_path="local:///app.py",
            app_name=original_name,
            app_id_suffix=lambda: "",
            namespace="spark",
            app_waiter=app_waiter,
            **output_arguments,
        )

    if app_waiter == "wait":
        expected_messages.append(f"Pod {pod_name} finished with status Succeeded")
    elif app_waiter == "log":
        expected_messages.append("application log line")
    captured = capsys.readouterr()
    if expected_print:
        assert captured.out == "\n".join(expected_messages) + "\n"
        assert caplog.records == []
    else:
        assert captured.out == ""
        assert [record.getMessage() for record in caplog.records] == expected_messages
