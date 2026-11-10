"""Text-only commands end with one `Error:` line and exit code 1, never a traceback or response body."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest
from click.testing import CliRunner
from kubernetes import client as k8s
from kubernetes.client import ApiException
from spark_on_k8s import client
from spark_on_k8s.cli.app import app_cli
from spark_on_k8s.cli.namespace import namespace_cli
from spark_on_k8s.utils import app_manager

RESPONSE_BODY = '{"message":"pods is forbidden: detail-from-the-response-body"}'


def cluster(monkeypatch, scenario):
    api = Mock()
    if scenario == "missing":
        api.list_namespaced_pod.return_value = SimpleNamespace(items=[])
        api.read_namespaced_pod.side_effect = ApiException(status=404, reason="Not Found")
    else:
        error = ApiException(status=403, reason="Forbidden")
        error.body = RESPONSE_BODY
        for call in (
            "list_namespaced_pod",
            "read_namespaced_pod",
            "read_namespace",
            "list_namespace",
            "create_namespace",
            "read_namespaced_service_account",
            "list_namespaced_service_account",
            "create_namespaced_service_account",
        ):
            getattr(api, call).side_effect = error
    monkeypatch.setattr(k8s, "CoreV1Api", Mock(return_value=api))
    monkeypatch.setattr(k8s, "RbacAuthorizationV1Api", Mock(return_value=api))
    monkeypatch.setattr(client.KubernetesClientManager, "client", MagicMock())
    monkeypatch.setattr(app_manager.watch, "Watch", Mock())
    return api


def ends_through_click(result):
    assert isinstance(result.exception, SystemExit), repr(result.exception)
    assert result.exit_code != 0
    assert result.stderr.startswith("Error: "), result.stderr
    assert "Traceback" not in result.stderr
    assert "detail-from-the-response-body" not in result.stderr + result.stdout


@pytest.mark.parametrize("scenario", ["missing", "forbidden"])
@pytest.mark.parametrize("command", ["logs", "kill", "delete"])
def test_text_only_application_commands_report_failures_through_click(monkeypatch, command, scenario):
    cluster(monkeypatch, scenario)
    result = CliRunner().invoke(app_cli, [command, "--namespace", "spark", "--app-id", "x"])
    ends_through_click(result)


def test_namespace_setup_reports_a_denied_request_through_click(monkeypatch):
    cluster(monkeypatch, "forbidden")
    result = CliRunner().invoke(namespace_cli, ["setup", "--namespace", "spark"])
    ends_through_click(result)
