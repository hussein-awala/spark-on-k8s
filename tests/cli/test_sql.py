from __future__ import annotations

import json
import sys
from unittest import mock

import pytest
from click.testing import CliRunner
from spark_on_k8s.cli.sql import sql_cli
from spark_on_k8s.utils.run import RunHandle
from spark_on_k8s.utils.spark_app_status import SparkAppStatus


def configure_client(instance, namespace="default", status=SparkAppStatus.Succeeded):
    instance.last_submitted_run = RunHandle(namespace=namespace, app_id="sql", driver_pod="sql-driver")
    instance.app_manager = mock.Mock()
    instance.app_manager.app_status.return_value = status


@pytest.mark.parametrize("flags", [[], ["--sql", "SELECT 1", "--file", "queries.sql"]])
def test_requires_exactly_one_source(flags, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "queries.sql").write_text("SELECT 1")
    with mock.patch("spark_on_k8s.client.SparkOnK8S", autospec=True) as client:
        result = CliRunner().invoke(sql_cli, ["submit", "--image", "spark:test", *flags])
    assert result.exit_code == 2
    assert "exactly one" in result.output
    client.assert_not_called()


@pytest.mark.parametrize("source", ["inline", "file"])
def test_sql_cli_forwards_source_and_resources(source, tmp_path):
    query = tmp_path / "queries.sql"
    query.write_text("SELECT 'a;b'")
    flags = ["--sql", "SELECT 'a;b'"] if source == "inline" else ["--file", str(query)]
    with mock.patch("spark_on_k8s.client.SparkOnK8S", autospec=True) as client:
        configure_client(client.return_value, namespace="analytics")
        result = CliRunner().invoke(
            sql_cli,
            [
                "submit",
                "--image",
                "spark:test",
                "--namespace",
                "analytics",
                "--driver-memory",
                "2048",
                "--executor-initial-instances",
                "1",
                "--conf",
                "spark.sql.shuffle.partitions=2",
                *flags,
            ],
        )
    assert result.exit_code == 0, result.output
    options = client.return_value.submit_app.call_args.kwargs
    sources = options["driver_ephemeral_configmaps_volumes"][0]["sources"]
    assert json.loads(sources[1]["text"]) == ["SELECT 'a;b'"]
    assert options["app_path"] == "/configmap/spark_sql.py"
    assert options["namespace"] == "analytics"
    assert options["driver_resources"].memory == 2048
    assert options["executor_instances"].initial == 1
    assert options["spark_conf"] == {"spark.sql.shuffle.partitions": "2"}
    assert options["app_waiter"] == "no_wait"
    client.return_value.app_manager.wait_for_app.assert_not_called()


@pytest.mark.parametrize(
    "flags, logs", [(["--wait"], False), (["--logs"], True), (["--logs", "--wait"], True)]
)
@pytest.mark.parametrize("status, exit_code", [(SparkAppStatus.Succeeded, 0), (SparkAppStatus.Failed, 3)])
def test_waiting_sql_cli_uses_shared_status_and_json_contract(flags, logs, status, exit_code):
    with mock.patch("spark_on_k8s.client.SparkOnK8S", autospec=True) as client:
        instance = client.return_value
        configure_client(instance, status=status)
        instance.app_manager.stream_logs.side_effect = lambda **kwargs: print("query output")
        result = CliRunner().invoke(
            sql_cli, ["submit", "--image", "spark:test", "--sql", "SELECT 1", "--output", "json", *flags]
        )
    assert result.exit_code == exit_code, result.output
    payload = json.loads(result.stdout)
    assert payload["schema_version"] == 1
    assert payload["status"] == status.value
    assert payload["run"]["driver_pod"] == "sql-driver"
    assert payload["error"] is None
    assert instance.submit_app.call_args.kwargs["app_waiter"] == "no_wait"
    assert instance.app_manager.stream_logs.called is logs
    instance.app_manager.wait_for_app.assert_called_once_with(
        namespace="default", pod_name="sql-driver", should_print=True
    )
    if logs:
        assert "query output" in result.stderr and "query output" not in result.stdout


def test_missing_optional_dependency_has_readable_json_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "sqlparse", None)
    with mock.patch("spark_on_k8s.client.SparkOnK8S", autospec=True) as client:
        result = CliRunner().invoke(
            sql_cli, ["submit", "--image", "spark:test", "--sql", "SELECT 1", "--output", "json"]
        )
    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert "spark-on-k8s[sql]" in payload["error"]["message"]
    # Preparation failures use the same submit document shape as other submissions.
    assert payload["run"] is None
    assert payload["status"] is None
    client.assert_not_called()


def test_sql_timeout_retains_run_identity_without_deleting():
    with mock.patch("spark_on_k8s.client.SparkOnK8S", autospec=True) as client:
        configure_client(client.return_value)
        client.return_value.app_manager.wait_for_app.side_effect = TimeoutError("budget elapsed")
        result = CliRunner().invoke(
            sql_cli,
            [
                "submit",
                "--image",
                "spark:test",
                "--sql",
                "SELECT 1",
                "--wait",
                "--timeout",
                "1",
                "--output",
                "json",
            ],
        )
    assert result.exit_code == 5, result.output
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "timeout"
    assert payload["run"]["driver_pod"] == "sql-driver"
    client.return_value.app_manager.delete_app.assert_not_called()


def test_logs_wait_for_terminal_phase_before_reporting_success():
    state = [SparkAppStatus.Running]
    with mock.patch("spark_on_k8s.client.SparkOnK8S", autospec=True) as client:
        instance = client.return_value
        configure_client(instance)
        instance.app_manager.app_status.side_effect = lambda **kwargs: state[0]
        instance.app_manager.wait_for_app.side_effect = lambda **kwargs: state.__setitem__(
            0, SparkAppStatus.Succeeded
        )
        result = CliRunner().invoke(
            sql_cli, ["submit", "--image", "spark:test", "--sql", "SELECT 1", "--logs"]
        )
    assert result.exit_code == 0, result.output
    instance.app_manager.wait_for_app.assert_called_once_with(
        namespace="default", pod_name="sql-driver", should_print=True
    )
