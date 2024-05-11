from __future__ import annotations

from unittest import mock

import pytest
from click.testing import CliRunner

TEST_NAMESPACE = "test-namespace"
TEST_APP_ID = "test-id"


@mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager")
def test_status(mock_spark_app_manager):
    from spark_on_k8s.cli.app import status
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    mock_spark_app_manager().app_status.return_value = SparkAppStatus.Succeeded
    command = ["--app-id", TEST_APP_ID, "--namespace", TEST_NAMESPACE]
    result = CliRunner().invoke(status, command)
    assert result.exit_code == 0
    assert result.output == "Succeeded\n"
    mock_spark_app_manager().app_status.assert_called_with(namespace=TEST_NAMESPACE, app_id=TEST_APP_ID)


@mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager")
def test_logs(mock_spark_app_manager):
    from spark_on_k8s.cli.app import logs

    command = ["--app-id", TEST_APP_ID, "--namespace", TEST_NAMESPACE]
    result = CliRunner().invoke(logs, command)
    assert result.exit_code == 0
    mock_spark_app_manager().stream_logs.assert_called_with(
        namespace=TEST_NAMESPACE, app_id=TEST_APP_ID, should_print=True
    )


@mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager")
def test_kill(mock_spark_app_manager):
    from spark_on_k8s.cli.app import kill

    command = ["--app-id", TEST_APP_ID, "--namespace", TEST_NAMESPACE]
    result = CliRunner().invoke(kill, command)
    assert result.exit_code == 0
    mock_spark_app_manager().kill_app.assert_called_with(namespace=TEST_NAMESPACE, app_id=TEST_APP_ID)


@pytest.mark.parametrize("force", [True, False])
@mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager")
def test_delete(mock_spark_app_manager, force):
    from spark_on_k8s.cli.app import delete

    command = ["--app-id", TEST_APP_ID, "--namespace", TEST_NAMESPACE]
    if force:
        command.append("--force")
    result = CliRunner().invoke(delete, command)
    assert result.exit_code == 0
    mock_spark_app_manager().delete_app.assert_called_with(
        namespace=TEST_NAMESPACE, app_id=TEST_APP_ID, force=force
    )


@mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager")
def test_wait(mock_spark_app_manager):
    from spark_on_k8s.cli.app import wait
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    mock_spark_app_manager().app_status.return_value = SparkAppStatus.Succeeded
    command = ["--app-id", TEST_APP_ID, "--namespace", TEST_NAMESPACE]
    result = CliRunner().invoke(wait, command)
    assert result.exit_code == 0
    mock_spark_app_manager().wait_for_app.assert_called_with(
        namespace=TEST_NAMESPACE,
        app_id=TEST_APP_ID,
        should_print=True,
    )
    mock_spark_app_manager().app_status.assert_called_with(
        namespace=TEST_NAMESPACE,
        app_id=TEST_APP_ID,
    )
    assert result.output == f"App {TEST_APP_ID} has terminated with status Succeeded\n"


# TODO: add some tests for submit command
