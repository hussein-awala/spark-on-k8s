from __future__ import annotations

from unittest import mock

from click.testing import CliRunner

TEST_NAMESPACE = "test-namespace"


@mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager")
def test_list(mock_spark_app_manager):
    from spark_on_k8s.cli.apps import list

    mock_spark_app_manager().list_apps.return_value = ["app1", "app2", "app3"]
    args = ["--namespace", TEST_NAMESPACE]
    result = CliRunner().invoke(list, args)
    assert result.exit_code == 0
    assert result.output == "app1\napp2\napp3\n"
    mock_spark_app_manager().list_apps.assert_called_with(namespace=TEST_NAMESPACE)
