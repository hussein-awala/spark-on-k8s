from __future__ import annotations

from unittest import mock

from click.testing import CliRunner

TEST_NAMESPACE = "test-namespace"


@mock.patch("spark_on_k8s.utils.setup_namespace.SparkOnK8SNamespaceSetup")
def test_setup(mock_spark_on_k8s_setuper):
    from spark_on_k8s.cli.namespace import setup

    args = ["--namespace", TEST_NAMESPACE]
    result = CliRunner().invoke(setup, args)
    assert result.exit_code == 0
    mock_spark_on_k8s_setuper().setup_namespace.assert_called_with(
        namespace=TEST_NAMESPACE, should_print=True
    )
