from __future__ import annotations

from unittest import mock

import pytest
from click.testing import CliRunner
from spark_on_k8s.cli.app import submit
from spark_on_k8s.cli.options import executor_allocation_ratio_option


@pytest.mark.parametrize("ratio", [0.25, 0.5, 1.0])
@pytest.mark.parametrize("use_default", [False, True])
def test_submit_preserves_fractional_allocation_ratio(ratio, use_default):
    command = ["--image", "spark:test", "--path", "local:///app.py", "--executor-min-instances", "1"]
    if not use_default:
        command.extend(["--executor-allocation-ratio", str(ratio)])

    with (
        mock.patch("spark_on_k8s.client.SparkOnK8S") as client_class,
        mock.patch.object(executor_allocation_ratio_option, "default", ratio),
    ):
        result = CliRunner().invoke(submit, command)

    assert result.exit_code == 0, result.output
    instances = client_class.return_value.submit_app.call_args.kwargs["executor_instances"]
    assert instances.executor_allocation_ratio == ratio
