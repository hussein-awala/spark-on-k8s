from __future__ import annotations

from importlib.util import find_spec
from unittest import mock

import pytest

pytestmark = pytest.mark.skipif(find_spec("airflow") is None, reason="Airflow extra is not installed")


@pytest.mark.parametrize("templated", [False, True])
@mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app", return_value="test-driver")
def test_executor_allocation_tuning_survives_submission(mock_submit_app, templated):
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator
    from spark_on_k8s.client import ExecutorInstances

    instances = ExecutorInstances(
        min="{{ minimum }}" if templated else 1,
        max="{{ maximum }}" if templated else 10,
        initial="{{ initial }}" if templated else 2,
        executor_allocation_ratio=0.25,
        scheduler_backlog_timeout="5s",
        sustained_scheduler_backlog_timeout="20s",
    )
    operator = SparkOnK8SOperator(
        task_id="allocation_tuning",
        image="spark:3.5.0",
        app_path="local:///app.py",
        app_waiter="no_wait",
        executor_instances=instances,
    )
    operator.render_template_fields({"minimum": 1, "maximum": 10, "initial": 2})
    operator.execute({"ti": mock.Mock(xcom_pull=mock.Mock(return_value=None))})

    assert mock_submit_app.call_args.kwargs["executor_instances"] == ExecutorInstances(
        min=1,
        max=10,
        initial=2,
        executor_allocation_ratio=0.25,
        scheduler_backlog_timeout="5s",
        sustained_scheduler_backlog_timeout="20s",
    )
