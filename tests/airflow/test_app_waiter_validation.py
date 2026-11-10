"""An app_waiter value that is not wait, log or no_wait is rejected before anything is submitted."""

from __future__ import annotations

from unittest import mock

import pytest

from conftest import AIRFLOW_INSTALLED

pytestmark = pytest.mark.skipif(not AIRFLOW_INSTALLED, reason="Airflow extra is not installed")


# A typo, a wrong case, and what a template renders for a missing value.
@pytest.mark.parametrize("waiter", ["logs", "Wait", "None", ""])
@pytest.mark.parametrize("deferrable", [False, True])
def test_unrecognised_app_waiter_is_rejected_before_submitting(waiter, deferrable):
    from airflow.exceptions import AirflowException
    from spark_on_k8s.airflow import operators

    operator = operators.SparkOnK8SOperator(
        task_id="unrecognised_waiter",
        image="spark:test",
        app_path="local:///job.py",
        namespace="spark",
        app_waiter="{{ params.waiter }}",
        deferrable=deferrable,
    )
    # What rendering leaves in the templated field.
    operator.app_waiter = waiter
    with (
        mock.patch.object(operators, "_AirflowKubernetesClientManager") as client_manager,
        mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app") as submit,
        pytest.raises(AirflowException, match="app_waiter must be one of"),
    ):
        operator.execute({"ti": mock.Mock(xcom_pull=mock.Mock(return_value=None))})

    client_manager.assert_not_called()
    submit.assert_not_called()


@pytest.mark.parametrize("waiter", ["no_wait", "wait", "log"])
def test_documented_app_waiters_are_accepted(waiter):
    from spark_on_k8s.airflow.operators import AppWaiter

    assert waiter in set(AppWaiter)
