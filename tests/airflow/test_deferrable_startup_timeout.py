from __future__ import annotations

import asyncio
import json
from unittest import mock

import pytest

from conftest import AIRFLOW_INSTALLED

pytestmark = pytest.mark.skipif(not AIRFLOW_INSTALLED, reason="Airflow extra is not installed")


@pytest.mark.parametrize("startup_timeout,deadline", [(0, None), (10, 1010)])
def test_operator_passes_absolute_startup_deadline(startup_timeout, deadline):
    from airflow.exceptions import TaskDeferred
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator

    operator = SparkOnK8SOperator(
        task_id="startup_deadline",
        image="spark:test",
        app_path="local:///app.py",
        deferrable=True,
        startup_timeout=startup_timeout,
    )
    with (
        mock.patch("time.time", return_value=1000),
        mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app", return_value="test-driver"),
        pytest.raises(TaskDeferred) as deferred,
    ):
        operator.execute({"ti": mock.Mock(xcom_pull=mock.Mock(return_value=None))})
    assert deferred.value.trigger.serialize()[1].get("startup_deadline") == deadline
    assert deferred.value.timeout is None  # Do not impose a timeout on running applications.


def test_expired_deadline_survives_trigger_serialization_and_restart():
    from spark_on_k8s.airflow.triggers import SparkOnK8STrigger
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    trigger = SparkOnK8STrigger(driver_pod_name="driver", namespace="spark", startup_deadline=105)
    _, kwargs = trigger.serialize()
    restored = SparkOnK8STrigger(**json.loads(json.dumps(kwargs)))
    assert restored.serialize() == trigger.serialize()
    client_manager = mock.Mock()
    client_manager.client.return_value = mock.AsyncMock()

    async def collect():
        return [event.payload async for event in restored.run()]

    with (
        mock.patch("time.time", return_value=106),
        mock.patch(
            "spark_on_k8s.airflow.triggers._AirflowKubernetesAsyncClientManager", return_value=client_manager
        ),
        mock.patch(
            "spark_on_k8s.utils.async_app_manager.AsyncSparkAppManager.app_status",
            new=mock.AsyncMock(return_value=SparkAppStatus.Pending),
        ),
        mock.patch(
            "spark_on_k8s.utils.async_app_manager.asyncio.sleep", new_callable=mock.AsyncMock
        ) as sleep,
    ):
        events = asyncio.run(collect())
    assert len(events) == 1
    assert events[0]["status"] == "startup_timeout"
    assert events[0]["namespace"] == "spark"
    assert events[0]["pod_name"] == "driver"
    sleep.assert_not_awaited()


def test_timeout_completion_cleans_up_before_attempting_logs():
    from airflow.exceptions import AirflowException
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator

    operator = SparkOnK8SOperator(
        task_id="startup_cleanup",
        image="spark:test",
        app_path="local:///app.py",
        app_waiter="log",
        on_finish_action="keep",
        deferrable=True,
        startup_timeout=10,
    )
    with mock.patch("spark_on_k8s.airflow.operators.SparkAppManager") as manager:
        with pytest.raises(AirflowException, match="Spark application startup timeout exceeded"):
            operator.execute_complete(
                {}, {"namespace": "spark", "pod_name": "driver", "status": "startup_timeout"}
            )
    manager.return_value.delete_app.assert_called_once_with(namespace="spark", pod_name="driver")
    manager.return_value.stream_logs.assert_not_called()


def test_cleanup_failure_does_not_hide_deferred_startup_timeout():
    from airflow.exceptions import AirflowException
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator

    operator = SparkOnK8SOperator(
        task_id="startup_cleanup_failure",
        image="spark:test",
        app_path="local:///app.py",
        deferrable=True,
        startup_timeout=10,
    )
    cleanup_error = RuntimeError("delete request failed")
    with mock.patch("spark_on_k8s.airflow.operators.SparkAppManager") as manager:
        manager.return_value.delete_app.side_effect = cleanup_error
        with pytest.raises(AirflowException, match="Spark application startup timeout exceeded") as failure:
            operator.execute_complete(
                {}, {"namespace": "spark", "pod_name": "driver", "status": "startup_timeout"}
            )
    assert failure.value.__cause__ is cleanup_error
    manager.return_value.delete_app.assert_called_once_with(namespace="spark", pod_name="driver")


def test_network_timeout_remains_an_error_event():
    from spark_on_k8s.airflow.triggers import SparkOnK8STrigger

    trigger = SparkOnK8STrigger(driver_pod_name="driver")

    async def collect():
        return [event.payload async for event in trigger.run()]

    with mock.patch(
        "spark_on_k8s.utils.async_app_manager.AsyncSparkAppManager.wait_for_app",
        new=mock.AsyncMock(side_effect=TimeoutError("network request timed out")),
    ):
        events = asyncio.run(collect())
    assert len(events) == 1
    assert events[0]["status"] == "error"
    assert events[0]["error"] == "network request timed out"
