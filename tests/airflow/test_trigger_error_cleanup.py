from __future__ import annotations

import asyncio
import json
from unittest import mock

import pytest

from conftest import AIRFLOW_INSTALLED

pytestmark = pytest.mark.skipif(not AIRFLOW_INSTALLED, reason="Airflow extra is not installed")


@pytest.fixture
def trigger_error_event():
    from spark_on_k8s.airflow.triggers import SparkOnK8STrigger

    trigger = SparkOnK8STrigger(driver_pod_name="application-driver", namespace="spark")

    async def collect():
        return [event.payload async for event in trigger.run()]

    with mock.patch(
        "spark_on_k8s.utils.async_app_manager.AsyncSparkAppManager.wait_for_app",
        new=mock.AsyncMock(side_effect=RuntimeError("Kubernetes connection lost")),
    ):
        events = asyncio.run(collect())
    assert len(events) == 1
    event = json.loads(json.dumps(events[0]))
    assert event["status"] == "error"
    assert event["error"] == "Kubernetes connection lost"
    assert "RuntimeError: Kubernetes connection lost" in event["stacktrace"]
    return event


@pytest.mark.parametrize("waiter", ["wait", "log"])
@pytest.mark.parametrize(
    "finish_action,should_delete", [("keep", False), ("delete", True), ("keep_if_failed", True)]
)
def test_trigger_error_skips_logs_and_preserves_cleanup_and_diagnostics(
    trigger_error_event, waiter, finish_action, should_delete
):
    from airflow.exceptions import AirflowException
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator
    from spark_on_k8s.utils.app_manager import SparkAppManager

    operator = SparkOnK8SOperator(
        task_id="trigger_failure",
        image="spark:test",
        app_path="local:///job.py",
        app_waiter=waiter,
        on_finish_action=finish_action,
        deferrable=True,
        spark_on_k8s_service_url="https://spark.example",
    )
    manager = mock.create_autospec(SparkAppManager, instance=True)
    # Replaying logs can fail for the same reason as the trigger, or wait indefinitely.
    manager.stream_logs.side_effect = RuntimeError("log endpoint unavailable")
    context = {"ti": mock.Mock()}
    with (
        mock.patch("spark_on_k8s.airflow.operators.SparkAppManager", return_value=manager),
        pytest.raises(AirflowException, match="SparkOnK8STrigger failed") as failure,
    ):
        operator.execute_complete(context, trigger_error_event)

    assert trigger_error_event["error"] in str(failure.value)
    assert trigger_error_event["stacktrace"] in str(failure.value)
    manager.stream_logs.assert_not_called()
    manager.wait_for_app.assert_not_called()
    if should_delete:
        manager.delete_app.assert_called_once_with(namespace="spark", pod_name="application-driver")
    else:
        manager.delete_app.assert_not_called()
    context["ti"].xcom_push.assert_called_once_with(
        key="spark_ui_link",
        value="https://spark.example/webserver/ui-history/history/application",
    )


@pytest.mark.parametrize("finish_action", ["delete", "keep_if_failed"])
def test_cleanup_failure_does_not_replace_trigger_error_diagnostics(trigger_error_event, finish_action):
    from airflow.exceptions import AirflowException
    from kubernetes.client import ApiException
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator
    from spark_on_k8s.utils.app_manager import SparkAppManager

    operator = SparkOnK8SOperator(
        task_id="trigger_failure_cleanup_failure",
        image="spark:test",
        app_path="local:///job.py",
        on_finish_action=finish_action,
        deferrable=True,
    )
    manager = mock.create_autospec(SparkAppManager, instance=True)
    # The trigger's stacktrace exists only in the event; a failed deletion must not discard it.
    cleanup_error = ApiException(status=503, reason="Kubernetes unavailable")
    manager.delete_app.side_effect = cleanup_error
    with (
        mock.patch("spark_on_k8s.airflow.operators.SparkAppManager", return_value=manager),
        pytest.raises(AirflowException, match="SparkOnK8STrigger failed") as failure,
    ):
        operator.execute_complete({"ti": mock.Mock()}, trigger_error_event)

    assert trigger_error_event["error"] in str(failure.value)
    assert trigger_error_event["stacktrace"] in str(failure.value)
    assert failure.value.__cause__ is cleanup_error
    manager.delete_app.assert_called_once_with(namespace="spark", pod_name="application-driver")


@pytest.mark.parametrize("status", ["Failed", "Unknown"])
def test_cleanup_failure_does_not_replace_terminal_status(status):
    from airflow.exceptions import AirflowException
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator

    operator = SparkOnK8SOperator(
        task_id="terminal_cleanup_failure",
        image="spark:test",
        app_path="local:///job.py",
        on_finish_action="delete",
        deferrable=True,
    )
    cleanup_error = RuntimeError("delete request failed")
    with mock.patch("spark_on_k8s.airflow.operators.SparkAppManager") as factory:
        factory.return_value.delete_app.side_effect = cleanup_error
        with pytest.raises(AirflowException, match=f"The job finished with status: {status}") as failure:
            operator.execute_complete(
                {}, {"namespace": "spark", "pod_name": "application-driver", "status": status}
            )
    assert failure.value.__cause__ is cleanup_error


def test_trigger_error_without_history_service_preserves_diagnostics(trigger_error_event):
    from airflow.exceptions import AirflowException
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator

    operator = SparkOnK8SOperator(
        task_id="trigger_failure_without_history",
        image="spark:test",
        app_path="local:///job.py",
        app_waiter="log",
        on_finish_action="keep",
        deferrable=True,
    )
    with mock.patch("spark_on_k8s.airflow.operators.SparkAppManager") as factory:
        with pytest.raises(AirflowException, match="Kubernetes connection lost"):
            operator.execute_complete({}, trigger_error_event)
    factory.return_value.stream_logs.assert_not_called()
    factory.return_value.delete_app.assert_not_called()


@pytest.mark.parametrize("status", ["Succeeded", "Failed"])
def test_terminal_status_still_replays_logs_before_cleanup(status):
    from airflow.exceptions import AirflowException
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    operator = SparkOnK8SOperator(
        task_id="terminal_logs",
        image="spark:test",
        app_path="local:///job.py",
        app_waiter="log",
        on_finish_action="delete",
        deferrable=True,
        # test_failure_postmortem.py covers the postmortem between log replay and cleanup.
        failure_diagnostics=False,
    )
    event = json.loads(
        json.dumps(
            {
                "namespace": "spark",
                "pod_name": "application-driver",
                "status": SparkAppStatus(status),
            }
        )
    )
    with mock.patch("spark_on_k8s.airflow.operators.SparkAppManager") as factory:
        if status == "Succeeded":
            assert operator.execute_complete({}, event) == "Succeeded"
        else:
            with pytest.raises(AirflowException, match="The job finished with status: Failed"):
                operator.execute_complete({}, event)
    assert factory.return_value.mock_calls == [
        mock.call.stream_logs(namespace="spark", pod_name="application-driver"),
        mock.call.delete_app(namespace="spark", pod_name="application-driver"),
    ]
