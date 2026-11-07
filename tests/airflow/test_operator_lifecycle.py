from __future__ import annotations

import asyncio
import json
from unittest import mock

import pytest

from conftest import AIRFLOW_INSTALLED

pytestmark = pytest.mark.skipif(not AIRFLOW_INSTALLED, reason="Airflow extra is not installed")

FINISH_CASES = [
    ("keep", "Succeeded", False),
    ("keep", "Failed", False),
    ("delete", "Succeeded", True),
    ("delete", "Failed", True),
    ("keep_if_failed", "Succeeded", True),
    ("keep_if_failed", "Failed", False),
]


@pytest.fixture
def make_operator():
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator

    def create(**kwargs):
        defaults = {
            "task_id": "spark_lifecycle",
            "image": "spark:test",
            "app_path": "local:///job.py",
            "namespace": "spark",
            "spark_on_k8s_service_url": "https://spark.example",
        }
        return SparkOnK8SOperator(**(defaults | kwargs))

    return create


@pytest.fixture
def task_context():
    return {"ti": mock.Mock(xcom_pull=mock.Mock(return_value=None))}


@pytest.fixture
def app_manager():
    from spark_on_k8s.utils.app_manager import SparkAppManager

    manager = mock.create_autospec(SparkAppManager, instance=True)
    factory = mock.Mock(return_value=manager)
    with (
        mock.patch("spark_on_k8s.airflow.operators.SparkAppManager", factory),
        mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager", factory),
    ):
        yield factory, manager


def assert_completion(operator, context, status, execute):
    from airflow.exceptions import AirflowException

    if status == "Succeeded":
        assert execute() == status
    else:
        with pytest.raises(AirflowException, match=f"The job finished with status: {status}"):
            execute()
    context["ti"].xcom_push.assert_any_call(
        key="spark_ui_link",
        value="https://spark.example/webserver/ui-history/history/application",
    )
    assert operator._driver_pod_name == "application-driver"


@pytest.mark.parametrize("finish_action,status,should_delete", FINISH_CASES)
def test_waiting_operator_preserves_failed_jobs_only_when_requested(
    make_operator, task_context, app_manager, finish_action, status, should_delete
):
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    _, manager = app_manager
    manager.app_status.return_value = SparkAppStatus(status)
    operator = make_operator(on_finish_action=finish_action, poll_interval=7)
    with mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app", return_value="application-driver"):
        assert_completion(operator, task_context, status, lambda: operator.execute(task_context))

    manager.wait_for_app.assert_called_once_with(
        namespace="spark", pod_name="application-driver", poll_interval=7, startup_timeout=0
    )
    manager.stream_logs.assert_not_called()
    manager.app_status.assert_called_once_with(namespace="spark", pod_name="application-driver")
    if should_delete:
        manager.delete_app.assert_called_once_with(namespace="spark", pod_name="application-driver")
    else:
        manager.delete_app.assert_not_called()
    links = [
        call.kwargs["value"]
        for call in task_context["ti"].xcom_push.call_args_list
        if call.kwargs["key"] == "spark_ui_link"
    ]
    assert links == [
        "https://spark.example/webserver/ui/spark/application",
        "https://spark.example/webserver/ui-history/history/application",
    ]


@pytest.mark.parametrize("finish_action,status,should_delete", FINISH_CASES)
@pytest.mark.parametrize("waiter", ["wait", "log"])
def test_trigger_json_event_completes_with_logs_links_and_finish_policy(
    make_operator, task_context, app_manager, finish_action, status, should_delete, waiter
):
    from spark_on_k8s.airflow.triggers import SparkOnK8STrigger
    from spark_on_k8s.utils.async_app_manager import AsyncSparkAppManager
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    trigger = SparkOnK8STrigger(
        driver_pod_name="application-driver",
        namespace="submitted-namespace",
        kubernetes_conn_id="spark_connection",
        poll_interval=7,
    )
    trigger_path, trigger_kwargs = trigger.serialize()
    assert trigger_path == "spark_on_k8s.airflow.triggers.SparkOnK8STrigger"
    restored_trigger = SparkOnK8STrigger(**json.loads(json.dumps(trigger_kwargs)))
    async_manager = mock.create_autospec(AsyncSparkAppManager, instance=True)
    async_manager.app_status.return_value = SparkAppStatus(status)

    async def collect():
        return [event.payload async for event in restored_trigger.run()]

    with mock.patch(
        "spark_on_k8s.utils.async_app_manager.AsyncSparkAppManager", return_value=async_manager
    ) as async_factory:
        events = asyncio.run(collect())
    assert len(events) == 1
    assert events[0]["status"] is SparkAppStatus(status)
    event = json.loads(json.dumps(events[0]))
    assert event == {
        "namespace": "submitted-namespace",
        "pod_name": "application-driver",
        "status": status,
    }
    assert async_factory.call_args.kwargs["k8s_client_manager"].kubernetes_conn_id == "spark_connection"
    async_manager.wait_for_app.assert_awaited_once_with(
        namespace="submitted-namespace", pod_name="application-driver", poll_interval=7, startup_deadline=None
    )
    async_manager.app_status.assert_awaited_once_with(
        namespace="submitted-namespace", pod_name="application-driver"
    )

    _, manager = app_manager
    operator = make_operator(app_waiter=waiter, on_finish_action=finish_action, deferrable=True)
    assert_completion(operator, task_context, status, lambda: operator.execute_complete(task_context, event))
    assert operator.namespace == "submitted-namespace"
    operations = []
    if waiter == "log":
        operations.append(
            mock.call.stream_logs(namespace="submitted-namespace", pod_name="application-driver")
        )
    if should_delete:
        operations.append(
            mock.call.delete_app(namespace="submitted-namespace", pod_name="application-driver")
        )
    assert manager.mock_calls == operations


def test_trigger_error_retains_diagnostics_after_deferred_cleanup(make_operator, task_context, app_manager):
    from airflow.exceptions import AirflowException
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

    operator = make_operator(on_finish_action="delete", deferrable=True)
    with pytest.raises(AirflowException, match="SparkOnK8STrigger failed") as failure:
        operator.execute_complete(task_context, event)
    assert event["error"] in str(failure.value)
    assert event["stacktrace"] in str(failure.value)
    _, manager = app_manager
    manager.delete_app.assert_called_once_with(namespace="spark", pod_name="application-driver")
    manager.stream_logs.assert_not_called()


@pytest.mark.parametrize("waiter", ["wait", "log"])
def test_synchronous_startup_timeout_deletes_even_with_keep_policy(
    make_operator, task_context, app_manager, waiter
):
    from airflow.exceptions import AirflowException

    _, manager = app_manager
    manager.wait_for_app.side_effect = TimeoutError("still pending")
    manager.stream_logs.side_effect = TimeoutError("still pending")
    operator = make_operator(app_waiter=waiter, on_finish_action="keep", startup_timeout=30)
    with (
        mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app", return_value="application-driver"),
        pytest.raises(AirflowException, match="Spark application startup timeout exceeded"),
    ):
        operator.execute(task_context)
    if waiter == "log":
        manager.stream_logs.assert_called_once_with(
            namespace="spark", pod_name="application-driver", startup_timeout=30
        )
        manager.wait_for_app.assert_not_called()
    else:
        manager.wait_for_app.assert_called_once_with(
            namespace="spark", pod_name="application-driver", poll_interval=10, startup_timeout=30
        )
        manager.stream_logs.assert_not_called()
    manager.delete_app.assert_called_once_with(namespace="spark", pod_name="application-driver")
    manager.app_status.assert_not_called()
    task_context["ti"].xcom_push.assert_any_call(
        key="spark_ui_link", value="https://spark.example/webserver/ui/spark/application"
    )


@pytest.mark.parametrize("kill_action", ["keep", "kill", "delete"])
@pytest.mark.parametrize("pod_name", [None, "application-driver"])
def test_task_cancellation_respects_policy_and_missing_driver(
    make_operator, task_context, app_manager, kill_action, pod_name
):
    operator = make_operator(on_kill_action=kill_action)
    operator._driver_pod_name = pod_name
    factory, manager = app_manager
    with mock.patch(
        "spark_on_k8s.airflow.operators.get_current_context", return_value=task_context
    ) as current_context:
        operator.on_kill()
    if kill_action == "keep" or pod_name is None:
        factory.assert_not_called()
        current_context.assert_not_called()
        task_context["ti"].xcom_push.assert_not_called()
    else:
        factory.assert_called_once()
        assert manager.mock_calls == [
            getattr(mock.call, f"{kill_action}_app")(namespace="spark", pod_name="application-driver")
        ]
        current_context.assert_called_once_with()
        task_context["ti"].xcom_push.assert_called_once_with(
            key="spark_ui_link",
            value="https://spark.example/webserver/ui-history/history/application",
        )
