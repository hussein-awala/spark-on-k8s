"""A vanished driver fails the task as an unknown outcome; a cleanup failure after success names it."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest import mock

import pytest

from conftest import AIRFLOW_INSTALLED

pytestmark = pytest.mark.skipif(not AIRFLOW_INSTALLED, reason="Airflow extra is not installed")

DELETED = "Pod application-driver was deleted, or does not exist, before a final status was observed"


@pytest.fixture
def cluster():
    """Replace only the Kubernetes-facing manager calls below the operator."""
    from kubernetes import client as k8s

    api = mock.Mock()
    api.list_namespaced_pod.return_value = k8s.V1PodList(items=[])
    with (
        mock.patch("spark_on_k8s.airflow.operators._AirflowKubernetesClientManager", mock.MagicMock()),
        mock.patch("kubernetes.client.CoreV1Api", return_value=api),
        mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager.wait_for_app") as wait,
        mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager.app_status") as status,
        mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager.delete_app") as delete,
        mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager.stream_logs") as logs,
        mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app", return_value="application-driver") as submit,
    ):
        yield SimpleNamespace(wait=wait, status=status, delete=delete, logs=logs, submit=submit)


def operator(**kwargs):
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator

    defaults = {
        "task_id": "outcome",
        "image": "spark:test",
        "app_path": "local:///job.py",
        "namespace": "spark",
        "on_finish_action": "delete",
        "failure_diagnostics": False,
    }
    return SparkOnK8SOperator(**(defaults | kwargs))


def context():
    return {"ti": mock.Mock(xcom_pull=mock.Mock(return_value=None), map_index=-1)}


def test_synchronous_wait_fails_when_the_driver_vanishes(cluster):
    from airflow.exceptions import AirflowException
    from spark_on_k8s.utils.spark_app_status import SparkAppDeletedError

    cluster.wait.side_effect = SparkAppDeletedError(DELETED + "; its outcome is unknown")

    with pytest.raises(AirflowException, match="outcome is unknown; failing the task"):
        operator().execute(context())

    cluster.status.assert_not_called()
    cluster.delete.assert_not_called()


def test_trigger_reports_a_vanished_driver_as_its_own_event():
    from spark_on_k8s.airflow.triggers import SparkOnK8STrigger
    from spark_on_k8s.utils.spark_app_status import SparkAppDeletedError

    trigger = SparkOnK8STrigger(driver_pod_name="application-driver", namespace="spark")

    async def collect():
        return [event.payload async for event in trigger.run()]

    with mock.patch(
        "spark_on_k8s.utils.async_app_manager.AsyncSparkAppManager.wait_for_app",
        new=mock.AsyncMock(side_effect=SparkAppDeletedError(DELETED)),
    ):
        (event,) = asyncio.run(collect())

    assert event == {
        "namespace": "spark",
        "pod_name": "application-driver",
        "status": "deleted",
        "error": DELETED,
    }


@pytest.mark.parametrize("waiter", ["wait", "log"])
def test_deferred_completion_fails_a_vanished_driver_without_replay_or_cleanup(cluster, waiter):
    from airflow.exceptions import AirflowException

    event = {"namespace": "spark", "pod_name": "application-driver", "status": "deleted", "error": DELETED}

    with pytest.raises(AirflowException, match="failing the task"):
        operator(app_waiter=waiter, deferrable=True).execute_complete(context(), event)

    cluster.logs.assert_not_called()
    cluster.delete.assert_not_called()


@pytest.mark.parametrize("finish_action", ["delete", "keep_if_failed"])
@pytest.mark.parametrize("deferrable", [False, True])
def test_cleanup_failure_after_success_names_the_success(cluster, finish_action, deferrable):
    from airflow.exceptions import AirflowException
    from kubernetes.client import ApiException
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    cleanup_error = ApiException(status=503, reason="Kubernetes unavailable")
    cluster.delete.side_effect = cleanup_error
    cluster.status.return_value = SparkAppStatus.Succeeded
    task = operator(on_finish_action=finish_action, deferrable=deferrable)

    with pytest.raises(AirflowException, match="status: Succeeded, but its cleanup failed") as raised:
        if deferrable:
            event = {"namespace": "spark", "pod_name": "application-driver", "status": "Succeeded"}
            task.execute_complete(context(), event)
        else:
            task.execute(context())

    assert raised.value.__cause__ is cleanup_error
