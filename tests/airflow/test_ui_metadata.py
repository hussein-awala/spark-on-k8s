from __future__ import annotations

import json
from unittest import mock

import pytest

from conftest import AIRFLOW_INSTALLED

pytestmark = pytest.mark.skipif(not AIRFLOW_INSTALLED, reason="Airflow extra is not installed")

APP_ID = "a" * 63
POD = "a" * 56 + "-driver"


def make_task(**kwargs):
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator

    return SparkOnK8SOperator(
        task_id="spark",
        image="spark:test",
        app_path="local:///app.py",
        namespace="spark",
        spark_on_k8s_service_url="https://spark.example",
        **kwargs,
    )


def test_submitted_full_id_is_persisted_with_current_try_and_legacy_keys():
    from spark_on_k8s.airflow.ui_config import CONSOLE_XCOM_KEY

    def submit(client, **kwargs):
        from spark_on_k8s.utils.console_models import AIRFLOW_TASK_ANNOTATION

        owner = json.loads(kwargs["driver_annotations"][AIRFLOW_TASK_ANNOTATION])
        assert owner == {
            "dag_id": "dag",
            "run_id": "manual__run",
            "task_id": "spark",
            "map_index": 4,
            "try_number": 3,
        }
        client.last_submitted_app_id = APP_ID
        return POD

    task = make_task(app_waiter="no_wait")
    ti = mock.Mock(
        dag_id="dag",
        run_id="manual__run",
        task_id="spark",
        map_index=4,
        region_id=None,
        region_index=None,
        try_number=3,
        xcom_pull=mock.Mock(return_value=None),
    )
    with (
        mock.patch.object(task, "_list_task_drivers", return_value=[]),
        mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app", autospec=True, side_effect=submit),
    ):
        task.execute({"ti": ti})
    pushed = {call.kwargs["key"]: call.kwargs["value"] for call in ti.xcom_push.call_args_list}
    assert pushed["driver_pod_name"] == POD
    assert pushed["driver_pod_namespace"] == "spark"
    assert pushed["spark_ui_link"] == f"https://spark.example/webserver/ui/spark/{APP_ID}"
    assert json.loads(json.dumps(pushed[CONSOLE_XCOM_KEY])) == {
        "version": 1,
        "namespace": "spark",
        "driver_pod": POD,
        "try_number": 3,
        "service_url": "https://spark.example",
        "app_id": APP_ID,
    }


@pytest.mark.parametrize("has_metadata", [False, True])
def test_adoption_preserves_verified_id_and_never_infers_truncated_id(has_metadata):
    from spark_on_k8s.airflow.ui_config import CONSOLE_XCOM_KEY
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    old_metadata = {"version": 1, "namespace": "spark", "driver_pod": POD, "app_id": APP_ID, "try_number": 3}
    values = {
        "driver_pod_namespace": "spark",
        "driver_pod_name": POD,
        CONSOLE_XCOM_KEY: old_metadata if has_metadata else None,
    }
    ti = mock.Mock(try_number=4, xcom_pull=mock.Mock(side_effect=lambda **kwargs: values.get(kwargs["key"])))
    task = make_task(app_waiter="no_wait")
    with (
        mock.patch(
            "spark_on_k8s.utils.app_manager.SparkAppManager.app_status", return_value=SparkAppStatus.Running
        ),
        mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app") as submit,
    ):
        task.execute({"ti": ti})
    submit.assert_not_called()
    metadata = next(
        call.kwargs["value"] for call in ti.xcom_push.call_args_list if call.kwargs["key"] == CONSOLE_XCOM_KEY
    )
    assert metadata["try_number"] == 4
    if has_metadata:
        assert metadata["app_id"] == APP_ID
    else:
        assert "app_id" not in metadata


def test_deferred_completion_keeps_full_history_link_before_deleting_driver():
    from spark_on_k8s.airflow.ui_config import CONSOLE_XCOM_KEY

    metadata = {"version": 1, "namespace": "spark", "driver_pod": POD, "app_id": APP_ID, "try_number": 3}
    ti = mock.Mock(map_index=4, xcom_pull=mock.Mock(return_value=metadata))
    task = make_task(app_waiter="wait", on_finish_action="delete", deferrable=True)
    with mock.patch("spark_on_k8s.airflow.operators.SparkAppManager") as manager:
        assert (
            task.execute_complete({"ti": ti}, {"namespace": "spark", "pod_name": POD, "status": "Succeeded"})
            == "Succeeded"
        )
    ti.xcom_pull.assert_called_once_with(key=CONSOLE_XCOM_KEY, task_ids="spark", map_indexes=4)
    ti.xcom_push.assert_called_once_with(
        key="spark_ui_link", value=f"https://spark.example/webserver/ui-history/history/{APP_ID}"
    )
    manager.return_value.delete_app.assert_called_once_with(namespace="spark", pod_name=POD)


def test_mismatched_prior_metadata_is_not_adopted():
    task = make_task()
    task._driver_pod_name = POD
    task._restore_console_metadata(
        {
            "ti": mock.Mock(
                xcom_pull=mock.Mock(
                    return_value={"namespace": "other", "driver_pod": POD, "app_id": "other-app"}
                )
            )
        }
    )
    assert task._spark_app_id is None


def test_driver_annotation_is_derived_from_current_task_and_cannot_be_overridden():
    from types import SimpleNamespace

    from spark_on_k8s.utils.console_models import AIRFLOW_TASK_ANNOTATION

    supplied = {"user-note": "preserved", AIRFLOW_TASK_ANNOTATION: "spoofed"}
    task = make_task(driver_annotations=supplied)
    ti = SimpleNamespace(
        dag_id="dag", run_id="manual__date/with:punctuation", task_id="spark", map_index=7, try_number=3
    )
    annotations = task._task_driver_annotations({"ti": ti})
    assert annotations["user-note"] == "preserved"
    assert json.loads(annotations[AIRFLOW_TASK_ANNOTATION]) == {
        "dag_id": "dag",
        "run_id": ti.run_id,
        "task_id": "spark",
        "map_index": 7,
        "try_number": 3,
    }
    assert supplied[AIRFLOW_TASK_ANNOTATION] == "spoofed"


def test_mapped_metadata_restoration_selects_only_the_current_map_index():
    metadata = {"version": 1, "namespace": "spark", "driver_pod": POD, "app_id": APP_ID}
    ti = mock.Mock(map_index=7)
    ti.xcom_pull.side_effect = lambda **kwargs: metadata if kwargs.get("map_indexes") == 7 else [metadata, {}]
    task = make_task()
    task._driver_pod_name = POD
    task._restore_console_metadata({"ti": ti})
    assert task._spark_app_id == APP_ID
