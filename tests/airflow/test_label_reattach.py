from __future__ import annotations

import re
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest import mock
from uuid import UUID

import pytest

from conftest import AIRFLOW_INSTALLED

pytestmark = pytest.mark.skipif(not AIRFLOW_INSTALLED, reason="Airflow extra is not installed")

NAMESPACE = "analytics"
TASK_KEY_LABEL = "spark-on-k8s.io/airflow-task-key"
DAG_ID_LABEL = "spark-on-k8s.io/airflow-dag-id"
TASK_ID_LABEL = "spark-on-k8s.io/airflow-task-id"
# Kubernetes label value syntax.
LABEL_VALUE = re.compile(r"(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?")
TASK = {
    "dag_id": "nightly",
    "run_id": "scheduled__2026-11-08T00:00:00+00:00",
    "task_id": "transform",
    "map_index": -1,
    "try_number": 2,
}


def identity(**overrides):
    from spark_on_k8s.utils.console_models import AirflowTaskContext

    return AirflowTaskContext(**(TASK | overrides))


def task_instance(**overrides):
    values = TASK | {"region_id": None, "region_index": None} | overrides
    return mock.Mock(**values, xcom_pull=mock.Mock(return_value=None))


def driver(name, phase, *, deleting=False):
    from kubernetes import client as k8s

    return k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            name=name,
            namespace=NAMESPACE,
            labels={"spark-role": "driver", "spark-app-id": name.removesuffix("-driver")},
            deletion_timestamp=datetime(2026, 11, 9, tzinfo=UTC) if deleting else None,
        ),
        status=k8s.V1PodStatus(phase=phase) if phase else None,
    )


def make_operator(**kwargs):
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator

    defaults = {
        "task_id": "transform",
        "image": "spark:test",
        "app_path": "local:///job.py",
        "namespace": NAMESPACE,
        "spark_on_k8s_service_url": "https://spark.example",
    }
    return SparkOnK8SOperator(**(defaults | kwargs))


def task_deferred():
    try:
        from airflow.sdk.exceptions import TaskDeferred
    except ImportError:  # Airflow 2
        from airflow.exceptions import TaskDeferred
    return TaskDeferred


def pushed(ti):
    return {call.kwargs["key"]: call.kwargs["value"] for call in ti.xcom_push.call_args_list}


def logged(method, text):
    """Return the arguments of the one log call whose message contains text."""
    (call,) = [call for call in method.call_args_list if text in call.args[0]]
    return call.args


@pytest.fixture
def cluster():
    """Replace only the Kubernetes API calls below the operator and record submissions."""
    from kubernetes import client as k8s
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    api = mock.Mock()
    api.list_namespaced_pod.return_value = k8s.V1PodList(items=[])
    with (
        mock.patch("spark_on_k8s.airflow.operators._AirflowKubernetesClientManager", mock.MagicMock()),
        mock.patch("kubernetes.client.CoreV1Api", return_value=api),
        mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager.wait_for_app") as wait,
        mock.patch(
            "spark_on_k8s.utils.app_manager.SparkAppManager.app_status",
            return_value=SparkAppStatus.Succeeded,
        ) as status,
        mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager.delete_app") as delete,
        mock.patch("spark_on_k8s.client.SparkOnK8S.submit_app", return_value="new-driver") as submit,
    ):
        yield SimpleNamespace(api=api, wait=wait, status=status, delete=delete, submit=submit)


def found(cluster, *pods):
    cluster.api.list_namespaced_pod.return_value.items = list(pods)


def assert_waited_for(cluster, pod_name):
    cluster.wait.assert_called_once_with(
        namespace=NAMESPACE, pod_name=pod_name, poll_interval=10, startup_timeout=0
    )


def test_task_key_is_shared_by_attempts_and_agrees_with_task_ownership():
    base = identity()
    # Pinned: changing the digest would stop retries after an upgrade from finding running drivers.
    assert base.task_key() == "78b8abedc23ba448eff534d4126980ba"
    same = [
        identity(try_number=7),
        # Airflow 3 reports the sentinel region explicitly; Airflow 2 reports none.
        identity(region_id=UUID(int=0), region_index=-1),
    ]
    different = [
        identity(dag_id="nightly_v2"),
        identity(run_id="manual__2026-11-08T00:00:00+00:00"),
        identity(task_id="group.transform"),
        identity(map_index=3),
        identity(region_index=4),
        identity(region_id=UUID(int=9)),
        # Field boundaries are part of the digest.
        identity(dag_id="nightly.transform", task_id="x"),
    ]
    for other in same + different:
        assert (other.task_key() == base.task_key()) is base.owns_same_task(other)
        # The ownership annotation written beside the label reproduces the same key.
        restored = type(other).model_validate_json(other.model_dump_json(exclude_none=True))
        assert restored.task_key() == other.task_key()
    assert len({other.task_key() for other in different}) == len(different)
    assert identity(dag_id="nightly", task_id="transform.x").task_key() != different[-1].task_key()


@pytest.mark.parametrize(
    "dag_id, readable",
    [
        ("nightly", "nightly"),
        ("group.daily-load_v2", "group.daily-load_v2"),
        ("_internal_", "internal"),
        ("café", "caf"),
        ("d" * 250, "d" * 63),
        ("___", None),
    ],
)
def test_driver_labels_are_valid_and_reserved_labels_cannot_be_supplied(dag_id, readable):
    supplied = {"team": "data", TASK_KEY_LABEL: "forged", DAG_ID_LABEL: "forged", TASK_ID_LABEL: "forged"}
    task = make_operator(driver_labels=supplied)

    labels = task._task_driver_labels({"ti": task_instance(dag_id=dag_id)})

    assert labels == {
        "team": "data",
        TASK_KEY_LABEL: identity(dag_id=dag_id).task_key(),
        TASK_ID_LABEL: "transform",
        **({DAG_ID_LABEL: readable} if readable else {}),
    }
    assert all(len(value) <= 63 and LABEL_VALUE.fullmatch(value) for value in labels.values())
    assert supplied[TASK_KEY_LABEL] == "forged"


def test_unbound_invocation_gets_no_task_labels_or_lookup(cluster):
    task = make_operator(driver_labels={"team": "data", TASK_KEY_LABEL: "forged"})

    task.execute({"ti": mock.MagicMock(xcom_pull=mock.Mock(return_value=None))})

    cluster.api.list_namespaced_pod.assert_not_called()
    assert cluster.submit.call_args.kwargs["driver_labels"] == {"team": "data"}


def test_first_attempt_without_earlier_drivers_submits_a_labelled_driver(cluster):
    ti = task_instance()

    assert make_operator().execute({"ti": ti}) == "Succeeded"

    cluster.api.list_namespaced_pod.assert_called_once_with(
        namespace=NAMESPACE, label_selector=f"spark-role=driver,{TASK_KEY_LABEL}={identity().task_key()}"
    )
    assert cluster.submit.call_args.kwargs["driver_labels"][TASK_KEY_LABEL] == identity().task_key()
    assert_waited_for(cluster, "new-driver")
    assert pushed(ti)["driver_pod_name"] == "new-driver"


@pytest.mark.parametrize("phase", ["Pending", "Running"])
def test_retry_without_xcom_adopts_the_active_driver_found_by_task_key(cluster, phase):
    from spark_on_k8s.airflow.ui_config import CONSOLE_XCOM_KEY

    found(
        cluster,
        driver("first-driver", "Failed"),
        driver("second-driver", phase),
        driver("cleared-driver", "Succeeded"),
    )
    ti = task_instance(try_number=3)

    assert make_operator().execute({"ti": ti}) == "Succeeded"

    cluster.submit.assert_not_called()
    assert_waited_for(cluster, "second-driver")
    values = pushed(ti)
    assert values["driver_pod_namespace"] == NAMESPACE
    assert values["driver_pod_name"] == "second-driver"
    assert values[CONSOLE_XCOM_KEY]["app_id"] == "second"
    assert values[CONSOLE_XCOM_KEY]["try_number"] == 3
    assert values["spark_ui_link"] == "https://spark.example/webserver/ui-history/history/second"


def test_deferred_retry_watches_the_adopted_driver(cluster):
    found(cluster, driver("running-driver", "Running"))
    task = make_operator(deferrable=True)

    with pytest.raises(task_deferred()) as deferred:
        task.execute({"ti": task_instance()})

    cluster.submit.assert_not_called()
    assert deferred.value.trigger.driver_pod_name == "running-driver"
    assert deferred.value.trigger.namespace == NAMESPACE


@pytest.mark.parametrize("count", [1, 2])
def test_succeeded_driver_is_reported_but_rerun_by_default(cluster, count):
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator

    found(cluster, *(driver(f"done{index}-driver", "Succeeded") for index in range(count)))
    with mock.patch.object(SparkOnK8SOperator, "log", new_callable=mock.PropertyMock) as log:
        assert make_operator().execute({"ti": task_instance()}) == "Succeeded"

    cluster.submit.assert_called_once()
    assert_waited_for(cluster, "new-driver")
    warning = logged(log.return_value.warning, "reattach_succeeded is False")
    assert warning[1:] == (", ".join(f"done{index}-driver" for index in range(count)), NAMESPACE)


@pytest.mark.parametrize("deferrable", [False, True])
def test_opt_in_adopts_one_succeeded_driver_without_rerunning_spark(cluster, deferrable):
    found(cluster, driver("failed-driver", "Failed"), driver("done-driver", "Succeeded"))
    task = make_operator(reattach_succeeded=True, on_finish_action="delete", deferrable=deferrable)
    ti = task_instance()

    if deferrable:
        with pytest.raises(task_deferred()) as deferred:
            task.execute({"ti": ti})
        assert deferred.value.trigger.driver_pod_name == "done-driver"
    else:
        assert task.execute({"ti": ti}) == "Succeeded"
        assert_waited_for(cluster, "done-driver")
        cluster.delete.assert_called_once_with(namespace=NAMESPACE, pod_name="done-driver")
    cluster.submit.assert_not_called()
    assert pushed(ti)["driver_pod_name"] == "done-driver"


def test_active_driver_is_preferred_to_a_succeeded_one_with_the_opt_in(cluster):
    found(cluster, driver("done-driver", "Succeeded"), driver("rerun-driver", "Running"))

    make_operator(reattach_succeeded=True).execute({"ti": task_instance()})

    cluster.submit.assert_not_called()
    assert_waited_for(cluster, "rerun-driver")


def test_failed_drivers_are_not_adopted(cluster):
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator

    found(cluster, driver("a-driver", "Failed"), driver("b-driver", "Failed"))
    with mock.patch.object(SparkOnK8SOperator, "log", new_callable=mock.PropertyMock) as log:
        make_operator(reattach_succeeded=True).execute({"ti": task_instance()})

    cluster.submit.assert_called_once()
    assert_waited_for(cluster, "new-driver")
    assert logged(log.return_value.info, "Not adopting failed")[1:] == ("a-driver, b-driver", NAMESPACE)


def test_drivers_being_deleted_are_ignored(cluster):
    found(
        cluster,
        driver("timed-out-driver", "Pending", deleting=True),
        driver("stopping-driver", "Running", deleting=True),
        driver("lost-driver", "Unknown", deleting=True),
    )

    make_operator().execute({"ti": task_instance()})

    cluster.submit.assert_called_once()
    assert_waited_for(cluster, "new-driver")


@pytest.mark.parametrize(
    "phases, reattach_succeeded",
    [
        (("Running", "Pending"), False),
        (("Running", "Running"), True),
        (("Succeeded", "Succeeded"), True),
    ],
)
def test_several_adoptable_drivers_are_never_guessed(cluster, phases, reattach_succeeded):
    from airflow.exceptions import AirflowException

    found(cluster, driver("b-driver", phases[1]), driver("a-driver", phases[0]), driver("c-driver", "Failed"))
    ti = task_instance()

    with pytest.raises(AirflowException, match=r"Several Spark drivers .*\(a-driver, b-driver\)"):
        make_operator(reattach_succeeded=reattach_succeeded).execute({"ti": ti})

    cluster.submit.assert_not_called()
    cluster.wait.assert_not_called()
    ti.xcom_push.assert_not_called()


@pytest.mark.parametrize("phase", ["Unknown", None])
def test_indeterminate_driver_blocks_resubmission_and_adoption(cluster, phase):
    from airflow.exceptions import AirflowException

    found(cluster, driver("lost-driver", phase), driver("other-driver", "Running"))
    ti = task_instance()

    with pytest.raises(AirflowException, match="lost-driver .* indeterminate status"):
        make_operator().execute({"ti": ti})

    cluster.submit.assert_not_called()
    ti.xcom_push.assert_not_called()


@pytest.mark.parametrize("failure", ["forbidden", "server", "transport", "timeout"])
def test_lookup_failure_is_not_absence(cluster, failure):
    from airflow.exceptions import AirflowException
    from kubernetes.client import ApiException

    error = {
        "forbidden": ApiException(status=403),
        "server": ApiException(status=500),
        "transport": ConnectionError("unreachable"),
        "timeout": TimeoutError("read timed out"),
    }[failure]
    cluster.api.list_namespaced_pod.side_effect = error
    ti = task_instance()

    with pytest.raises(AirflowException, match="refusing to submit another application") as caught:
        make_operator().execute({"ti": ti})

    assert caught.value.__cause__ is error
    cluster.submit.assert_not_called()
    ti.xcom_push.assert_not_called()


def test_label_lookup_can_be_disabled_but_drivers_are_still_labelled(cluster):
    found(cluster, driver("running-driver", "Running"))

    make_operator(reattach_by_label=False).execute({"ti": task_instance()})

    cluster.api.list_namespaced_pod.assert_not_called()
    cluster.submit.assert_called_once()
    assert cluster.submit.call_args.kwargs["driver_labels"][TASK_KEY_LABEL] == identity().task_key()


def retained_xcom(ti, pod_name):
    values = {"driver_pod_namespace": NAMESPACE, "driver_pod_name": pod_name}
    ti.xcom_pull.side_effect = lambda **kwargs: values.get(kwargs["key"])
    return ti


def test_retained_xcom_record_takes_precedence_over_labels(cluster):
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    # These labelled drivers would be ambiguous; the recorded driver is adopted without a lookup.
    found(cluster, driver("a-driver", "Running"), driver("b-driver", "Running"))
    cluster.status.side_effect = [SparkAppStatus.Running, SparkAppStatus.Succeeded]
    ti = retained_xcom(task_instance(), "recorded-driver")

    assert make_operator().execute({"ti": ti}) == "Succeeded"

    cluster.api.list_namespaced_pod.assert_not_called()
    cluster.submit.assert_not_called()
    assert_waited_for(cluster, "recorded-driver")


def test_terminal_xcom_record_falls_back_to_the_task_key(cluster):
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    # The receipt of a later attempt that crashed before persisting XCom is found by its label.
    found(cluster, driver("recorded-driver", "Failed"), driver("later-driver", "Running"))
    cluster.status.side_effect = [SparkAppStatus.Failed, SparkAppStatus.Succeeded]
    ti = retained_xcom(task_instance(), "recorded-driver")

    make_operator().execute({"ti": ti})

    cluster.submit.assert_not_called()
    assert_waited_for(cluster, "later-driver")
    assert pushed(ti)["driver_pod_name"] == "later-driver"


def test_xcom_lookup_failure_fails_before_the_label_lookup(cluster):
    from airflow.exceptions import AirflowException
    from kubernetes.client import ApiException

    cluster.status.side_effect = ApiException(status=503)
    ti = retained_xcom(task_instance(), "recorded-driver")

    with pytest.raises(AirflowException, match="Cannot verify previously recorded Spark driver"):
        make_operator().execute({"ti": ti})

    cluster.api.list_namespaced_pod.assert_not_called()
    cluster.submit.assert_not_called()


@pytest.mark.parametrize("reattach_succeeded", [False, True])
def test_succeeded_xcom_record_follows_the_same_opt_in(cluster, reattach_succeeded):
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    cluster.status.side_effect = [SparkAppStatus.Succeeded, SparkAppStatus.Succeeded]
    ti = retained_xcom(task_instance(), "recorded-driver")

    make_operator(reattach_succeeded=reattach_succeeded).execute({"ti": ti})

    if reattach_succeeded:
        cluster.api.list_namespaced_pod.assert_not_called()
        cluster.submit.assert_not_called()
        assert_waited_for(cluster, "recorded-driver")
    else:
        cluster.api.list_namespaced_pod.assert_called_once()
        cluster.submit.assert_called_once()
        assert_waited_for(cluster, "new-driver")


def test_sql_operator_uses_the_same_task_key(cluster):
    from spark_on_k8s.airflow.operators import SparkSqlOnK8SOperator

    found(cluster, driver("sql-driver", "Running"))
    task = SparkSqlOnK8SOperator(task_id="transform", image="spark:test", namespace=NAMESPACE, sql="SELECT 1")

    task.execute({"ti": task_instance()})

    cluster.submit.assert_not_called()
    assert_waited_for(cluster, "sql-driver")


@pytest.mark.parametrize("map_index", [0, 1])
def test_mapped_task_instances_label_and_look_up_only_their_own_drivers(cluster, map_index):
    # Mapped instances share dag_id, run_id and task_id; only map_index tells their drivers apart.
    own = identity(map_index=map_index).task_key()
    assert own != identity(map_index=1 - map_index).task_key()

    make_operator().execute({"ti": task_instance(map_index=map_index, try_number=1)})

    (lookup,) = [
        call
        for call in cluster.api.list_namespaced_pod.call_args_list
        if TASK_KEY_LABEL in call.kwargs.get("label_selector", "")
    ]
    assert lookup.kwargs["label_selector"] == f"spark-role=driver,{TASK_KEY_LABEL}={own}"
    assert cluster.submit.call_args.kwargs["driver_labels"][TASK_KEY_LABEL] == own
