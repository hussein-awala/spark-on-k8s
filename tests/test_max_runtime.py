from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner
from spark_on_k8s.client import SparkOnK8S
from spark_on_k8s.utils.run import MAX_RUNTIME_LIMIT

BASE = {
    "image": "spark:4.0.5",
    "app_path": "local:///opt/spark/work-dir/job.py",
    "app_name": "bounded",
    "app_id_suffix": lambda: "-test",
    "namespace": "spark",
    "app_waiter": "no_wait",
}


def planned_pod(**kwargs):
    plan = SparkOnK8S(k8s_client_manager=MagicMock()).plan_app(**BASE, **kwargs)
    return next(resource for resource in plan.resources if resource["kind"] == "Pod"), plan


def test_max_runtime_sets_the_driver_deadline_and_is_shown_in_the_plan():
    pod, plan = planned_pod(max_runtime=3600)
    assert pod["spec"]["activeDeadlineSeconds"] == 3600
    displayed = next(item for item in plan.to_dict()["resources"] if item["kind"] == "Pod")
    assert displayed["spec"]["activeDeadlineSeconds"] == 3600
    # The deadline belongs to the driver only; executors and Services get no Spark property for it.
    assert not any("deadline" in argument.lower() for argument in pod["spec"]["containers"][0]["args"])


def test_no_deadline_is_set_by_default():
    pod, _ = planned_pod()
    assert "activeDeadlineSeconds" not in pod["spec"]


def test_submission_creates_the_driver_with_the_deadline():
    manager = MagicMock()
    with patch("spark_on_k8s.client.k8s.CoreV1Api") as api:
        api.return_value.create_namespaced_pod.return_value.metadata.uid = "uid"
        SparkOnK8S(k8s_client_manager=manager).submit_app(**BASE, max_runtime=120)
    created = api.return_value.create_namespaced_pod.call_args.kwargs["body"]
    assert created.spec.active_deadline_seconds == 120


@pytest.mark.parametrize("value", [0, -5, True, 1.5, "60", MAX_RUNTIME_LIMIT + 1])
def test_invalid_max_runtime_is_rejected_before_any_cluster_access(value):
    manager = MagicMock()
    with pytest.raises(ValueError, match="max_runtime must be a whole number of seconds"):
        SparkOnK8S(k8s_client_manager=manager).submit_app(**BASE, max_runtime=value)
    manager.client.assert_not_called()


def test_max_runtime_overrides_a_template_deadline_and_otherwise_keeps_it():
    template = {
        "apiVersion": "v1",
        "kind": "Pod",
        "spec": {"activeDeadlineSeconds": 900, "containers": [{"name": "driver"}]},
    }
    kept, _ = planned_pod(driver_pod_template=template)
    assert kept["spec"]["activeDeadlineSeconds"] == 900
    replaced, _ = planned_pod(driver_pod_template=template, max_runtime=60)
    assert replaced["spec"]["activeDeadlineSeconds"] == 60
    assert template["spec"]["activeDeadlineSeconds"] == 900


@pytest.mark.parametrize(
    "command, extra",
    [
        ("app", ["--path", "local:///opt/spark/work-dir/job.py"]),
        ("sql", ["--sql", "SELECT 1"]),
    ],
)
def test_cli_submit_forwards_max_runtime(command, extra):
    from spark_on_k8s.cli.app import submit as app_submit
    from spark_on_k8s.cli.sql import submit as sql_submit

    with patch("spark_on_k8s.client.SparkOnK8S.submit_app", side_effect=RuntimeError("stop")) as submit:
        result = CliRunner().invoke(
            app_submit if command == "app" else sql_submit,
            ["--image", "spark:4.0.5", "--namespace", "spark", "--max-runtime", "600", *extra],
        )
    assert submit.call_args.kwargs["max_runtime"] == 600, result.output


@pytest.mark.parametrize("value", ["0", "1.5", str(MAX_RUNTIME_LIMIT + 1)])
def test_cli_rejects_invalid_max_runtime_as_a_usage_error(value):
    from spark_on_k8s.cli.app import submit

    with patch("spark_on_k8s.client.SparkOnK8S.submit_app") as called:
        result = CliRunner().invoke(
            submit,
            ["--image", "spark:4.0.5", "--path", "local:///job.py", "--max-runtime", value],
        )
    assert result.exit_code == 2
    called.assert_not_called()


def test_cli_submit_sets_no_max_runtime_by_default():
    from spark_on_k8s.cli.app import submit

    with patch("spark_on_k8s.client.SparkOnK8S.submit_app", side_effect=RuntimeError("stop")) as called:
        result = CliRunner().invoke(
            submit, ["--image", "spark:4.0.5", "--path", "local:///job.py", "--output", "json"]
        )
    assert called.call_args.kwargs["max_runtime"] is None
    assert json.loads(result.output)["error"]["message"] == "stop"
