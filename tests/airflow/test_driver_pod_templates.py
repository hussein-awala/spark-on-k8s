from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from conftest import AIRFLOW_INSTALLED

pytestmark = pytest.mark.skipif(not AIRFLOW_INSTALLED, reason="Airflow extra is not installed")


@pytest.mark.parametrize("template_kind", ["mapping", "yaml", "path"])
def test_operator_renders_and_submits_driver_template(template_kind, monkeypatch, tmp_path):
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator
    from spark_on_k8s.client import SparkOnK8S

    template = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"labels": {"team": "{{ team }}"}},
        "spec": {"containers": [{"name": "{{ container }}"}]},
    }
    expected = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"labels": {"team": "analytics"}},
        "spec": {"containers": [{"name": "spark-main"}]},
    }
    if template_kind == "yaml":
        import yaml

        template, expected = yaml.safe_dump(template), yaml.safe_dump(expected)
    elif template_kind == "path":
        template = str(tmp_path / "{{ team }}-driver.yaml")
        expected = str(tmp_path / "analytics-driver.yaml")
    submit = MagicMock(return_value="report-driver")
    monkeypatch.setattr(SparkOnK8S, "submit_app", submit)
    operator = SparkOnK8SOperator(
        task_id="report",
        image="spark:v1",
        app_path="local:///job.py",
        app_waiter="no_wait",
        driver_pod_template=template,
        driver_container_name="{{ container }}",
    )
    context = {"team": "analytics", "container": "spark-main", "ti": MagicMock()}
    context["ti"].xcom_pull.return_value = None
    operator.render_template_fields(context)
    operator.execute(context)

    actual = submit.call_args.kwargs["driver_pod_template"]
    if template_kind == "yaml":
        assert yaml.safe_load(actual) == yaml.safe_load(expected)
    else:
        assert actual == expected
    assert submit.call_args.kwargs["driver_container_name"] == "spark-main"


def test_driver_template_survives_dag_serialization_for_application_and_sql():
    from airflow.serialization import serialized_objects
    from spark_on_k8s.airflow._compat import DAG
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator, SparkSqlOnK8SOperator

    template = {"apiVersion": "v1", "kind": "Pod", "spec": {"containers": [{"name": "{{ driver }}"}]}}
    with DAG(dag_id="driver_templates", schedule=None, start_date=datetime(2026, 1, 1, tzinfo=UTC)) as dag:
        tasks = [
            SparkOnK8SOperator(
                task_id="application",
                image="spark:v1",
                app_path="local:///job.py",
                driver_pod_template=template,
                driver_container_name="{{ driver }}",
            ),
            SparkSqlOnK8SOperator(
                task_id="sql",
                image="spark:v1",
                sql="SELECT 1",
                driver_pod_template=template,
                driver_container_name="{{ driver }}",
            ),
        ]
    for task in tasks:
        task.render_template_fields({"driver": "spark-main"})
    serializer = (
        serialized_objects.DagSerialization
        if hasattr(serialized_objects, "DagSerialization")
        else serialized_objects.SerializedDAG
    )
    restored = serializer.from_dict(json.loads(json.dumps(serializer.to_dict(dag))))
    for task in restored.tasks:
        assert task.driver_container_name == "spark-main"
        assert task.driver_pod_template["spec"]["containers"] == [{"name": "spark-main"}]
