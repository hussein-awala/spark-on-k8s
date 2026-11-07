from __future__ import annotations

import json
from datetime import UTC, datetime
from importlib.metadata import distribution

import pytest

from conftest import AIRFLOW_INSTALLED

pytestmark = pytest.mark.skipif(not AIRFLOW_INSTALLED, reason="Airflow extra is not installed")


def test_installed_provider_is_discovered_with_its_extra_link():
    from airflow.providers_manager import ProvidersManager

    installed = distribution("spark-on-k8s")
    entry_points = [
        entry
        for entry in installed.entry_points
        if entry.group == "apache_airflow_provider" and entry.name == "provider_info"
    ]
    assert len(entry_points) == 1
    entry_point = entry_points[0]
    assert entry_point.value == "spark_on_k8s.airflow.provider_info:get_provider_info"
    provider_info = entry_point.load()()
    assert provider_info["package-name"] == "spark-on-k8s"

    manager = ProvidersManager()
    registered = manager.providers["spark-on-k8s"]
    assert registered.version == installed.version
    # Airflow may augment provider data with distribution metadata such as documentation URLs.
    for key, value in provider_info.items():
        assert registered.data[key] == value
    assert "spark_on_k8s.airflow.operator_links.SparkOnK8SOperatorLink" in manager.extra_links_class_names


def test_dag_round_trip_preserves_both_operators_templates_and_extra_links(tmp_path):
    from airflow.serialization import serialized_objects
    from spark_on_k8s.airflow._compat import AIRFLOW_V_3_0_PLUS, DAG
    from spark_on_k8s.airflow.operator_links import SparkOnK8SOperatorLink
    from spark_on_k8s.airflow.operators import SparkOnK8SOperator, SparkSqlOnK8SOperator

    (tmp_path / "queries.sql").write_text("SELECT '{{ ds }};quoted'; SELECT 2;", encoding="utf-8")
    with DAG(
        dag_id="installed_spark_provider",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        catchup=False,
        template_searchpath=[str(tmp_path)],
    ) as dag:
        application = SparkOnK8SOperator(
            task_id="application",
            image="spark:{{ ds_nodash }}",
            app_path="local:///job.py",
            app_arguments=["{{ ds }}"],
        )
        sql = SparkSqlOnK8SOperator(
            task_id="sql",
            image="spark:{{ ds_nodash }}",
            sql="queries.sql",
        )
        application >> sql

    for task in (application, sql):
        task.render_template_fields({"ds": "2026-11-07", "ds_nodash": "20261107"})
    # Airflow 3.4 separates the serializer from its deserialized DAG representation.
    serializer = (
        serialized_objects.DagSerialization
        if hasattr(serialized_objects, "DagSerialization")
        else serialized_objects.SerializedDAG
    )
    restored = serializer.from_dict(json.loads(json.dumps(serializer.to_dict(dag))))

    assert restored.dag_id == dag.dag_id
    assert set(restored.task_ids) == {"application", "sql"}
    assert restored.get_task("application").downstream_task_ids == {"sql"}
    assert restored.get_task("sql").upstream_task_ids == {"application"}
    assert restored.get_task("application").app_arguments == ["2026-11-07"]
    assert restored.get_task("sql").sql == "SELECT '2026-11-07;quoted'; SELECT 2;"
    for original in (application, sql):
        task = restored.get_task(original.task_id)
        assert task.task_type == type(original).__name__
        assert task.image == "spark:20261107"
        assert set(task.template_fields) == set(original.template_fields)
        links = {link.name: link for link in task.operator_extra_links}
        assert set(links) == {"Spark Job UI"}
        assert links["Spark Job UI"].xcom_key == "spark_ui_link"
        if not AIRFLOW_V_3_0_PLUS:
            # Airflow 2 must discover the provider to restore the actual link class.
            assert isinstance(links["Spark Job UI"], SparkOnK8SOperatorLink)
