from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from conftest import AIRFLOW_INSTALLED

pytestmark = pytest.mark.skipif(not AIRFLOW_INSTALLED, reason="Airflow extra is not installed")


@pytest.mark.parametrize("kind", ["application", "sql"])
def test_rendered_driver_environment_reaches_submitted_driver_pod(kind, monkeypatch):
    from kubernetes import client as k8s
    from spark_on_k8s.airflow import operators

    api = MagicMock()
    api.create_namespaced_pod.return_value = SimpleNamespace(
        metadata=SimpleNamespace(name="environment-rendered-driver", uid="driver-uid")
    )
    monkeypatch.setattr(k8s, "CoreV1Api", MagicMock(return_value=api))
    monkeypatch.setattr(operators, "_AirflowKubernetesClientManager", MagicMock())
    options = {
        "task_id": "environment",
        "image": "spark-test:v1",
        "namespace": "analytics",
        "service_account": "spark",
        "app_name": "environment",
        "app_id_suffix": "-rendered",
        "app_waiter": "no_wait",
        "secret_values": {},
        "driver_env_vars_from_secrets": [],
        "spark_conf": {
            "spark.kubernetes.driverEnv.AIRFLOW_RUN_TAG": "{{ run_tag }}",
        },
        "driver_container_name": "{{ driver_container }}",
        "driver_pod_template": {
            "apiVersion": "v1",
            "kind": "Pod",
            "spec": {
                "containers": [
                    {"name": "observer", "image": "observer:v1"},
                    {"name": "{{ driver_container }}", "env": [{"name": "PROFILE", "value": "shared"}]},
                ]
            },
        },
    }
    if kind == "application":
        operator = operators.SparkOnK8SOperator(app_path="local:///job.py", **options)
    else:
        operator = operators.SparkSqlOnK8SOperator(sql="SELECT 1", **options)
    task_instance = MagicMock(
        dag_id="driver_environment",
        run_id="manual__rendered",
        task_id="environment",
        map_index=-1,
        try_number=1,
        region_id=None,
        region_index=None,
    )
    task_instance.xcom_pull.return_value = None
    context = {"run_tag": "scheduled_2026_11_08", "driver_container": "spark-main", "ti": task_instance}

    operator.render_template_fields(context)
    operator.execute(context)

    api.create_namespaced_pod.assert_called_once()
    pod = api.create_namespaced_pod.call_args.kwargs["body"]
    assert pod["metadata"]["name"] == "environment-rendered-driver"
    assert pod["metadata"]["namespace"] == "analytics"
    driver, observer = pod["spec"]["containers"]
    assert driver["name"] == "spark-main"
    environment = {variable["name"]: variable for variable in driver["env"]}
    assert environment["AIRFLOW_RUN_TAG"] == {"name": "AIRFLOW_RUN_TAG", "value": "scheduled_2026_11_08"}
    assert environment["PROFILE"]["value"] == "shared"
    assert "env" not in observer
    assert operator.spark_conf["spark.kubernetes.driverEnv.AIRFLOW_RUN_TAG"] == "scheduled_2026_11_08"
    api.create_namespaced_service.assert_called_once()
    assert api.create_namespaced_config_map.call_count == (1 if kind == "sql" else 0)
