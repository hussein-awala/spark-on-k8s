from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from spark_on_k8s.airflow.ui_config import ui_plugin_enabled


class SparkTaskUIPlugin(AirflowPlugin):
    """A dormant plugin unless an administrator enables the supported native UI."""

    name = "spark_on_k8s_ui"
    react_apps = []
    fastapi_apps = []


if ui_plugin_enabled():
    from spark_on_k8s.airflow.ui.backend import create_ui_app

    SparkTaskUIPlugin.react_apps = [
        {
            "name": "Spark on Kubernetes",
            "bundle_url": "spark-on-k8s/static/airflow.js",
            "destination": "task_instance",
            "url_route": "spark-on-k8s",
            "applies_to": {"operator": ["SparkOnK8SOperator", "SparkSqlOnK8SOperator"]},
        }
    ]
    SparkTaskUIPlugin.fastapi_apps = [
        {"name": "spark_on_k8s_ui", "url_prefix": "/spark-on-k8s", "app": create_ui_app()}
    ]
