from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from spark_on_k8s.airflow.ui_config import UISettings, ui_plugin_enabled


class SparkTaskUIPlugin(AirflowPlugin):
    """A dormant plugin unless an administrator enables the supported native UI."""

    name = "spark_on_k8s_ui"
    react_apps = []
    fastapi_apps = []


if ui_plugin_enabled():
    from spark_on_k8s.airflow.ui.backend import create_ui_app

    _settings = UISettings.from_environment()
    SparkTaskUIPlugin.react_apps = [
        {
            "name": "Spark on Kubernetes",
            "bundle_url": "spark-on-k8s/static/airflow.js",
            "destination": "task_instance",
            "url_route": "spark-on-k8s",
            # Airflow matches exact class names in the browser; subclasses are listed by the
            # administrator. This only decides where the tab appears, not what it may access.
            "applies_to": {"operator": _settings.task_operators},
        }
    ]
    SparkTaskUIPlugin.fastapi_apps = [
        {"name": "spark_on_k8s_ui", "url_prefix": "/spark-on-k8s", "app": create_ui_app(_settings)}
    ]
