from __future__ import annotations

import os
import subprocess
import sys
from unittest import mock

import pytest


@pytest.mark.parametrize(
    "airflow_version,enabled", [("2.11.2", False), ("3.0.3", False), ("3.3.0", False), ("3.4.0", True)]
)
def test_native_plugin_activation_is_opt_in_and_has_a_34_floor(monkeypatch, airflow_version, enabled):
    from spark_on_k8s.airflow.ui_config import ui_plugin_enabled

    monkeypatch.setenv("SPARK_ON_K8S_AIRFLOW_SERVICE_URL", "https://spark.example/base/")
    with mock.patch("spark_on_k8s.airflow.ui_config.version", return_value=airflow_version):
        assert ui_plugin_enabled() is enabled


def test_plugin_disabled_without_admin_configuration(monkeypatch):
    from spark_on_k8s.airflow.ui_config import ui_plugin_enabled

    monkeypatch.delenv("SPARK_ON_K8S_AIRFLOW_SERVICE_URL", raising=False)
    assert not ui_plugin_enabled()


@pytest.mark.parametrize(
    "url",
    [
        "file:///etc/passwd",
        "https://user:secret@spark.example",
        "https://spark.example/?token=secret",
        "https://spark.example/#fragment",
        "https://spark.example:bad",
        "https://spark.example\\@evil.example",
    ],
)
def test_invalid_upstream_configuration_disables_activation(monkeypatch, url):
    from spark_on_k8s.airflow.ui_config import ui_plugin_enabled

    monkeypatch.setenv("SPARK_ON_K8S_AIRFLOW_SERVICE_URL", url)
    with mock.patch("spark_on_k8s.airflow.ui_config.version", return_value="3.4.0"):
        assert not ui_plugin_enabled()


def test_provider_discovery_does_not_import_optional_ui_dependencies():
    code = """
import importlib.abc, sys
class BlockUI(importlib.abc.MetaPathFinder):
    def find_spec(self, name, *args):
        if name.startswith(("fastapi", "httpx", "websockets", "airflow.api_fastapi",
                            "spark_on_k8s.api", "spark_on_k8s.airflow.ui.")):
            raise AssertionError("Unexpected optional import: " + name)
sys.meta_path.insert(0, BlockUI())
from spark_on_k8s.airflow.provider_info import get_provider_info
get_provider_info()
"""
    env = {**os.environ}
    env.pop("SPARK_ON_K8S_AIRFLOW_SERVICE_URL", None)
    subprocess.run(
        [sys.executable, "-I", "-B", "-c", code], check=True, env=env, capture_output=True, text=True
    )


@pytest.mark.parametrize("configured", [False, True])
def test_direct_plugin_activates_only_with_supported_configuration(configured):
    from conftest import AIRFLOW_INSTALLED

    if not AIRFLOW_INSTALLED:
        pytest.skip("Airflow extra is not installed")
    code = """
import importlib.abc, sys
from spark_on_k8s.airflow.ui_config import ui_plugin_enabled
enabled = ui_plugin_enabled()
class BlockBackend(importlib.abc.MetaPathFinder):
    def find_spec(self, name, *args):
        if name.startswith("spark_on_k8s.api") or (
            not enabled and name.startswith("spark_on_k8s.airflow.ui.backend")
        ):
            raise AssertionError("Unexpected optional backend import: " + name)
sys.meta_path.insert(0, BlockBackend())
from spark_on_k8s.airflow.ui.plugin import SparkTaskUIPlugin
assert bool(SparkTaskUIPlugin.react_apps) is enabled
assert bool(SparkTaskUIPlugin.fastapi_apps) is enabled
from spark_on_k8s.airflow.provider_info import get_provider_info
assert "plugins" not in get_provider_info()
"""
    env = dict(os.environ)
    if configured:
        env["SPARK_ON_K8S_AIRFLOW_SERVICE_URL"] = "https://spark.example"
    else:
        env.pop("SPARK_ON_K8S_AIRFLOW_SERVICE_URL", None)
    subprocess.run(
        [sys.executable, "-I", "-B", "-c", code], check=True, env=env, capture_output=True, text=True
    )
