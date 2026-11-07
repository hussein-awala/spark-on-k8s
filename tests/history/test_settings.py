from __future__ import annotations

import pytest
from spark_on_k8s.history.settings import HistorySettings, boolean, integer, namespaces


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    import os

    for name in os.environ:
        if name.startswith("SPARK_ON_K8S_RUN_HISTORY") or name == "SPARK_ON_K8S_METRICS_ENABLED":
            monkeypatch.delenv(name)


def test_history_and_metrics_are_opt_in():
    settings = HistorySettings.from_env()
    assert not settings.enabled and not settings.metrics_enabled


def test_configured_store_limits_and_explicit_namespaces(tmp_path, monkeypatch):
    monkeypatch.setenv("SPARK_ON_K8S_RUN_HISTORY_ENABLED", "true")
    monkeypatch.setenv("SPARK_ON_K8S_RUN_HISTORY_PATH", str(tmp_path / "runs.sqlite3"))
    monkeypatch.setenv("SPARK_ON_K8S_RUN_HISTORY_NAMESPACES", "analytics, other,analytics")
    monkeypatch.setenv("SPARK_ON_K8S_METRICS_ENABLED", "1")
    settings = HistorySettings.from_env()
    assert settings.enabled and settings.metrics_enabled
    assert settings.namespaces == ("analytics", "other")
    assert settings.lease_seconds > settings.collection_timeout + settings.interval
    assert settings.freshness_seconds > settings.lease_seconds


@pytest.mark.parametrize("path", ["", "relative.sqlite3", ":memory:"])
def test_history_requires_an_explicit_absolute_file(monkeypatch, path):
    monkeypatch.setenv("SPARK_ON_K8S_RUN_HISTORY_ENABLED", "1")
    monkeypatch.setenv("SPARK_ON_K8S_RUN_HISTORY_PATH", path)
    with pytest.raises(ValueError, match="absolute"):
        HistorySettings.from_env()


@pytest.mark.parametrize("value", ["*", "", "a,", "Invalid", ",".join(f"n{i}" for i in range(21))])
def test_unbounded_or_invalid_namespaces_are_rejected(value):
    with pytest.raises(ValueError, match="namespaces"):
        namespaces(value)


def test_invalid_boolean_and_integer_configuration_fails_closed(monkeypatch):
    monkeypatch.setenv("OPTION", "maybe")
    with pytest.raises(ValueError, match="true or false"):
        boolean("OPTION")
    with pytest.raises(ValueError, match="integer"):
        integer("OPTION", 10, 1, 20)
    monkeypatch.setenv("OPTION", "21")
    with pytest.raises(ValueError, match="between"):
        integer("OPTION", 10, 1, 20)
