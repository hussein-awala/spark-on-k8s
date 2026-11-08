from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from spark_on_k8s.utils import app_manager, async_app_manager
from spark_on_k8s.utils.spark_app_status import SparkAppStatus


@pytest.mark.parametrize("already_running", [False, True])
def test_sync_waiter_propagates_api_errors(monkeypatch, already_running):
    error = app_manager.ApiException(status=403, reason="Forbidden")
    statuses = [error, SparkAppStatus.Succeeded]
    if already_running:
        statuses.insert(0, SparkAppStatus.Running)
    manager = app_manager.SparkAppManager(k8s_client_manager=MagicMock())
    status = Mock(side_effect=statuses)
    monkeypatch.setattr(manager, "app_status", status)
    sleep = Mock()
    monkeypatch.setattr(app_manager.time, "sleep", sleep)

    with pytest.raises(app_manager.ApiException) as raised:
        manager.wait_for_app(namespace="spark", pod_name="test-driver")

    assert raised.value is error
    assert status.call_count == (2 if already_running else 1)
    assert sleep.call_count == (1 if already_running else 0)


@pytest.mark.parametrize("already_running", [False, True])
def test_async_waiter_propagates_api_errors(monkeypatch, already_running):
    error = async_app_manager.ApiException(status=403, reason="Forbidden")
    statuses = [error, SparkAppStatus.Succeeded]
    if already_running:
        statuses.insert(0, SparkAppStatus.Running)
    manager = async_app_manager.AsyncSparkAppManager(k8s_client_manager=MagicMock())
    status = AsyncMock(side_effect=statuses)
    monkeypatch.setattr(manager, "app_status", status)
    sleep = AsyncMock()
    monkeypatch.setattr(async_app_manager.asyncio, "sleep", sleep)

    with pytest.raises(async_app_manager.ApiException) as raised:
        asyncio.run(manager.wait_for_app(namespace="spark", pod_name="test-driver"))

    assert raised.value is error
    assert status.call_count == (2 if already_running else 1)
    assert sleep.call_count == (1 if already_running else 0)


@pytest.mark.parametrize("asynchronous", [False, True])
def test_waiting_by_application_id_names_the_application_not_a_missing_pod(capsys, asynchronous):
    module = async_app_manager if asynchronous else app_manager
    manager_type = module.AsyncSparkAppManager if asynchronous else module.SparkAppManager
    manager = manager_type(k8s_client_manager=MagicMock())
    statuses = [SparkAppStatus.Running, SparkAppStatus.Succeeded]
    manager.app_status = AsyncMock(side_effect=statuses) if asynchronous else Mock(side_effect=statuses)
    call = manager.wait_for_app(namespace="spark", app_id="daily-job", poll_interval=0, should_print=True)
    if asynchronous:
        asyncio.run(call)
    output = capsys.readouterr().out
    assert output == (
        "App daily-job status is Running, sleep 0s\nApp daily-job finished with status Succeeded\n"
    )


def test_sync_waiter_still_returns_when_pod_is_deleted(monkeypatch):
    manager = app_manager.SparkAppManager(k8s_client_manager=MagicMock())
    status = Mock(side_effect=app_manager.ApiException(status=404))
    monkeypatch.setattr(manager, "app_status", status)

    manager.wait_for_app(namespace="spark", pod_name="test-driver")

    status.assert_called_once()


def test_async_waiter_still_returns_when_pod_is_deleted(monkeypatch):
    manager = async_app_manager.AsyncSparkAppManager(k8s_client_manager=MagicMock())
    status = AsyncMock(side_effect=async_app_manager.ApiException(status=404))
    monkeypatch.setattr(manager, "app_status", status)

    asyncio.run(manager.wait_for_app(namespace="spark", pod_name="test-driver"))

    status.assert_awaited_once()
