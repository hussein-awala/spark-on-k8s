from __future__ import annotations

import asyncio
from unittest import mock

import pytest
from spark_on_k8s.utils.async_app_manager import AsyncSparkAppManager
from spark_on_k8s.utils.spark_app_status import SparkAppStatus


@pytest.mark.parametrize(
    "deadline,statuses,expected_timeout",
    [
        (99, [SparkAppStatus.Pending], True),
        (None, [SparkAppStatus.Pending, SparkAppStatus.Running, SparkAppStatus.Succeeded], False),
        (
            105,
            [SparkAppStatus.Pending, *([SparkAppStatus.Running] * 3), SparkAppStatus.Succeeded],
            False,
        ),
    ],
    ids=["expired-pending", "disabled", "running-past-deadline"],
)
def test_startup_deadline_only_limits_pending(deadline, statuses, expected_timeout):
    client_manager = mock.Mock()
    client_manager.client.return_value = mock.AsyncMock()
    manager = AsyncSparkAppManager(k8s_client_manager=client_manager)
    manager.app_status = mock.AsyncMock(side_effect=statuses)
    now = [100]

    async def sleep(delay):
        now[0] += delay

    with (
        mock.patch("time.time", side_effect=lambda: now[0]),
        mock.patch("spark_on_k8s.utils.async_app_manager.asyncio.sleep", side_effect=sleep),
    ):
        call = manager.wait_for_app(
            namespace="spark", pod_name="driver", poll_interval=2, startup_deadline=deadline
        )
        if expected_timeout:
            with pytest.raises(TimeoutError, match="App startup timeout"):
                asyncio.run(call)
        else:
            asyncio.run(call)
    client_manager.client.return_value.__aexit__.assert_awaited_once()
    if deadline == 105:
        assert now[0] > deadline


def test_pending_poll_does_not_sleep_past_deadline():
    client_manager = mock.Mock()
    client_manager.client.return_value = mock.AsyncMock()
    manager = AsyncSparkAppManager(k8s_client_manager=client_manager)
    manager.app_status = mock.AsyncMock(return_value=SparkAppStatus.Pending)
    now = [100]

    async def sleep(delay):
        now[0] += delay

    with (
        mock.patch("time.time", side_effect=lambda: now[0]),
        mock.patch("spark_on_k8s.utils.async_app_manager.asyncio.sleep", side_effect=sleep) as sleeper,
    ):
        with pytest.raises(TimeoutError, match="App startup timeout"):
            asyncio.run(
                manager.wait_for_app(
                    namespace="spark", pod_name="driver", poll_interval=30, startup_deadline=105
                )
            )
    sleeper.assert_awaited_once_with(5)
