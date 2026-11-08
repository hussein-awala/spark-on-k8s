from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from spark_on_k8s.k8s.sync_client import KubernetesClientManager
from spark_on_k8s_dbt.api import BoundedKubernetesClientManager


class FakeApiClient:
    def __init__(self):
        self.rest_client = SimpleNamespace(pool_manager=SimpleNamespace(connection_pool_kw={"retries": 3}))
        self.calls = []
        self.close = Mock()
        self.stall = False

    def call_api(
        self,
        resource_path,
        method,
        path_params=None,
        query_params=None,
        header_params=None,
        body=None,
        post_params=None,
        files=None,
        response_types_map=None,
        auth_settings=None,
        async_req=None,
        _return_http_data_only=None,
        collection_formats=None,
        _preload_content=True,
        _request_timeout=None,
        _host=None,
        _request_auth=None,
    ):
        self.calls.append(
            {"path": resource_path, "method": method, "timeout": _request_timeout, "body": body}
        )
        if self.stall:
            time.sleep(_request_timeout)
            raise TimeoutError("simulated stalled transport")
        return _request_timeout


@pytest.fixture
def raw_client(monkeypatch):
    raw = FakeApiClient()
    monkeypatch.setattr(KubernetesClientManager, "create_client", lambda self: raw)
    return raw


def manager(**kwargs):
    return BoundedKubernetesClientManager(
        deadline=kwargs.pop("deadline", lambda: None),
        config_file="/explicit/kubeconfig",
        context="explicit-context",
        in_cluster=False,
        client_configuration=None,
        **kwargs,
    )


def test_default_budget_and_retry_changes_are_per_client(raw_client):
    unrelated = FakeApiClient()
    client = manager().create_client()
    assert client.call_api("/api/v1/pods", "POST", body={"name": "driver"}) == 15
    assert raw_client.calls == [
        {"path": "/api/v1/pods", "method": "POST", "timeout": 15, "body": {"name": "driver"}}
    ]
    assert raw_client.rest_client.pool_manager.connection_pool_kw["retries"] == 0
    assert unrelated.rest_client.pool_manager.connection_pool_kw["retries"] == 3
    assert unrelated.call_api("/api", "GET") is None


@pytest.mark.parametrize(
    "requested,expected", [(None, 15), (2, 2), (20, 15), ((2, 10), 2), ((None, 3), 3), ((None, None), 15)]
)
def test_existing_shorter_scalar_and_pair_budgets_are_preserved(raw_client, requested, expected):
    client = manager().create_client()
    assert client.call_api("/api", "GET", _request_timeout=requested) == expected


def test_positional_timeout_is_bounded_without_duplicate_keyword(raw_client):
    client = manager().create_client()
    assert client.call_api(*(["/api", "GET"] + [None] * 12 + [2])) == 2
    assert len(raw_client.calls) == 1


def test_deadline_is_evaluated_for_every_request_and_can_change_to_cleanup(raw_client, monkeypatch):
    monkeypatch.setattr("spark_on_k8s_dbt.api.time.monotonic", lambda: 100.0)
    deadline = [103.0]
    client = manager(deadline=lambda: deadline[0]).create_client()
    assert client.call_api("/create", "POST") == 3
    deadline[0] = 112.0
    assert client.call_api("/delete", "DELETE") == 12
    deadline[0] = 99.0
    with pytest.raises(TimeoutError, match="deadline expired"):
        client.call_api("/late", "POST")
    assert [call["path"] for call in raw_client.calls] == ["/create", "/delete"]


def test_simulated_stalled_submission_obeys_remaining_budget(raw_client):
    raw_client.stall = True
    client = manager(deadline=lambda: deadline, request_timeout=1).create_client()
    started = time.monotonic()
    deadline = started + 0.02
    with pytest.raises(TimeoutError, match="simulated stalled"):
        client.call_api("/driver", "POST")
    elapsed = time.monotonic() - started
    assert 0 < raw_client.calls[0]["timeout"] <= 0.02
    assert elapsed < 0.5


@pytest.mark.parametrize("requested", [0, -1, True, float("inf"), float("nan"), (1,), (1, 2, 3), "15"])
def test_invalid_caller_timeout_cannot_disable_the_bound(raw_client, requested):
    client = manager().create_client()
    with pytest.raises(ValueError, match="positive and finite"):
        client.call_api("/driver", "POST", _request_timeout=requested)
    assert not raw_client.calls


def test_client_still_closes_after_a_bounded_request_failure(raw_client):
    bounded = manager(deadline=lambda: time.monotonic() - 1)
    with pytest.raises(TimeoutError), bounded.client() as client:
        client.call_api("/driver", "POST")
    raw_client.close.assert_called_once()
