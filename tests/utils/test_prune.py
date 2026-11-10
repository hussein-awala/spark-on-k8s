from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from kubernetes import client as k8s
from kubernetes.client import ApiException
from spark_on_k8s.utils.app_manager import SparkAppManager


def driver(app_id, *, phase="Succeeded", age=7200, name=None, uid=None, reason=None, deleting=False):
    now = datetime.now(UTC)
    return k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            name=name or f"{app_id}-driver",
            namespace="spark",
            uid=uid if uid is not None else f"{app_id}-uid",
            labels={"spark-role": "driver", "spark-app-id": app_id},
            creation_timestamp=now - timedelta(seconds=age),
            deletion_timestamp=now if deleting else None,
        ),
        status=k8s.V1PodStatus(phase=phase, reason=reason),
    )


@pytest.fixture
def cluster(monkeypatch):
    api = MagicMock()
    monkeypatch.setattr(k8s, "CoreV1Api", lambda client: api)
    return SparkAppManager(k8s_client_manager=MagicMock()), api


def test_selects_only_old_terminal_drivers_created_by_this_client(cluster):
    manager, api = cluster
    long_id = "a" * 45 + "-20261107120000"
    api.list_namespaced_pod.return_value = k8s.V1PodList(
        items=[
            driver("done", reason=None),
            driver("expired", phase="Failed", reason="DeadlineExceeded"),
            driver("running", phase="Running"),
            driver("unknown", phase=None),
            driver("fresh", age=10),
            driver("leaving", deleting=True),
            # Hashed (current) and truncated (older releases) long driver names are both recognized.
            driver(long_id, name=SparkAppManager._resource_name(app_id=long_id, suffix="-driver")),
            driver(long_id + "x", name=f"{(long_id + 'x')[:56]}-driver"),
            # Labels alone are not ownership: a foreign name, invalid ID or missing UID is skipped.
            driver("foreign", name="someone-elses-pod"),
            driver("Not_A_Label"),
            driver("no-uid", uid=""),
        ]
    )
    selection = manager.find_prunable_drivers(namespace="spark", older_than=3600)
    api.list_namespaced_pod.assert_called_once_with(
        namespace="spark", label_selector="spark-role=driver,spark-app-id"
    )
    selected = {item["app_id"]: item for item in selection["items"]}
    assert set(selected) == {"done", "expired", long_id, long_id + "x"}
    assert selected["expired"]["reason"] == "DeadlineExceeded"
    assert selected["done"]["driver_uid"] == "done-uid"
    assert selected["done"]["age_seconds"] >= 7200
    assert selection["excluded"] == {"active": 2, "recent": 1, "deleting": 1, "unverified": 3}
    api.delete_namespaced_pod.assert_not_called()


def test_active_drivers_require_an_explicit_opt_in_and_age(cluster):
    manager, api = cluster
    api.list_namespaced_pod.return_value = k8s.V1PodList(
        items=[driver("old", phase="Running"), driver("pending", phase="Pending", age=30)]
    )
    with pytest.raises(ValueError, match="positive older_than"):
        manager.find_prunable_drivers(namespace="spark", include_active=True)
    api.list_namespaced_pod.assert_not_called()
    selection = manager.find_prunable_drivers(namespace="spark", older_than=3600, include_active=True)
    assert [item["app_id"] for item in selection["items"]] == ["old"]
    assert selection["excluded"]["recent"] == 1


@pytest.mark.parametrize("value", [-1, True, float("nan"), "60"])
def test_invalid_age_is_rejected_without_cluster_access(cluster, value):
    manager, api = cluster
    with pytest.raises(ValueError, match="older_than"):
        manager.find_prunable_drivers(namespace="spark", older_than=value)
    api.list_namespaced_pod.assert_not_called()


def test_deletion_is_uid_guarded_and_reports_absent_or_replaced_drivers(cluster):
    manager, api = cluster
    item = {"app_id": "done", "driver_pod": "done-driver", "driver_uid": "done-uid"}
    api.read_namespaced_pod.return_value = driver("done")
    assert manager.delete_prunable_driver(namespace="spark", item=item) == "deletion_requested"
    options = api.delete_namespaced_pod.call_args.kwargs["body"]
    assert options.preconditions.uid == "done-uid"
    assert options.propagation_policy == "Foreground"
    # A same-name replacement is never deleted.
    api.delete_namespaced_pod.reset_mock()
    api.read_namespaced_pod.return_value = driver("done", uid="new-uid")
    assert manager.delete_prunable_driver(namespace="spark", item=item) == "replaced"
    api.delete_namespaced_pod.assert_not_called()
    api.read_namespaced_pod.side_effect = ApiException(status=404)
    assert manager.delete_prunable_driver(namespace="spark", item=item) == "absent"
    api.read_namespaced_pod.side_effect = None
    api.read_namespaced_pod.return_value = driver("done")
    api.delete_namespaced_pod.side_effect = ApiException(status=409)
    assert manager.delete_prunable_driver(namespace="spark", item=item) == "replaced"
    api.delete_namespaced_pod.side_effect = ApiException(status=403)
    with pytest.raises(ApiException):
        manager.delete_prunable_driver(namespace="spark", item=item)
