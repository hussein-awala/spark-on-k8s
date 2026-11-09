from __future__ import annotations

import json
from unittest import mock

from click.testing import CliRunner

TEST_NAMESPACE = "test-namespace"


@mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager")
def test_list(mock_spark_app_manager):
    from spark_on_k8s.cli.apps import list

    mock_spark_app_manager().list_apps.return_value = ["app1", "app2", "app3"]
    args = ["--namespace", TEST_NAMESPACE]
    result = CliRunner().invoke(list, args)
    assert result.exit_code == 0
    assert result.output == "app1\napp2\napp3\n"
    mock_spark_app_manager().list_apps.assert_called_with(namespace=TEST_NAMESPACE)


def prune_item(app_id, phase="Failed", reason=None):
    return {
        "app_id": app_id,
        "driver_pod": f"{app_id}-driver",
        "driver_uid": f"{app_id}-uid",
        "phase": phase,
        "reason": reason,
        "created_at": "2026-11-07T12:00:00+00:00",
        "age_seconds": 90000,
    }


@mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager")
def test_prune_defaults_to_a_report_without_deleting(mock_spark_app_manager):
    from spark_on_k8s.cli.apps import prune

    manager = mock_spark_app_manager.return_value
    expired = prune_item("expired", reason="DeadlineExceeded")
    manager.find_prunable_drivers.return_value = {
        "items": [expired],
        "excluded": {"active": 2, "recent": 0, "deleting": 0, "unverified": 0},
    }
    result = CliRunner().invoke(prune, ["--namespace", TEST_NAMESPACE, "--older-than", "86400"])
    assert result.exit_code == 0, result.output
    manager.find_prunable_drivers.assert_called_once_with(
        namespace=TEST_NAMESPACE, older_than=86400, include_active=False
    )
    assert "Would delete expired-driver (application expired, Failed/DeadlineExceeded" in result.output
    assert "2 still active (see --include-active)" in result.output
    assert "nothing was deleted (use --delete)" in result.output

    result = CliRunner().invoke(prune, ["--namespace", TEST_NAMESPACE, "--output", "json"])
    assert result.exit_code == 0, result.output
    document = json.loads(result.output)
    assert document["operation"] == "prune"
    assert document["dry_run"] is True
    assert document["error"] is None
    assert document["criteria"] == {"older_than": 0, "include_active": False}
    assert document["items"] == [{**expired, "action": "would_delete", "error": None}]
    assert document["excluded"]["active"] == 2
    manager.delete_prunable_driver.assert_not_called()


@mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager")
def test_prune_delete_reports_each_outcome_and_partial_failure(mock_spark_app_manager):
    from kubernetes.client.exceptions import ApiException
    from spark_on_k8s.cli.apps import prune

    manager = mock_spark_app_manager.return_value
    items = [prune_item(name) for name in ("one", "gone", "swapped", "denied", "two")]
    manager.find_prunable_drivers.return_value = {"items": items, "excluded": {}}
    denied = ApiException(status=403, reason="Forbidden")
    denied.body = "private response body"
    manager.delete_prunable_driver.side_effect = [
        "deletion_requested",
        "absent",
        "replaced",
        denied,
        "deletion_requested",
    ]
    result = CliRunner().invoke(prune, ["-n", TEST_NAMESPACE, "--delete", "--output", "json"])
    assert result.exit_code == 1
    document = json.loads(result.stdout)
    assert document["dry_run"] is False
    assert [item["action"] for item in document["items"]] == [
        "deletion_requested",
        "absent",
        "replaced",
        "failed",
        "deletion_requested",
    ]
    assert document["items"][3]["error"] == {"code": "api_error", "message": "Kubernetes API 403: Forbidden"}
    assert document["error"]["code"] == "operation_error"
    assert "1 of 5" in document["error"]["message"]
    assert "private response body" not in result.output
    assert manager.delete_prunable_driver.call_count == 5
    manager.delete_prunable_driver.assert_any_call(namespace=TEST_NAMESPACE, item=items[0])


@mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager")
def test_prune_text_delete_separates_failures_from_requests(mock_spark_app_manager):
    from spark_on_k8s.cli.apps import prune

    manager = mock_spark_app_manager.return_value
    manager.find_prunable_drivers.return_value = {
        "items": [prune_item("one"), prune_item("two")],
        "excluded": {},
    }
    manager.delete_prunable_driver.side_effect = ["deletion_requested", RuntimeError("unavailable")]
    result = CliRunner().invoke(prune, ["-n", TEST_NAMESPACE, "--delete"])
    assert result.exit_code == 1
    assert "Deletion requested for one-driver" in result.stdout
    assert "Failed to delete two-driver" in result.stderr
    assert "Deletion failed for 1 of 2" in result.stderr


@mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager")
def test_prune_active_drivers_need_an_explicit_age(mock_spark_app_manager):
    from spark_on_k8s.cli.apps import prune

    result = CliRunner().invoke(prune, ["-n", TEST_NAMESPACE, "--include-active", "--delete"])
    assert result.exit_code == 2
    assert "requires a positive --older-than" in result.output
    mock_spark_app_manager.assert_not_called()


@mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager")
def test_prune_listing_failure_and_interruption_keep_the_json_contract(mock_spark_app_manager):
    from kubernetes.client.exceptions import ApiException
    from spark_on_k8s.cli.apps import prune

    manager = mock_spark_app_manager.return_value
    manager.find_prunable_drivers.side_effect = ApiException(status=403, reason="Forbidden")
    result = CliRunner().invoke(prune, ["-n", TEST_NAMESPACE, "--output", "json"])
    assert result.exit_code == 1
    document = json.loads(result.stdout)
    assert document["items"] is None
    assert document["error"]["code"] == "api_error"

    manager.find_prunable_drivers.side_effect = None
    manager.find_prunable_drivers.return_value = {
        "items": [prune_item("one"), prune_item("two")],
        "excluded": {},
    }
    manager.delete_prunable_driver.side_effect = ["deletion_requested", KeyboardInterrupt()]
    result = CliRunner().invoke(prune, ["-n", TEST_NAMESPACE, "--delete", "--output", "json"])
    assert result.exit_code == 130
    document = json.loads(result.stdout)
    assert document["error"]["code"] == "interrupted"
    assert "may already have been requested" in document["error"]["message"]
    assert [item["action"] for item in document["items"]] == ["deletion_requested", None]
