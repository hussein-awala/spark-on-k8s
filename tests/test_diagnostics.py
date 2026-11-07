from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from click.testing import CliRunner
from kubernetes import client as k8s
from kubernetes_asyncio.client import ApiException as AsyncApiException
from spark_on_k8s.client import SparkOnK8S
from spark_on_k8s.diagnostics import classify_pod, collect_diagnosis, collect_diagnosis_async
from spark_on_k8s.doctor import CALLER_CHECKS, check_cluster, driver_rbac


def pod(name="driver", role="driver", uid="driver-uid", owner=None):
    return k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            name=name,
            namespace="spark",
            uid=uid,
            labels={"spark-role": role, "spark-app-id": "report", "spark-app-selector": "report"},
            owner_references=[k8s.V1OwnerReference(api_version="v1", kind="Pod", name="driver", uid=owner)]
            if owner
            else None,
        ),
        spec=k8s.V1PodSpec(containers=[k8s.V1Container(name=role, image="spark:test")]),
        status=k8s.V1PodStatus(phase="Pending"),
    )


def event(reason, message, uid="driver-uid"):
    return k8s.CoreV1Event(
        metadata=k8s.V1ObjectMeta(name="event"),
        reason=reason,
        message=message,
        involved_object=k8s.V1ObjectReference(kind="Pod", name="driver", uid=uid),
    )


@pytest.mark.parametrize(
    "reason, code",
    [
        ("ImagePullBackOff", "image_pull_failed"),
        ("ErrImagePull", "image_pull_failed"),
        ("InvalidImageName", "image_pull_failed"),
        ("CrashLoopBackOff", "crash_loop"),
    ],
)
def test_waiting_container_has_evidence_and_remediation(reason, code):
    value = pod().to_dict()
    value["status"]["container_statuses"] = [
        {"name": "driver", "state": {"waiting": {"reason": reason, "message": "observed reason"}}}
    ]
    findings = classify_pod(value, [])
    assert len(findings) == 1
    assert findings[0].code == code
    assert findings[0].container == "driver"
    assert findings[0].evidence == "observed reason"
    assert findings[0].remediation


@pytest.mark.parametrize("previous", [False, True])
def test_oom_kill_is_observed_even_after_recovery(previous):
    value = pod().to_dict()
    value["status"]["phase"] = "Running" if previous else "Failed"
    value["status"]["container_statuses"] = [
        {
            "name": "executor",
            "last_state" if previous else "state": {"terminated": {"reason": "OOMKilled", "exit_code": 137}},
        }
    ]
    finding = classify_pod(value, [])[0]
    assert finding.code == "oom_killed"
    assert finding.severity == ("warning" if previous else "error")
    assert "Previous" in finding.evidence if previous else "Current" in finding.evidence


@pytest.mark.parametrize(
    "reason,message,codes",
    [
        (
            "FailedScheduling",
            "0/2 nodes: Insufficient cpu; untolerated taint dedicated=batch",
            {"unschedulable", "insufficient_resources", "taint_mismatch"},
        ),
        ("FailedScheduling", 'persistentvolumeclaim "data" not found', {"unschedulable", "pvc_missing"}),
        ("FailedMount", 'persistentvolumeclaim "data" is unbound', {"pvc_missing"}),
        (
            "FailedCreate",
            'pods is forbidden: User "spark" cannot create resource "pods"',
            {"permission_denied"},
        ),
        ("Failed", 'Failed to pull image "registry/image"', {"image_pull_failed"}),
    ],
)
def test_event_diagnoses_use_observed_messages(reason, message, codes):
    assert {f.code for f in classify_pod(pod().to_dict(), [event(reason, message).to_dict()])} == codes


def test_exit_137_alone_is_not_claimed_as_oom_and_no_events_does_not_invent_a_cause():
    value = pod().to_dict()
    value["status"]["container_statuses"] = [{"name": "driver", "state": {"terminated": {"exit_code": 137}}}]
    assert [f.code for f in classify_pod(value, [])] == ["container_failed"]
    assert [f.code for f in classify_pod(pod().to_dict(), [])] == ["cause_unknown"]


def test_evidence_redacts_credentials_and_deduplicates_repeated_events():
    events = [
        event("Failed", "Failed to pull image https://user:private@registry/image?token=private").to_dict()
    ] * 3
    findings = classify_pod(pod().to_dict(), events)
    assert len(findings) == 1
    assert "private" not in findings[0].evidence


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def collection(request):
    asynchronous = request.param
    api = Mock()
    factory = AsyncMock if asynchronous else Mock
    api.list_namespaced_pod = factory(
        return_value=k8s.V1PodList(
            items=[
                pod("executor-1", "executor", "exec-uid", "driver-uid"),
                pod("foreign", "executor", "foreign-uid", "another-driver"),
            ],
            metadata=k8s.V1ListMeta(_continue="more"),
        )
    )
    api.list_namespaced_event = factory(
        side_effect=[
            k8s.CoreV1EventList(
                items=[
                    event("FailedScheduling", "Insufficient cpu"),
                    event("Failed", "FOREIGN EVENT", "old-pod"),
                ],
                metadata=k8s.V1ListMeta(),
            ),
            k8s.CoreV1EventList(items=[], metadata=k8s.V1ListMeta()),
        ]
    )

    def collect(**kwargs):
        result = (collect_diagnosis_async if asynchronous else collect_diagnosis)(
            api, pod(), namespace="spark", **kwargs
        )
        return asyncio.run(result) if asynchronous else result

    return api, collect, AsyncApiException if asynchronous else k8s.ApiException


def test_collection_is_namespaced_bounded_and_excludes_foreign_ownership(collection):
    api, collect, _ = collection
    report = collect(pod_limit=2, event_limit=3)
    assert report.inspected_pods == 2
    assert report.inspected_events == 1
    assert report.incomplete is True
    assert "FOREIGN EVENT" not in json.dumps(report.to_dict())
    kwargs = api.list_namespaced_pod.call_args.kwargs
    assert kwargs["namespace"] == "spark"
    assert kwargs["label_selector"] == "spark-role=executor,spark-app-selector=report"
    assert kwargs["limit"] == 2
    assert 0 < kwargs["_request_timeout"] <= 10
    assert api.list_namespaced_event.call_count == 2
    assert [call.kwargs["field_selector"] for call in api.list_namespaced_event.call_args_list] == [
        "involvedObject.uid=driver-uid",
        "involvedObject.uid=exec-uid",
    ]
    assert all(call.kwargs["limit"] == 3 for call in api.list_namespaced_event.call_args_list)


def test_denied_executor_or_event_access_returns_partial_pod_evidence(collection):
    api, collect, error_type = collection
    api.list_namespaced_pod.side_effect = error_type(status=403)
    api.list_namespaced_event.side_effect = error_type(status=403)
    report = collect()
    assert report.incomplete is True
    assert report.inspected_pods == 1
    assert [f.code for f in report.findings].count("collection_incomplete") == 2
    assert any(f.code == "cause_unknown" for f in report.findings)


def test_sdk_rejects_unsafe_selector_before_connecting():
    client_manager = MagicMock()
    spark = SparkOnK8S(k8s_client_manager=client_manager)
    with pytest.raises(ValueError):
        spark.diagnose_app(namespace="spark", app_id="anything,spark-role=executor")
    client_manager.client.assert_not_called()


def test_driver_rbac_is_namespace_scoped_and_contains_no_secret_or_cluster_grants():
    role, binding = driver_rbac("analytics", "spark")
    assert role["kind"] == "Role"
    assert binding["roleRef"]["kind"] == "Role"
    assert role["metadata"]["namespace"] == binding["metadata"]["namespace"] == "analytics"
    assert binding["subjects"] == [{"kind": "ServiceAccount", "name": "spark", "namespace": "analytics"}]
    assert {resource for rule in role["rules"] for resource in rule["resources"]} == {
        "pods",
        "services",
        "configmaps",
    }
    assert "*" not in json.dumps([role, binding])


def test_doctor_only_reads_account_and_reviews_caller_permissions(monkeypatch):
    api, auth = Mock(), Mock()
    api.read_namespaced_service_account.return_value = k8s.V1ServiceAccount(
        metadata=k8s.V1ObjectMeta(name="spark")
    )
    auth.create_self_subject_access_review.return_value = k8s.V1SelfSubjectAccessReview(
        spec=k8s.V1SelfSubjectAccessReviewSpec(), status=k8s.V1SubjectAccessReviewStatus(allowed=False)
    )
    monkeypatch.setattr(k8s, "CoreV1Api", Mock(return_value=api))
    monkeypatch.setattr(k8s, "AuthorizationV1Api", Mock(return_value=auth))
    report = check_cluster(Mock(), namespace="analytics", service_account="spark")
    assert report.driver_permissions_verified is False
    assert len(report.caller_permissions) == len(CALLER_CHECKS)
    assert all(check["allowed"] is False for check in report.caller_permissions)
    assert [call[0] for call in api.mock_calls] == ["read_namespaced_service_account"]
    assert all(call[0] == "create_self_subject_access_review" for call in auth.mock_calls)
    for call in auth.mock_calls:
        assert call.kwargs["body"].spec.resource_attributes.namespace == "analytics"
        assert 0 < call.kwargs["_request_timeout"] <= 10


def test_doctor_manifest_generation_is_offline(monkeypatch):
    from spark_on_k8s.cli.diagnostics import doctor

    connect = Mock(side_effect=AssertionError("Must stay offline"))
    monkeypatch.setattr(SparkOnK8S, "doctor", connect)
    result = CliRunner().invoke(
        doctor, ["--namespace", "analytics", "--service-account", "spark", "--emit-rbac"]
    )
    assert result.exit_code == 0, result.output
    assert "kind: RoleBinding" in result.output
    assert "ClusterRole" not in result.output
    connect.assert_not_called()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"pod_limit": 0},
        {"pod_limit": True},
        {"event_limit": 201},
        {"event_limit": 0},
        {"timeout": 0},
        {"timeout": None},
        {"timeout": 31},
        {"timeout": float("nan")},
    ],
)
def test_invalid_collection_bounds_never_contact_kubernetes(kwargs):
    api = Mock()
    with pytest.raises(ValueError):
        collect_diagnosis(api, pod(), namespace="spark", **kwargs)
    assert not api.mock_calls


@pytest.mark.parametrize("labels", [{}, {"spark-role": "executor"}, {"spark-role": "driver"}])
def test_missing_or_non_driver_identity_does_not_scan_namespace(labels):
    api, driver = Mock(), pod()
    driver.metadata.labels = labels
    with pytest.raises(ValueError):
        collect_diagnosis(api, driver, namespace="spark")
    assert not api.mock_calls


@pytest.mark.parametrize("asynchronous", [False, True])
def test_missing_uid_never_queries_events_by_name(asynchronous):
    api = Mock()
    factory = AsyncMock if asynchronous else Mock
    api.list_namespaced_pod = factory(return_value=k8s.V1PodList(items=[]))
    api.list_namespaced_event = factory()
    driver = pod(uid=None)
    result = (collect_diagnosis_async if asynchronous else collect_diagnosis)(api, driver, namespace="spark")
    report = asyncio.run(result) if asynchronous else result
    assert report.incomplete and report.inspected_pods == 1
    api.list_namespaced_event.assert_not_called()


def test_event_page_continuation_marks_snapshot_incomplete(collection):
    api, collect, _ = collection
    api.list_namespaced_pod.return_value = k8s.V1PodList(items=[])
    api.list_namespaced_event.side_effect = None
    api.list_namespaced_event.return_value = k8s.CoreV1EventList(
        items=[event("Forbidden", "")], metadata=k8s.V1ListMeta(_continue="next")
    )
    report = collect()
    assert report.incomplete and report.inspected_events == 1
    assert {f.code for f in report.findings} == {"collection_incomplete", "permission_denied"}


def test_timeout_returns_available_pod_evidence(collection):
    api, collect, _ = collection
    api.list_namespaced_pod.side_effect = TimeoutError("private details")
    api.list_namespaced_event.side_effect = TimeoutError("private details")
    report = collect()
    assert report.incomplete and report.inspected_pods == 1
    assert "private details" not in json.dumps(report.to_dict())


def test_sync_http_timeout_is_a_partial_report():
    from urllib3.exceptions import ReadTimeoutError

    api = Mock()
    api.list_namespaced_pod.side_effect = ReadTimeoutError(None, "/pods", "timed out")
    api.list_namespaced_event.side_effect = ReadTimeoutError(None, "/events", "timed out")
    assert collect_diagnosis(api, pod(), namespace="spark").incomplete


@pytest.mark.parametrize("by_pod", [False, True])
def test_sdk_lookup_preserves_identity_and_closes_client(monkeypatch, by_pod):
    manager, api = MagicMock(), Mock()
    driver = pod(name="report.driver")
    driver.metadata.labels.pop("spark-app-selector")
    api.read_namespaced_pod.return_value = driver
    api.list_namespaced_pod.side_effect = (
        [k8s.V1PodList(items=[])] if by_pod else [k8s.V1PodList(items=[driver]), k8s.V1PodList(items=[])]
    )
    api.list_namespaced_event.return_value = k8s.CoreV1EventList(items=[])
    monkeypatch.setattr(k8s, "CoreV1Api", Mock(return_value=api))
    report = SparkOnK8S(k8s_client_manager=manager).diagnose_app(
        namespace="spark", **({"pod_name": "report.driver"} if by_pod else {"app_id": "report"})
    )
    assert report.driver_pod == "report.driver" and report.app_id == "report"
    manager.client.return_value.__exit__.assert_called_once()
    assert api.list_namespaced_pod.call_args.kwargs["label_selector"] == (
        "spark-role=executor,spark-app-selector=report"
    )
    if not by_pod:
        assert api.list_namespaced_pod.call_args_list[0].kwargs["limit"] == 2
        assert api.list_namespaced_pod.call_args_list[0].kwargs["label_selector"] == (
            "spark-role=driver,spark-app-id=report"
        )


@pytest.mark.parametrize("count", [0, 2])
def test_sdk_ambiguous_or_missing_driver_does_not_collect_events(monkeypatch, count):
    api = Mock()
    api.list_namespaced_pod.return_value = k8s.V1PodList(items=[pod()] * count)
    monkeypatch.setattr(k8s, "CoreV1Api", Mock(return_value=api))
    with pytest.raises(ValueError, match="Expected one"):
        SparkOnK8S(k8s_client_manager=MagicMock()).diagnose_app(namespace="spark", app_id="report")
    api.list_namespaced_event.assert_not_called()


@pytest.mark.parametrize("kwargs", [{}, {"app_id": "report", "pod_name": "driver"}])
def test_sdk_requires_one_driver_identifier(kwargs):
    manager = MagicMock()
    with pytest.raises(ValueError, match="exactly one"):
        SparkOnK8S(k8s_client_manager=manager).diagnose_app(namespace="spark", **kwargs)
    manager.client.assert_not_called()


@pytest.mark.parametrize("missing", [False, True])
def test_doctor_distinguishes_unavailable_reviews_and_disabled_credentials(monkeypatch, missing):
    api, auth = Mock(), Mock()
    if missing:
        api.read_namespaced_service_account.side_effect = k8s.ApiException(status=404)
        auth.create_self_subject_access_review.side_effect = k8s.ApiException(status=403)
    else:
        api.read_namespaced_service_account.return_value = k8s.V1ServiceAccount(
            metadata=k8s.V1ObjectMeta(name="spark"), automount_service_account_token=False
        )
        auth.create_self_subject_access_review.return_value = k8s.V1SelfSubjectAccessReview(
            spec=k8s.V1SelfSubjectAccessReviewSpec(), status=k8s.V1SubjectAccessReviewStatus(allowed=True)
        )
    monkeypatch.setattr(k8s, "CoreV1Api", Mock(return_value=api))
    monkeypatch.setattr(k8s, "AuthorizationV1Api", Mock(return_value=auth))
    report = SparkOnK8S(k8s_client_manager=MagicMock()).doctor(namespace="spark")
    assert report.to_dict()["driver_permissions_verified"] is False
    codes = {f.code for f in report.findings}
    assert codes == (
        {"service_account_unavailable", "caller_permission_unknown"}
        if missing
        else {"service_account_found", "service_account_token_disabled"}
    )
    assert all(p["allowed"] is (None if missing else True) for p in report.caller_permissions)


@pytest.mark.parametrize("timeout", [0, None, 31])
def test_doctor_rejects_unbounded_timeouts(timeout):
    with pytest.raises(ValueError, match="timeout"):
        check_cluster(Mock(), namespace="spark", service_account="spark", timeout=timeout)


def test_long_service_account_role_names_remain_valid_and_distinct():
    first = driver_rbac("spark", "a" * 253)
    second = driver_rbac("spark", "a" * 252 + "b")
    assert len(first[0]["metadata"]["name"]) <= 253
    assert first[0]["metadata"]["name"] != second[0]["metadata"]["name"]
    assert first[1]["roleRef"]["name"] == first[0]["metadata"]["name"]
    assert first[1]["subjects"][0]["name"] == "a" * 253


@pytest.mark.parametrize("command_name", ["doctor", "diagnose"])
@pytest.mark.parametrize("output", ["text", "json"])
def test_diagnostic_cli_preserves_structured_report_and_text_guidance(monkeypatch, command_name, output):
    from spark_on_k8s.cli.app import diagnose
    from spark_on_k8s.cli.diagnostics import doctor
    from spark_on_k8s.diagnostics import Diagnosis
    from spark_on_k8s.doctor import DoctorReport

    if command_name == "doctor":
        report = DoctorReport("spark", "spark")
        method, command, extra = "doctor", doctor, []
    else:
        report = Diagnosis("spark", "report", "driver", findings=classify_pod(pod().to_dict(), []))
        method, command, extra = "diagnose_app", diagnose, ["--app-id", "report"]
    call = Mock(return_value=report)
    monkeypatch.setattr(SparkOnK8S, method, call)
    result = CliRunner().invoke(command, ["--namespace", "spark", "--output", output, *extra])
    assert result.exit_code == 0, result.output
    if output == "json":
        assert json.loads(result.output) == report.to_dict()
    elif command_name == "doctor":
        assert "permissions are not verified" in result.output
        assert "No specific problems" in result.output
    else:
        assert "Evidence:" in result.output and "Next:" in result.output
    assert call.call_args.kwargs["namespace"] == "spark"


@pytest.mark.parametrize("command_name", ["doctor", "diagnose"])
def test_diagnostic_cli_errors_are_nonzero_and_do_not_echo_credentials(monkeypatch, command_name):
    from spark_on_k8s.cli.app import diagnose
    from spark_on_k8s.cli.diagnostics import doctor

    method = "doctor" if command_name == "doctor" else "diagnose_app"
    monkeypatch.setattr(SparkOnK8S, method, Mock(side_effect=RuntimeError("private bearer token")))
    result = CliRunner().invoke(
        doctor if command_name == "doctor" else diagnose,
        ["--namespace", "spark", *([] if command_name == "doctor" else ["--app-id", "report"])],
    )
    assert result.exit_code != 0 and "Unable to" in result.output
    assert "private bearer token" not in result.output


@pytest.mark.parametrize(
    "message,remediation", [("bounded", "Increase pod_limit"), ("ownership", "Inspect owner references")]
)
def test_incomplete_preserves_cause_specific_next_step(message, remediation):
    from datetime import datetime

    from spark_on_k8s.diagnostics import Diagnosis, _incomplete

    report = Diagnosis("spark", "job", "driver")
    _incomplete(report, "driver", message, remediation=remediation)
    assert report.incomplete
    assert report.findings[0].remediation == remediation
    assert datetime.fromisoformat(report.collected_at).tzinfo is not None


@pytest.mark.parametrize(
    "status,expected", [(403, "permissions"), (404, "cannot be reconstructed"), (503, "transient")]
)
def test_read_failure_guidance_distinguishes_recovery_actions(status, expected):
    from kubernetes.client import ApiException
    from spark_on_k8s.diagnostics import _read_remediation

    assert expected in _read_remediation(ApiException(status=status))
