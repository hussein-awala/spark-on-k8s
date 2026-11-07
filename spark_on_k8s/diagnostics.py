from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

from spark_on_k8s.utils.run import WaitDeadline

_NAME = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")
_POD_NAME = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)*$")
_LABEL = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9_.-]*[A-Za-z0-9])?$")


@dataclass(frozen=True)
class DiagnosticFinding:
    code: str
    severity: Literal["info", "warning", "error"]
    resource: str
    summary: str
    evidence: str
    remediation: str
    container: str | None = None


@dataclass
class Diagnosis:
    namespace: str
    app_id: str
    driver_pod: str
    findings: list[DiagnosticFinding] = field(default_factory=list)
    inspected_pods: int = 0
    inspected_events: int = 0
    incomplete: bool = False
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_target(namespace: str, value: str, *, label: bool = False) -> None:
    if not isinstance(namespace, str) or len(namespace) > 63 or not _NAME.fullmatch(namespace):
        raise ValueError("Namespace must be a Kubernetes DNS label of at most 63 characters")
    pattern = _LABEL if label else _POD_NAME
    if not isinstance(value, str) or len(value) > (63 if label else 253) or not pattern.fullmatch(value):
        raise ValueError("Invalid Kubernetes application or pod identifier")


def _evidence(value: Any) -> str:
    # Kubernetes events can echo image URLs or error text. Never display embedded credentials.
    text = str(value or "")[:1000]
    text = re.sub(r"(://)[^/\s@]+@", r"\1<redacted>@", text)
    return re.sub(
        r"(?i)((?:password|token|secret|credential|access.key)\s*[=:]\s*)[^\s&;]+", r"\1<redacted>", text
    )


def classify_pod(pod: dict, events: list[dict]) -> list[DiagnosticFinding]:
    """Classify observed pod/container states and events; never inspect logs or secrets."""
    name = pod["metadata"]["name"]
    status = pod.get("status") or {}
    findings: dict[tuple[str, str | None], DiagnosticFinding] = {}

    def add(code, summary, evidence, remediation, container=None, severity="error"):
        findings.setdefault(
            (code, container),
            DiagnosticFinding(
                code,
                severity,
                name,
                summary,
                _evidence(evidence),
                remediation,
                container,
            ),
        )

    for container in (status.get("init_container_statuses") or []) + (status.get("container_statuses") or []):
        container_name = container.get("name")
        current = container.get("state") or {}
        waiting = current.get("waiting") or {}
        reason = waiting.get("reason")
        if reason in {"ImagePullBackOff", "ErrImagePull", "InvalidImageName"}:
            add(
                "image_pull_failed",
                "Container image is unavailable",
                waiting.get("message") or reason,
                "Check the image reference, registry reachability and imagePullSecrets in this namespace.",
                container_name,
            )
        if reason == "CrashLoopBackOff":
            add(
                "crash_loop",
                "Container repeatedly exits",
                waiting.get("message") or reason,
                "Inspect current and previous container logs and its exit code before changing "
                "restart or resource settings.",
                container_name,
            )
        for previous, state in ((False, current), (True, container.get("last_state") or {})):
            terminated = state.get("terminated") or {}
            if terminated.get("reason") == "OOMKilled":
                add(
                    "oom_killed",
                    "A container attempt was killed for exceeding memory",
                    f"{'Previous' if previous else 'Current'} attempt: OOMKilled; "
                    f"exit code {terminated.get('exit_code')}",
                    "Compare memory usage with the container limit and Spark memory/overhead "
                    "settings; investigate before increasing limits.",
                    container_name,
                    "warning" if previous else "error",
                )
            elif not previous and terminated.get("exit_code") not in (None, 0):
                add(
                    "container_failed",
                    "Container exited unsuccessfully",
                    f"{terminated.get('reason') or 'Terminated'}; exit code {terminated['exit_code']}",
                    "Inspect this container's logs for the application error. The exit code alone "
                    "does not identify the cause.",
                    container_name,
                )
    messages = [(event.get("reason", ""), event.get("message", "")) for event in events]
    messages.extend(
        (condition.get("reason", ""), condition.get("message", ""))
        for condition in status.get("conditions") or []
        if condition.get("type") == "PodScheduled" and condition.get("status") == "False"
    )
    for reason, message in messages:
        message = message or ""
        lower = message.lower()
        if reason in {"FailedScheduling", "Unschedulable"}:
            add(
                "unschedulable",
                "Kubernetes could not schedule the pod",
                message or reason,
                "Review the scheduler's stated constraints, resource requests, node selectors and affinity.",
            )
            if "insufficient " in lower:
                add(
                    "insufficient_resources",
                    "Scheduler reports insufficient resources",
                    message,
                    "Compare requested resources with eligible node capacity; adjust requests or "
                    "add capacity.",
                )
            if "taint" in lower:
                add(
                    "taint_mismatch",
                    "Scheduler reports a taint constraint",
                    message,
                    "Check whether the workload belongs on these nodes before adding a matching toleration.",
                )
        if "persistentvolumeclaim" in lower and ("not found" in lower or "unbound" in lower):
            add(
                "pvc_missing",
                "A required persistent volume claim is unavailable",
                message,
                "Check the named PVC in this namespace, its binding state and storage class. Do "
                "not create an empty replacement for required data.",
            )
        if "forbidden" in lower or reason == "Forbidden":
            add(
                "permission_denied",
                "Kubernetes reported a denied operation",
                message or reason,
                "Identify the denied verb/resource and acting service account in the evidence; "
                "ask an administrator for a namespace-scoped permission.",
            )
        if reason in {"FailedToRetrieveImagePullSecret"} or "failed to pull image" in lower:
            add(
                "image_pull_failed",
                "Kubernetes could not access the container image",
                message or reason,
                "Check the image reference, registry reachability and imagePullSecrets in this namespace.",
            )
    if not findings and status.get("phase") in {"Pending", "Failed"}:
        add(
            "cause_unknown",
            f"Pod is {status['phase']} without a specific observed cause",
            status.get("message") or status.get("reason") or f"Pod phase: {status['phase']}",
            "Inspect container logs and newer Kubernetes events. Absence of diagnostic evidence "
            "does not establish a cause.",
            severity="warning",
        )
    return list(findings.values())


def _limits(pod_limit: int, event_limit: int, timeout: float) -> WaitDeadline:
    if type(pod_limit) is not int or not 1 <= pod_limit <= 50:
        raise ValueError("pod_limit must be between 1 and 50")
    if type(event_limit) is not int or not 1 <= event_limit <= 200:
        raise ValueError("event_limit must be between 1 and 200")
    if timeout is None or timeout <= 0 or timeout > 30:
        raise ValueError("timeout must be between 0 (exclusive) and 30 seconds")
    return WaitDeadline(timeout)


def _report(driver, namespace: str) -> Diagnosis:
    labels = driver.metadata.labels or {}
    if labels.get("spark-role") != "driver":
        raise ValueError("The selected pod is not a Spark driver")
    app_id = labels.get("spark-app-id") or labels.get("spark-app-selector")
    if not app_id:
        raise ValueError("The driver has no Spark application identity label")
    validate_target(namespace, app_id, label=True)
    return Diagnosis(namespace, app_id, driver.metadata.name)


def _incomplete(report: Diagnosis, resource: str, message: str) -> None:
    report.incomplete = True
    report.findings.append(
        DiagnosticFinding(
            "collection_incomplete",
            "warning",
            resource,
            "Some diagnostic evidence is unavailable",
            message,
            "Check read access to pods/events and retry. This report is a bounded snapshot, not a "
            "complete history.",
        )
    )


def _executor_options(driver, report: Diagnosis, limit: int) -> dict:
    selector = (driver.metadata.labels or {}).get("spark-app-selector", report.app_id)
    validate_target(report.namespace, selector, label=True)
    return {
        "namespace": report.namespace,
        "label_selector": f"spark-role=executor,spark-app-selector={selector}",
        "limit": limit,
    }


def _owned_executors(driver, response, report: Diagnosis) -> list:
    if response.metadata and response.metadata._continue:
        _incomplete(report, report.driver_pod, "Executor limit reached; additional pods were not inspected.")
    owned = []
    for pod in response.items:
        if driver.metadata.uid and any(
            owner.uid == driver.metadata.uid for owner in pod.metadata.owner_references or []
        ):
            owned.append(pod)
        else:
            _incomplete(
                report,
                report.driver_pod,
                "An executor label matched, but driver ownership could not be verified; it was excluded.",
            )
    return owned


def _event_options(pod, namespace: str, limit: int) -> dict:
    return {
        "namespace": namespace,
        "field_selector": f"involvedObject.uid={pod.metadata.uid}",
        "limit": limit,
    }


def _record(report: Diagnosis, pod, response=None) -> None:
    events = response.items if response is not None else []
    if response is not None and response.metadata and response.metadata._continue:
        _incomplete(report, pod.metadata.name, "Event limit reached; additional events were not inspected.")
    # Filter again by UID: never mix events from a replaced pod with the same name.
    events = [
        event for event in events if event.involved_object and event.involved_object.uid == pod.metadata.uid
    ]
    report.inspected_pods += 1
    report.inspected_events += len(events)
    report.findings.extend(classify_pod(pod.to_dict(), [event.to_dict() for event in events]))


def collect_diagnosis(api, driver, *, namespace: str, pod_limit=20, event_limit=100, timeout=10) -> Diagnosis:
    """Collect at most one executor page and one event page per inspected pod."""
    from kubernetes.client import ApiException
    from urllib3.exceptions import TimeoutError as HTTPTimeoutError

    deadline = _limits(pod_limit, event_limit, timeout)
    report = _report(driver, namespace)
    pods = [driver]
    try:
        response = api.list_namespaced_pod(
            **_executor_options(driver, report, pod_limit), **deadline.request_options()
        )
        pods.extend(_owned_executors(driver, response, report))
    except (ApiException, TimeoutError, HTTPTimeoutError) as error:
        _incomplete(
            report, driver.metadata.name, f"Executor listing failed ({getattr(error, 'status', 'timeout')})."
        )
    for pod in pods:
        response = None
        try:
            if not pod.metadata.uid:
                raise ValueError("Pod UID is unavailable")
            response = api.list_namespaced_event(
                **_event_options(pod, namespace, event_limit), **deadline.request_options()
            )
        except (ApiException, TimeoutError, HTTPTimeoutError, ValueError) as error:
            _incomplete(
                report,
                pod.metadata.name,
                f"Event listing failed ({getattr(error, 'status', 'unavailable')}).",
            )
        _record(report, pod, response)
    return report


async def collect_diagnosis_async(
    api, driver, *, namespace: str, pod_limit=20, event_limit=100, timeout=10
) -> Diagnosis:
    """Async transport for the same classifier and bounded collection policy."""
    from kubernetes_asyncio.client import ApiException

    deadline = _limits(pod_limit, event_limit, timeout)
    report = _report(driver, namespace)
    pods = [driver]
    try:
        response = await api.list_namespaced_pod(
            **_executor_options(driver, report, pod_limit), **deadline.request_options()
        )
        pods.extend(_owned_executors(driver, response, report))
    except (ApiException, TimeoutError) as error:
        _incomplete(
            report, driver.metadata.name, f"Executor listing failed ({getattr(error, 'status', 'timeout')})."
        )
    for pod in pods:
        response = None
        try:
            if not pod.metadata.uid:
                raise ValueError("Pod UID is unavailable")
            response = await api.list_namespaced_event(
                **_event_options(pod, namespace, event_limit), **deadline.request_options()
            )
        except (ApiException, TimeoutError, ValueError) as error:
            _incomplete(
                report,
                pod.metadata.name,
                f"Event listing failed ({getattr(error, 'status', 'unavailable')}).",
            )
        _record(report, pod, response)
    return report
