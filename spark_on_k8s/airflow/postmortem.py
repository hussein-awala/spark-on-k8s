"""Bounded failure evidence written to the Airflow task log before cleanup can delete the driver."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from kubernetes import client as k8s

from spark_on_k8s.diagnostics import _redact_credentials, collect_diagnosis
from spark_on_k8s.utils.run import WaitDeadline
from spark_on_k8s.utils.spark_app_status import driver_container_name

if TYPE_CHECKING:
    from spark_on_k8s.diagnostics import Diagnosis
    from spark_on_k8s.k8s.sync_client import KubernetesClientManager

# One budget covers every read, so a postmortem delays cleanup by about this many seconds at most.
POSTMORTEM_TIMEOUT = 5.0
# The diagnosis may use at most this part of the budget, leaving time for the log tail.
_DIAGNOSIS_TIMEOUT = 3.0
MAX_LOG_TAIL_LINES = 1000
# Kubernetes stops sending the requested tail after this many bytes.
_LOG_TAIL_TRANSFER_BYTES = 1024 * 1024
_LOG_READ_CHUNK = 64 * 1024
_LOG_LINE_CHARS = 2000
_LOG_TAIL_CHARS = 64 * 1024
_MAX_FINDINGS = 20
_LEVELS = {"error": logging.ERROR, "warning": logging.WARNING}
_OUTCOMES = {
    "startup_timeout": "exceeded its startup timeout",
    "error": "could not be observed by the trigger",
    "killed": "is being stopped because the task was killed",
}


def describe_error(error: BaseException) -> str:
    """Name a failure without its response body or headers, which can be large or sensitive."""
    status = getattr(error, "status", None)
    return f"{type(error).__name__} {status}" if isinstance(status, int) else type(error).__name__


def log_failure_postmortem(
    log: logging.Logger,
    k8s_client_manager: KubernetesClientManager,
    *,
    namespace: str,
    pod_name: str,
    status: str,
    diagnose: bool,
    log_tail_lines: int,
    timeout: float = POSTMORTEM_TIMEOUT,
) -> dict[str, Any] | None:
    """Log a bounded diagnosis and the driver's last log lines, before the driver is deleted.

    Diagnosis and log-tail failures are logged separately so that one cannot hide the other.
    Failing to create a client or read the driver pod raises; the caller decides how to report it.

    Returns:
        A compact, JSON-serializable diagnosis summary, or None when no diagnosis was collected.
    """
    deadline = WaitDeadline(timeout)
    with k8s_client_manager.client() as client:
        api = k8s.CoreV1Api(client)
        driver = api.read_namespaced_pod(namespace=namespace, name=pod_name, **deadline.request_options())
        log.warning(
            "Spark failure postmortem for %s/%s, which %s:",
            namespace,
            pod_name,
            _OUTCOMES.get(status, f"finished with status {status}"),
        )
        summary = None
        if diagnose:
            report = _log_diagnosis(log, api, driver, namespace, deadline)
            summary = _summary(report, status) if report is not None else None
        if log_tail_lines:
            _log_driver_tail(log, api, driver, namespace, log_tail_lines, deadline)
    return summary


def _log_diagnosis(log, api, driver, namespace: str, deadline: WaitDeadline) -> Diagnosis | None:
    try:
        report = collect_diagnosis(
            api, driver, namespace=namespace, timeout=min(_DIAGNOSIS_TIMEOUT, deadline.remaining())
        )
    except Exception as error:
        log.warning("Diagnosis unavailable (%s).", describe_error(error))
        return None
    log.warning(
        "Diagnosis: %d finding(s) from %d pod(s) and %d event(s)%s.",
        len(report.findings),
        report.inspected_pods,
        report.inspected_events,
        "; collection was incomplete" if report.incomplete else "",
    )
    for finding in report.findings[:_MAX_FINDINGS]:
        # Evidence is already bounded and redacted by the diagnostics module.
        log.log(
            _LEVELS.get(finding.severity, logging.INFO),
            "[%s] %s on pod %s%s: %s. Evidence: %s. Next step: %s",
            finding.severity,
            finding.code,
            finding.resource,
            f", container {finding.container}" if finding.container else "",
            finding.summary,
            finding.evidence,
            finding.remediation,
        )
    if len(report.findings) > _MAX_FINDINGS:
        log.warning("%d more finding(s) omitted.", len(report.findings) - _MAX_FINDINGS)
    if not report.findings:
        log.info("No specific problem was observed in pod states or events; this does not establish a cause.")
    return report


def _summary(report: Diagnosis, status: str) -> dict[str, Any]:
    # Codes and resources only: evidence can contain workload-specific text and is in the task log.
    return {
        "version": 1,
        "status": str(status),
        "namespace": report.namespace,
        "app_id": report.app_id,
        "driver_pod": report.driver_pod,
        "incomplete": report.incomplete,
        "findings": [
            {
                "code": finding.code,
                "severity": finding.severity,
                "resource": finding.resource,
                "container": finding.container,
            }
            for finding in report.findings[:_MAX_FINDINGS]
        ],
        "omitted_findings": max(0, len(report.findings) - _MAX_FINDINGS),
    }


def _container_started(driver, container: str) -> bool:
    for status in (driver.status.container_statuses if driver.status else None) or []:
        if status.name == container:
            state, last_state = status.state, status.last_state
            return bool(
                (state and (state.running or state.terminated)) or (last_state and last_state.terminated)
            )
    return False


def _log_driver_tail(log, api, driver, namespace: str, tail_lines: int, deadline: WaitDeadline) -> None:
    container = driver_container_name(driver)
    if not _container_started(driver, container):
        log.info("Driver container %s has not started; no driver log is available.", container)
        return
    try:
        lines, notes = _read_tail(api, driver.metadata.name, namespace, container, tail_lines, deadline)
    except Exception as error:
        log.warning("Driver log tail unavailable (%s).", describe_error(error))
        return
    log.info(
        "Last %d driver log line(s) from container %s%s:",
        len(lines),
        container,
        f" ({'; '.join(notes)})" if notes else "",
    )
    for line in lines:
        log.info("%s", line)
    log.info("End of the driver log tail.")


def _read_tail(
    api, pod_name: str, namespace: str, container: str, tail_lines: int, deadline: WaitDeadline
) -> tuple[list[str], list[str]]:
    response = api.read_namespaced_pod_log(
        name=pod_name,
        namespace=namespace,
        container=container,
        tail_lines=tail_lines,
        limit_bytes=_LOG_TAIL_TRANSFER_BYTES,
        # Decode leniently below: a byte limit can split a character, and logs need not be UTF-8.
        _preload_content=False,
        **deadline.request_options(),
    )
    data = bytearray()
    notes = []
    try:
        while len(data) < _LOG_TAIL_TRANSFER_BYTES:
            deadline.remaining()
            chunk = response.read(_LOG_READ_CHUNK)
            if not chunk:
                break
            data += chunk
    except Exception as error:
        if not data:
            raise
        notes.append(f"the read stopped early ({describe_error(error)})")
    finally:
        response.close()
    if len(data) >= _LOG_TAIL_TRANSFER_BYTES:
        notes.append("the transfer limit was reached, so the most recent lines may be missing")
    lines = bytes(data).decode("utf-8", errors="replace").splitlines()[-tail_lines:]
    kept: list[str] = []
    size = 0
    # Keep the most recent lines within the size limit.
    for line in reversed(lines):
        line = _redact_credentials(line)
        if len(line) > _LOG_LINE_CHARS:
            line = f"{line[:_LOG_LINE_CHARS]} ... [{len(line) - _LOG_LINE_CHARS} characters omitted]"
        size += len(line) + 1
        if size > _LOG_TAIL_CHARS:
            break
        kept.append(line)
    if len(kept) < len(lines):
        notes.append(f"{len(lines) - len(kept)} earlier line(s) omitted by the size limit")
    kept.reverse()
    return kept, notes
