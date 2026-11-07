"""Keep the real HTTP log bytes usable as line-delimited scenario evidence."""

from __future__ import annotations

import io
import json
import runpy
from pathlib import Path
from unittest import mock

import pytest
from urllib3.response import HTTPResponse


@pytest.mark.parametrize("integration", ["airflow", "spark"])
def test_log_response_retains_receipt_lines_and_releases_connection(integration):
    runner = runpy.run_path(str(Path(__file__).parents[1] / integration / "run_kubernetes.py"))
    prefix = "AIRFLOW_K8S_CASE_RESULT "
    records = [{"scenario": "sync-success", "passed": True}, {"scenario": "deferred-timeout", "passed": True}]
    body = (
        b"Spark log with invalid UTF-8: \xff\n"
        + b"\n".join((prefix + json.dumps(record)).encode() for record in records)
        + b"\n"
    )
    response = HTTPResponse(body=io.BytesIO(body), preload_content=False)
    api = mock.Mock(read_namespaced_pod_log=mock.Mock(return_value=response))
    text = runner["read_logs"](api, "spark", "runner", container="airflow")
    extracted = [
        json.loads(line.removeprefix(prefix)) for line in text.splitlines() if line.startswith(prefix)
    ]
    assert extracted == records
    assert "\ufffd" in text
    assert response.closed
    api.read_namespaced_pod_log.assert_called_once_with(
        "runner",
        "spark",
        container="airflow",
        _preload_content=False,
        _request_timeout=runner["REQUEST_TIMEOUT"],
    )
