"""A client that disconnects during a job upload is audited as rejected: nothing was received or created."""

from __future__ import annotations

import asyncio

import pytest
from spark_on_k8s.api import audit
from spark_on_k8s.api.main import app

from .test_audit import TOKEN, events, remote  # noqa: F401


def abandon_upload(path):
    """Send the first part of a body, then disconnect, as uvicorn reports a dropped connection."""
    messages = iter(
        [{"type": "http.request", "body": b'{"apiVersion"', "more_body": True}, {"type": "http.disconnect"}]
    )
    sent = []

    async def receive():
        return next(messages)

    async def send(message):
        sent.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"authorization", b"Bearer " + TOKEN.encode()), (b"content-type", b"application/json")],
        "client": ("10.0.0.1", 1234),
        "server": ("testserver", 80),
    }
    raised = None
    try:
        asyncio.run(app(scope, receive, send))
    except Exception as error:
        raised = error
    return raised, sent


@pytest.mark.parametrize("operation", ["submit", "plan"])
def test_abandoned_upload_is_not_recorded_as_an_unknown_submission(remote, events, operation):  # noqa: F811
    _client, api = remote
    raised, sent = abandon_upload(f"/jobs/v1/{operation}")
    print("RAISED", repr(raised))
    print("SENT", sent)
    print("EVENTS", events())
    print("METRICS", [line for line in audit.metric_lines() if not line.startswith("#")])
    # Nothing was read, so nothing was created.
    api.create_namespaced_pod.assert_not_called()
    [event] = events()
    assert event["event"] == f"job.{operation}"
    # "unknown" means that a resource may exist and the operator must reconcile it.
    assert event["outcome"] != "unknown", event
    assert (event["code"], event["status"]) != ("internal_error", 500), event
