from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from socketserver import TCPServer

import pytest
from click.testing import CliRunner
from spark_on_k8s.cli.remote import remote_cli
from spark_on_k8s.remote import RemoteClient, RemoteError

TOKEN = "test-api-token-with-more-than-32-characters"
DOCUMENT = {
    "apiVersion": "spark-on-k8s/v1",
    "kind": "SparkJob",
    "metadata": {"name": "report"},
    "spec": {"image": "approved:image", "app_path": "local:///jobs/report.py"},
}
RECEIPT = {
    "schema_version": 1,
    "operation": "submit",
    "error": None,
    "status": None,
    "run": {"namespace": "analytics", "app_id": "report-uuid", "driver_pod": "report-uuid-driver"},
}


@pytest.fixture
def http_server():
    requests = []
    state = {"status": 201, "body": RECEIPT, "stall": None}
    release = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            payload = self.rfile.read(int(self.headers["Content-Length"]))
            requests.append((self.path, dict(self.headers), json.loads(payload)))
            if state["stall"] == "headers":
                self.wfile.write(b"HTTP/1.0 200 OK\r\nX-Slow: ")
                self.wfile.flush()
                release.wait(5)
                return
            body = state["body"] if isinstance(state["body"], bytes) else json.dumps(state["body"]).encode()
            self.send_response(state["status"])
            self.send_header("Content-Type", "application/json")
            self.send_header("Location", "/token-theft-target")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if state["stall"] == "body":
                self.wfile.write(body[:1])
                self.wfile.flush()
                release.wait(5)
                return
            if state["stall"] == "trickle":
                for byte in body:
                    try:
                        self.wfile.write(bytes([byte]))
                        self.wfile.flush()
                    except OSError:
                        return
                    if release.wait(0.03):
                        return
                return
            self.wfile.write(body)

        def log_message(self, *_args):
            pass

    class LocalHTTPServer(ThreadingHTTPServer):
        def server_bind(self):
            # HTTPServer normally reverse-resolves its address; these tests need no DNS.
            TCPServer.server_bind(self)
            self.server_name = "localhost"
            self.server_port = self.server_address[1]

    server = LocalHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/spark", state, requests
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        thread.join(timeout=3)
        assert not thread.is_alive()


def test_cli_sends_versioned_job_and_receives_run_without_local_kubernetes(
    http_server, tmp_path, monkeypatch
):
    url, _, requests = http_server
    file = tmp_path / "job.yaml"
    file.write_text(
        "apiVersion: spark-on-k8s/v1\nkind: SparkJob\nmetadata:\n  name: report\nspec:\n"
        "  image: approved:image\n  app_path: local:///jobs/report.py\n"
    )
    token_file = tmp_path / "token"
    token_file.write_text(TOKEN + "\n")

    def forbidden(*_args, **_kwargs):
        raise AssertionError(
            "Remote CLI must not load local Kubernetes credentials or construct a local plan"
        )

    monkeypatch.setattr("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client", forbidden)
    monkeypatch.setattr("spark_on_k8s.jobs.JobSpec.plan", forbidden)
    result = CliRunner().invoke(
        remote_cli, ["submit", str(file), "--api-url", url, "--token-file", str(token_file)]
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == RECEIPT
    assert result.stderr == ""
    assert len(requests) == 1
    path, headers, document = requests[0]
    assert path == "/spark/jobs/v1/submit"
    assert headers["Authorization"] == "Bearer " + TOKEN
    assert document == DOCUMENT
    assert TOKEN not in result.output


def test_remote_plan_and_text_output(http_server, tmp_path, monkeypatch):
    url, state, requests = http_server
    state.update(
        status=200,
        body={
            "schema_version": 1,
            "operation": "plan",
            "error": None,
            "plan": {"preview": True, "resources": []},
        },
    )
    file = tmp_path / "job.json"
    file.write_text(json.dumps(DOCUMENT))
    monkeypatch.setenv("SPARK_ON_K8S_API_TOKEN", TOKEN)
    monkeypatch.setenv("SPARK_ON_K8S_API_URL", url)
    result = CliRunner().invoke(remote_cli, ["plan", str(file), "--output", "text"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {"preview": True, "resources": []}
    assert requests[0][0] == "/spark/jobs/v1/plan"


@pytest.mark.parametrize(
    "status,code,exit_code",
    [
        (401, "unauthorized", 1),
        (403, "policy_denied", 1),
        (404, "remote_disabled", 4),
        (502, "submission_error", 1),
    ],
)
def test_cli_preserves_structured_errors_and_does_not_retry(
    http_server, tmp_path, monkeypatch, status, code, exit_code
):
    url, state, requests = http_server
    state.update(
        status=status, body={"schema_version": 1, "error": {"code": code, "message": "Not accepted."}}
    )
    file = tmp_path / "job.json"
    file.write_text(json.dumps(DOCUMENT))
    monkeypatch.setenv("SPARK_ON_K8S_API_TOKEN", TOKEN)
    result = CliRunner().invoke(remote_cli, ["submit", str(file), "--api-url", url])
    assert result.exit_code == exit_code
    assert json.loads(result.stdout) == {
        "schema_version": 1,
        "operation": "submit",
        "error": {"code": code, "message": "Not accepted."},
    }
    assert len(requests) == 1


@pytest.mark.parametrize("stall", ["headers", "body", "trickle"])
def test_http_deadline_interrupts_stalled_headers_and_detached_body_socket(http_server, stall):
    url, state, requests = http_server
    state["stall"] = stall
    started = time.monotonic()
    with pytest.raises(RemoteError) as error:
        RemoteClient(url, TOKEN, timeout=0.15).request("submit", DOCUMENT)
    assert error.value.code == "timeout"
    assert time.monotonic() - started < 2
    assert len(requests) == 1


def test_redirects_never_forward_tokens_or_repeat_post(http_server):
    url, state, requests = http_server
    state["status"] = 307
    with pytest.raises(RemoteError) as error:
        RemoteClient(url, TOKEN).request("submit", DOCUMENT)
    assert error.value.code == "redirect_refused"
    assert len(requests) == 1
    assert requests[0][0] == "/spark/jobs/v1/submit"


@pytest.mark.parametrize(
    "body",
    [
        {"schema_version": 2, "error": None},
        {"schema_version": 1, "error": None, "operation": "submit", "run": {}},
        {"schema_version": 1, "error": None, "operation": "different"},
    ],
)
def test_unrecognized_responses_cannot_be_reported_as_success(http_server, body):
    url, state, _ = http_server
    state["body"] = body
    with pytest.raises(RemoteError) as error:
        RemoteClient(url, TOKEN).request("submit", DOCUMENT)
    assert error.value.code == "invalid_response"


def test_response_size_is_bounded(http_server, monkeypatch):
    url, _, _ = http_server
    monkeypatch.setattr("spark_on_k8s.remote.MAX_RESPONSE_BYTES", 16)
    with pytest.raises(RemoteError) as error:
        RemoteClient(url, TOKEN).request("submit", DOCUMENT)
    assert error.value.code == "invalid_response"
    assert "inspect existing runs before retrying" in str(error.value)


@pytest.mark.parametrize(
    "status,body",
    [
        (201, b"not json"),
        (201, {"schema_version": 1, "error": None, "operation": "submit", "run": {}}),
        (200, RECEIPT),
        (502, b"<html>Bad gateway</html>"),
    ],
)
def test_uncertain_submit_responses_warn_before_any_retry(http_server, tmp_path, monkeypatch, status, body):
    url, state, requests = http_server
    state.update(status=status, body=body)
    file = tmp_path / "job.json"
    file.write_text(json.dumps(DOCUMENT))
    monkeypatch.setenv("SPARK_ON_K8S_API_TOKEN", TOKEN)
    result = CliRunner().invoke(remote_cli, ["submit", str(file), "--api-url", url])
    assert result.exit_code == 1
    error = json.loads(result.stdout)["error"]
    assert error["code"] == "invalid_response"
    # A success status or gateway failure cannot establish that no run was created.
    assert error["message"].endswith(
        "A submission may still have succeeded; inspect existing runs before retrying."
    )
    assert len(requests) == 1


@pytest.mark.parametrize(
    "status,body,operation",
    [
        (401, b"<html>Sign in</html>", "submit"),
        (201, b"not json", "plan"),
    ],
)
def test_rejected_or_preview_invalid_responses_do_not_claim_a_possible_submission(
    http_server, status, body, operation
):
    url, state, _ = http_server
    state.update(status=status, body=body)
    with pytest.raises(RemoteError) as caught:
        RemoteClient(url, TOKEN).request(operation, DOCUMENT)
    assert caught.value.code == "invalid_response"
    assert "may still have succeeded" not in str(caught.value)


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com",
        "https://user:password@example.com",
        "file:///tmp/socket",
        "https://example.com?token=secret",
        "https://example.com#fragment",
    ],
)
def test_client_refuses_insecure_or_credential_bearing_urls(url):
    with pytest.raises(ValueError, match="HTTPS"):
        RemoteClient(url, TOKEN)


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan"), 301])
def test_client_rejects_unbounded_or_invalid_deadlines(timeout):
    with pytest.raises(ValueError, match="timeout"):
        RemoteClient("https://example.com", TOKEN, timeout=timeout)


def test_remote_cli_rejects_local_credentials_before_sending(http_server, tmp_path, monkeypatch):
    url, _, requests = http_server
    file = tmp_path / "job.json"
    file.write_text(json.dumps({**DOCUMENT, "kubernetes": {"config_file": "/not-a-kubeconfig"}}))
    monkeypatch.setenv("SPARK_ON_K8S_API_TOKEN", TOKEN)
    result = CliRunner().invoke(remote_cli, ["submit", str(file), "--api-url", url])
    assert result.exit_code == 2
    assert json.loads(result.stdout)["error"]["code"] == "invalid_request"
    assert requests == []


@pytest.mark.parametrize("token", ["", "contains space", "line\nbreak", "\N{SNOWMAN}"])
def test_invalid_bearer_value_is_rejected_before_a_connection(token):
    with pytest.raises(ValueError, match="printable ASCII"):
        RemoteClient("https://example.test", token)


@pytest.mark.parametrize("operation,document", [("kill", {}), ("submit", {"large": "x" * (1024 * 1024)})])
def test_unsupported_or_oversized_requests_never_open_connection(monkeypatch, operation, document):
    from unittest.mock import Mock

    connect = Mock()
    monkeypatch.setattr("spark_on_k8s.remote.http.client.HTTPSConnection", connect)
    with pytest.raises(ValueError):
        RemoteClient("https://example.test", TOKEN).request(operation, document)
    connect.assert_not_called()


@pytest.mark.parametrize(
    "status,body,operation",
    [
        (403, {"schema_version": 1, "error": {"code": 12}}, "submit"),
        (200, {"schema_version": 1, "operation": "plan", "error": None, "plan": []}, "plan"),
    ],
)
def test_malformed_error_or_plan_is_never_reported_as_success(http_server, status, body, operation):
    url, state, requests = http_server
    state.update(status=status, body=body)
    with pytest.raises(RemoteError) as caught:
        RemoteClient(url, TOKEN).request(operation, DOCUMENT)
    assert caught.value.code == "invalid_response"
    assert len(requests) == 1


@pytest.mark.parametrize("timeout", [False, True])
def test_connection_failure_is_sanitized_closed_and_not_retried(monkeypatch, timeout):
    from unittest.mock import Mock

    connection = Mock()
    connection.connect.side_effect = TimeoutError("private host") if timeout else OSError("private host")
    factory = Mock(return_value=connection)
    monkeypatch.setattr("spark_on_k8s.remote.http.client.HTTPSConnection", factory)
    with pytest.raises(RemoteError) as caught:
        RemoteClient("https://example.test", TOKEN).request("submit", DOCUMENT)
    assert caught.value.code == ("timeout" if timeout else "connection_error")
    assert "private host" not in str(caught.value)
    connection.connect.assert_called_once()
    connection.close.assert_called_once()
    connection.request.assert_not_called()


@pytest.mark.parametrize(
    "url, host, port",
    [
        ("http://[::1]/spark", "::1", 80),
        ("https://[2001:db8::1]/team", "2001:db8::1", 443),
        ("https://example.test", "example.test", 443),
        ("https://[2001:db8::1]:8443", "2001:db8::1", 8443),
    ],
)
def test_bracketed_ipv6_url_without_port_uses_the_scheme_default(monkeypatch, url, host, port):
    import http.client

    attempts = []

    def connect(self):
        attempts.append((self.host, self.port))
        raise OSError("not attempted in tests")

    # Exercise the real connection classes; http.client would otherwise parse an IPv6 tail as a port.
    monkeypatch.setattr(http.client.HTTPConnection, "connect", connect)
    with pytest.raises(RemoteError):
        RemoteClient(url, TOKEN).request("plan", DOCUMENT)
    assert attempts == [(host, port)]


def test_deadline_exhausted_during_connect_never_sends_request(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import Mock

    clock = iter([0.0, 2.0])
    connection = Mock()
    monkeypatch.setattr("spark_on_k8s.remote.time", SimpleNamespace(monotonic=lambda: next(clock)))
    monkeypatch.setattr("spark_on_k8s.remote.http.client.HTTPSConnection", Mock(return_value=connection))
    with pytest.raises(RemoteError) as caught:
        RemoteClient("https://example.test", TOKEN, timeout=1).request("submit", DOCUMENT)
    assert caught.value.code == "timeout"
    connection.request.assert_not_called()
    connection.close.assert_called_once()


def test_oversized_cli_token_file_is_rejected_before_request(tmp_path, monkeypatch):
    from unittest.mock import Mock

    file = tmp_path / "job.json"
    file.write_text(json.dumps(DOCUMENT))
    token = tmp_path / "token"
    token.write_text("x" * 4097)
    request = Mock()
    monkeypatch.setattr(RemoteClient, "request", request)
    result = CliRunner().invoke(
        remote_cli, ["submit", str(file), "--api-url", "https://example.test", "--token-file", str(token)]
    )
    assert result.exit_code == 2
    assert json.loads(result.stdout)["error"]["code"] == "invalid_request"
    request.assert_not_called()


@pytest.mark.parametrize("interrupted", [False, True])
def test_cli_interrupt_and_text_error_preserve_exit_status(tmp_path, monkeypatch, interrupted):
    from unittest.mock import Mock

    file = tmp_path / "job.json"
    file.write_text(json.dumps(DOCUMENT))
    monkeypatch.setenv("SPARK_ON_K8S_API_TOKEN", TOKEN)
    failure = KeyboardInterrupt() if interrupted else RemoteError("denied", "Not authorized.", status=403)
    monkeypatch.setattr(RemoteClient, "request", Mock(side_effect=failure))
    result = CliRunner().invoke(
        remote_cli, ["submit", str(file), "--api-url", "https://example.test", "--output", "text"]
    )
    assert result.exit_code == (130 if interrupted else 1)
    assert result.stdout == ""
    assert ("Interrupted" if interrupted else "Not authorized") in result.stderr


def test_successful_remote_submit_text_prints_only_driver_identity(http_server, tmp_path, monkeypatch):
    url, _, _ = http_server
    file = tmp_path / "job.json"
    file.write_text(json.dumps(DOCUMENT))
    monkeypatch.setenv("SPARK_ON_K8S_API_TOKEN", TOKEN)
    result = CliRunner().invoke(remote_cli, ["submit", str(file), "--api-url", url, "--output", "text"])
    assert result.exit_code == 0
    assert result.stdout == RECEIPT["run"]["driver_pod"] + "\n"
    assert result.stderr == ""


@pytest.mark.parametrize("operation", ["plan", "submit"])
def test_yaml_only_values_are_reported_as_invalid_input_before_sending(
    http_server, tmp_path, monkeypatch, operation
):
    url, _, requests = http_server
    file = tmp_path / "job.yaml"
    # YAML reads an unquoted date as a date object, which the JSON request cannot carry.
    file.write_text(
        "apiVersion: spark-on-k8s/v1\nkind: SparkJob\nmetadata:\n  name: report\nspec:\n"
        "  image: approved:image\n  app_path: local:///jobs/report.py\n"
        "  driver_pod_template:\n    apiVersion: v1\n    kind: Pod\n    spec:\n      containers:\n"
        "        - name: driver\n          env:\n            - name: RUN_DATE\n"
        "              value: 2024-01-01\n"
    )
    monkeypatch.setenv("SPARK_ON_K8S_API_TOKEN", TOKEN)
    result = CliRunner().invoke(remote_cli, [operation, str(file), "--api-url", url])
    assert result.exit_code == 2, result.output
    document = json.loads(result.stdout)
    assert document["schema_version"] == 1
    assert document["operation"] == operation
    assert document["error"]["code"] == "invalid_request"
    assert requests == []
