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
            body = json.dumps(state["body"]).encode()
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


@pytest.mark.parametrize("stall", ["headers", "body"])
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
