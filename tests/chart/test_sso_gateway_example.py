"""Keep the oauth2-proxy example consistent with the chart release it protects."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import yaml

HELM = shutil.which("helm")
ROOT = Path(__file__).resolve().parents[2]
CHART = ROOT / "chart"
EXAMPLE = ROOT / "examples/sso-gateway"
VALUES = EXAMPLE / "console-values.yaml"
RELEASE = "spark-console"
NAMESPACE = "spark-system"
pytestmark = pytest.mark.skipif(HELM is None, reason="Helm is required to render the chart")


def documents(name):
    return [document for document in yaml.safe_load_all((EXAMPLE / name).read_text()) if document]


GATEWAY = {document["kind"]: document for document in documents("gateway.yaml")}
(POLICY,) = documents("network-policy.yaml")
POD = GATEWAY["Deployment"]["spec"]["template"]
PROXY = POD["spec"]["containers"][0]
ARGS = dict(argument.removeprefix("--").split("=", 1) for argument in PROXY["args"])
ALPHA = yaml.safe_load(GATEWAY["ConfigMap"]["data"]["alpha-config.yaml"])
(PROVIDER,) = ALPHA["providers"]


@pytest.fixture
def release(release_chart):
    result = subprocess.run(
        [HELM, "template", RELEASE, str(CHART), "--namespace", NAMESPACE, "-f", str(VALUES)],
        check=True,
        capture_output=True,
        text=True,
    )
    rendered = [document for document in yaml.safe_load_all(result.stdout) if document]
    name = f"{RELEASE}-spark-on-k8s"
    resources = {document["kind"]: document for document in rendered if document["metadata"]["name"] == name}
    container = resources["Deployment"]["spec"]["template"]["spec"]["containers"][0]
    return {
        "kinds": {document["kind"] for document in rendered},
        "service": resources["Service"],
        "pod_labels": resources["Deployment"]["spec"]["template"]["metadata"]["labels"],
        "container": container,
        "env": {entry["name"]: entry.get("value") for entry in container["env"]},
    }


def test_public_origin_matches_callback_route_and_log_stream_allowlist(release):
    ingress = GATEWAY["Ingress"]["spec"]
    (rule,) = ingress["rules"]
    assert any(rule["host"] in tls["hosts"] for tls in ingress["tls"])
    origin = f"https://{rule['host']}"
    callback = urlsplit(ARGS["redirect-url"])
    assert (f"{callback.scheme}://{callback.netloc}", callback.path) == (origin, "/oauth2/callback")
    # The gateway terminates TLS, so only an explicit list accepts the browser's log streams.
    assert release["env"]["SPARK_ON_K8S_API_WEBSOCKET_ALLOWED_ORIGINS"] == origin
    assert "SPARK_ON_K8S_API_ROOT_PATH" not in release["env"]
    (path,) = rule["http"]["paths"]
    assert (path["path"], path["pathType"]) == ("/", "Prefix")
    service = GATEWAY["Service"]
    assert path["backend"]["service"] == {"name": service["metadata"]["name"], "port": {"name": "http"}}
    assert service["spec"]["ports"][0]["name"] == "http"
    assert service["spec"]["selector"].items() <= POD["metadata"]["labels"].items()


def test_gateway_routes_every_path_and_websocket_upgrade_to_the_api_service(release):
    (upstream,) = ALPHA["upstreamConfig"]["upstreams"]
    assert upstream["path"] == "/"
    assert upstream.get("proxyWebSockets", True) is True
    assert not upstream.get("static") and "rewriteTarget" not in upstream
    target = urlsplit(upstream["uri"])
    service = release["service"]
    assert target.scheme == "http"
    assert target.hostname in {
        service["metadata"]["name"],
        f"{service['metadata']['name']}.{NAMESPACE}.svc.cluster.local",
    }
    assert target.port == service["spec"]["ports"][0]["port"]
    assert release["kinds"].isdisjoint({"Ingress"})
    assert service["spec"]["type"] == "ClusterIP"


def test_gateway_answers_api_probe_paths_without_authentication_bypasses(release):
    container = release["container"]
    probes = {container[probe]["httpGet"]["path"] for probe in ("livenessProbe", "readinessProbe")}
    # oauth2-proxy answers its ping and ready paths before routing, so these never reach the API.
    assert {ARGS["ping-path"], ARGS["ready-path"]} == probes == {"/health", "/ready"}
    assert {PROXY["livenessProbe"]["httpGet"]["path"], PROXY["readinessProbe"]["httpGet"]["path"]} == probes
    assert ARGS.keys().isdisjoint(
        {"skip-auth-route", "skip-auth-regex", "skip-auth-preflight", "trusted-ip", "skip-jwt-bearer-tokens"}
    )


def test_network_policy_admits_only_gateway_pods_to_the_api_port(release):
    spec = POLICY["spec"]
    selector = spec["podSelector"]["matchLabels"]
    assert selector.items() <= release["pod_labels"].items()
    assert not selector.items() <= POD["metadata"]["labels"].items()
    assert spec["policyTypes"] == ["Ingress"]
    (rule,) = spec["ingress"]
    (peer,) = rule["from"]
    # A namespaceSelector or ipBlock would widen access beyond the gateway pods.
    assert peer.keys() == {"podSelector"}
    assert peer["podSelector"]["matchLabels"].items() <= POD["metadata"]["labels"].items()
    (api_port,) = release["container"]["ports"]
    assert rule["ports"] == [{"protocol": "TCP", "port": api_port["name"]}]


def test_gateway_restricts_users_and_injects_no_identity_headers():
    assert ALPHA.keys().isdisjoint({"injectRequestHeaders", "injectResponseHeaders"})
    assert PROVIDER["allowedGroups"]
    assert ARGS["email-domain"] not in {"", "*"}
    assert PROVIDER["code_challenge_method"] == "S256"
    assert PROVIDER["oidcConfig"]["insecureSkipNonce"] is False
    assert (ARGS["cookie-secure"], ARGS["cookie-samesite"]) == ("true", "lax")
    assert ARGS["cookie-name"].startswith("__Host-")
    assert len(ARGS) == len(PROXY["args"])


def test_example_commits_no_credentials():
    for path in EXAMPLE.iterdir():
        assert all(document.get("kind") != "Secret" for document in documents(path.name)), path
    assert "clientSecret" not in PROVIDER
    assert ARGS.keys().isdisjoint({"cookie-secret", "client-secret", "client-secret-file"})
    secret = {"name": "spark-console-gateway", "key": "cookie-secret"}
    assert PROXY["env"] == [{"name": "OAUTH2_PROXY_COOKIE_SECRET", "valueFrom": {"secretKeyRef": secret}}]
    mount = Path(PROVIDER["clientSecretFile"])
    volume_name = next(m["name"] for m in PROXY["volumeMounts"] if Path(m["mountPath"]) == mount.parent)
    volume = next(v for v in POD["spec"]["volumes"] if v["name"] == volume_name)
    assert volume["secret"]["secretName"] == secret["name"]
    assert volume["secret"]["items"] == [{"key": "client-secret", "path": mount.name}]


def test_gateway_image_is_pinned_to_a_release_tag():
    repository, tag = PROXY["image"].rsplit(":", 1)
    assert repository == "quay.io/oauth2-proxy/oauth2-proxy"
    assert re.fullmatch(r"v\d+\.\d+\.\d+", tag)
