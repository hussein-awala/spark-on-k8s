"""Optional operational objects: NetworkPolicy, PodDisruptionBudget, ServiceMonitor, workers,
History Server probes and the remote-submission block."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

HELM = shutil.which("helm")
CHART = Path(__file__).resolve().parents[2] / "chart"
REMOTE_FIXTURE = Path(__file__).resolve().parents[1] / "integration" / "chart" / "remote-values.yaml"
SERVICE_MONITOR_API = ["--api-versions", "monitoring.coreos.com/v1/ServiceMonitor"]
HISTORY = {"enabled": True, "image": {"tag": "fixture"}}
NEW_OBJECTS = {"NetworkPolicy", "PodDisruptionBudget", "ServiceMonitor"}
pytestmark = pytest.mark.skipif(HELM is None, reason="Helm is required to render the chart")


def invoke(tmp_path, values, command="template", extra=(), chart=None):
    path = tmp_path / "values.yaml"
    path.write_text(yaml.safe_dump(values))
    args = [HELM, command]
    if command == "template":
        args.extend(["test", str(chart or CHART), "--namespace", "control"])
    else:
        args.append(str(chart or CHART))
    return subprocess.run([*args, "-f", str(path), *extra], capture_output=True, text=True)


def resources(tmp_path, values, extra=(), chart=None):
    result = invoke(tmp_path, values, extra=extra, chart=chart)
    assert result.returncode == 0, result.stderr
    return [item for item in yaml.safe_load_all(result.stdout) if item]


def rejected(tmp_path, values, extra=()):
    result = invoke(tmp_path, values, extra=extra)
    assert result.returncode != 0, result.stdout
    return result.stderr


def kinds(documents, kind):
    return [item for item in documents if item["kind"] == kind]


def named(documents, kind, name):
    return next(item for item in kinds(documents, kind) if item["metadata"]["name"] == name)


def api_pod(documents):
    return named(documents, "Deployment", "test-spark-on-k8s")["spec"]["template"]


def history_pod(documents):
    return named(documents, "Deployment", "test-spark-on-k8s-history")["spec"]["template"]


def environment(documents):
    return {item["name"]: item.get("value") for item in api_pod(documents)["spec"]["containers"][0]["env"]}


def selects(selector, labels):
    return selector.items() <= labels.items()


def test_operations_objects_are_disabled_by_default(tmp_path):
    docs = resources(tmp_path, {"sparkHistory": HISTORY})
    assert not NEW_OBJECTS & {item["kind"] for item in docs}
    env = environment(docs)
    assert "SPARK_ON_K8S_API_WORKERS" not in env
    assert not any(name.startswith("SPARK_ON_K8S_SUBMISSION_") for name in env)
    assert "volumes" not in api_pod(docs)["spec"]


def test_values_reused_from_an_older_release_render_without_new_settings(tmp_path):
    # `helm upgrade --reuse-values` keeps the previous release's values, so settings added in
    # this version are absent rather than defaulted; absence must mean "disabled", not a nil error.
    older = tmp_path / "older"
    shutil.copytree(CHART, older)
    values = yaml.safe_load((older / "values.yaml").read_text())
    for key in ("workers", "remote", "podDisruptionBudget", "networkPolicy"):
        del values[key]
    del values["metrics"]["serviceMonitor"]
    for key in ("readinessProbe", "livenessProbe", "podDisruptionBudget", "networkPolicy"):
        del values["sparkHistory"][key]
    (older / "values.yaml").write_text(yaml.safe_dump(values))
    docs = resources(tmp_path, {"sparkHistory": HISTORY}, chart=older)
    assert not NEW_OBJECTS & {item["kind"] for item in docs}
    assert "readinessProbe" not in history_pod(docs)["spec"]["containers"][0]
    current = resources(tmp_path, {"sparkHistory": HISTORY})
    assert api_pod(docs) == api_pod(current)
    # Enabling a new feature on such a release fails clearly or renders, never with a nil pointer.
    partial = {
        "networkPolicy": {"enabled": True, "egress": {"enabled": True}},
        "podDisruptionBudget": {"enabled": True},
        "metrics": {"enabled": True, "serviceMonitor": {"enabled": True}},
        "remote": {"enabled": True, "existingSecret": "api-remote"},
    }
    result = invoke(tmp_path, partial, extra=SERVICE_MONITOR_API, chart=older)
    assert result.returncode != 0 and "remote.enabled requires remote.tokenKey" in result.stderr
    del partial["remote"]
    docs = resources(tmp_path, partial, extra=SERVICE_MONITOR_API, chart=older)
    assert {item["kind"] for item in docs} >= NEW_OBJECTS


def test_network_policy_admits_listed_peers_and_this_release_to_the_api_port(tmp_path):
    peer = {"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "ingress-nginx"}}}
    docs = resources(
        tmp_path,
        {"service": {"port": 9000}, "networkPolicy": {"enabled": True, "ingress": {"from": [peer]}}},
    )
    policy = named(docs, "NetworkPolicy", "test-spark-on-k8s")["spec"]
    assert policy["policyTypes"] == ["Ingress"]
    assert "egress" not in policy
    [rule] = policy["ingress"]
    assert rule["ports"] == [{"port": 9000, "protocol": "TCP"}]
    assert rule["from"][1:] == [peer]
    # The policy selects, and admits, the API pods and the chart's helm test pod.
    test_pod = next(item for item in docs if item["kind"] == "Pod")
    for labels in (api_pod(docs)["metadata"]["labels"], test_pod["metadata"]["labels"]):
        assert selects(policy["podSelector"]["matchLabels"], labels)
        assert selects(rule["from"][0]["podSelector"]["matchLabels"], labels)
    # Without a namespace selector, the release peer is limited to the release namespace.
    assert list(rule["from"][0]) == ["podSelector"]


@pytest.mark.parametrize(
    "namespaces,selector",
    [
        (
            "",
            {
                "matchExpressions": [
                    {"key": "kubernetes.io/metadata.name", "operator": "In", "values": ["control"]}
                ]
            },
        ),
        (
            " analytics, batch,analytics",
            {
                "matchExpressions": [
                    {"key": "kubernetes.io/metadata.name", "operator": "In", "values": ["analytics", "batch"]}
                ]
            },
        ),
        ("*", {}),
    ],
)
def test_network_policy_egress_follows_the_api_namespace_scope(tmp_path, namespaces, selector):
    api_server = {"ipBlock": {"cidr": "172.18.0.2/32"}}
    extra = {"to": [{"ipBlock": {"cidr": "10.20.0.0/16"}}], "ports": [{"port": 8443}]}
    values = {
        "namespaces": namespaces,
        "networkPolicy": {
            "enabled": True,
            "egress": {"enabled": True, "kubernetesApi": {"to": [api_server]}, "extraRules": [extra]},
        },
    }
    policy = named(resources(tmp_path, values), "NetworkPolicy", "test-spark-on-k8s")["spec"]
    assert policy["policyTypes"] == ["Ingress", "Egress"]
    dns, kubernetes, drivers, release, *rest = policy["egress"]
    assert dns["to"][0]["podSelector"] == {"matchLabels": {"k8s-app": "kube-dns"}}
    assert {(port["port"], port["protocol"]) for port in dns["ports"]} == {(53, "UDP"), (53, "TCP")}
    assert kubernetes == {
        "to": [api_server],
        "ports": [{"port": 443, "protocol": "TCP"}, {"port": 6443, "protocol": "TCP"}],
    }
    assert drivers["to"] == [
        {"podSelector": {"matchLabels": {"spark-role": "driver"}}, "namespaceSelector": selector}
    ]
    assert drivers["ports"] == [{"port": 4040, "protocol": "TCP"}]
    assert release["ports"] == [{"port": 8000, "protocol": "TCP"}]
    assert rest == [extra]


def test_network_policy_egress_reaches_the_history_server_and_optional_dns_peers(tmp_path):
    values = {
        "sparkHistory": HISTORY,
        "networkPolicy": {"enabled": True, "egress": {"enabled": True, "dns": {"to": []}}},
    }
    docs = resources(tmp_path, values)
    egress = named(docs, "NetworkPolicy", "test-spark-on-k8s")["spec"]["egress"]
    # No API server peers: no rule rather than a rule open to every destination.
    assert not any(port["port"] in (443, 6443) for rule in egress for port in rule["ports"])
    # Empty DNS peers allow port 53 to any destination, for example a node-local cache.
    assert "to" not in egress[0] and egress[0]["ports"][0]["port"] == 53
    history = egress[-1]
    assert history["ports"] == [{"port": 18080, "protocol": "TCP"}]
    assert selects(history["to"][0]["podSelector"]["matchLabels"], history_pod(docs)["metadata"]["labels"])
    # The History Server needs its own flag for its ingress policy.
    assert len(kinds(docs, "NetworkPolicy")) == 1


def test_history_network_policy_admits_the_api_and_listed_peers(tmp_path):
    peer = {"ipBlock": {"cidr": "10.0.0.0/8"}}
    history = {**HISTORY, "networkPolicy": {"enabled": True, "ingress": {"from": [peer]}}}
    assert not kinds(
        resources(tmp_path, {"sparkHistory": {"networkPolicy": {"enabled": True}}}), "NetworkPolicy"
    )
    docs = resources(tmp_path, {"sparkHistory": history})
    policy = named(docs, "NetworkPolicy", "test-spark-on-k8s-history")["spec"]
    assert policy["policyTypes"] == ["Ingress"]
    [rule] = policy["ingress"]
    assert rule["ports"] == [{"port": 18080, "protocol": "TCP"}]
    assert selects(policy["podSelector"]["matchLabels"], history_pod(docs)["metadata"]["labels"])
    assert not selects(policy["podSelector"]["matchLabels"], api_pod(docs)["metadata"]["labels"])
    assert selects(rule["from"][0]["podSelector"]["matchLabels"], api_pod(docs)["metadata"]["labels"])
    assert rule["from"][1:] == [peer]
    service = named(docs, "Service", "test-spark-on-k8s-history")
    assert service["spec"]["ports"][0]["targetPort"] == 18080


def test_install_notes_warn_when_network_policy_admits_or_reaches_too_little(tmp_path):
    kubeconfig = tmp_path / "kubeconfig"
    kubeconfig.write_text("")  # Client-side dry run; never read the caller's cluster context.

    def notes(values):
        path = tmp_path / "notes-values.yaml"
        path.write_text(yaml.safe_dump(values))
        result = subprocess.run(
            [HELM, "install", "console", str(CHART), "--dry-run=client", "-f", str(path)],
            capture_output=True,
            text=True,
            env={**os.environ, "KUBECONFIG": str(kubeconfig)},
        )
        assert result.returncode == 0, result.stderr
        return result.stdout.partition("NOTES:")[2]

    peer = [{"podSelector": {"matchLabels": {"app": "gateway"}}}]
    assert "networkPolicy" not in notes({})
    assert "networkPolicy.ingress.from" in notes({"networkPolicy": {"enabled": True}})
    assert "networkPolicy" not in notes({"networkPolicy": {"enabled": True, "ingress": {"from": peer}}})
    egress = {"enabled": True, "ingress": {"from": peer}, "egress": {"enabled": True}}
    assert "kubernetesApi.to is empty" in notes({"networkPolicy": egress})
    egress["egress"]["kubernetesApi"] = {"to": [{"ipBlock": {"cidr": "172.18.0.2/32"}}]}
    assert "networkPolicy" not in notes({"networkPolicy": egress})


@pytest.mark.parametrize(
    "values,expected",
    [
        ({}, {"maxUnavailable": 1}),
        ({"replicaCount": 2, "podDisruptionBudget": {"minAvailable": 1}}, {"minAvailable": 1}),
        ({"replicaCount": 3, "podDisruptionBudget": {"minAvailable": "50%"}}, {"minAvailable": "50%"}),
        ({"podDisruptionBudget": {"maxUnavailable": "10%"}}, {"maxUnavailable": "10%"}),
        (
            {"autoscaling": {"enabled": True, "minReplicas": 2}, "podDisruptionBudget": {"minAvailable": 1}},
            {"minAvailable": 1},
        ),
    ],
)
def test_pod_disruption_budget_renders_one_limit_for_the_api_pods(tmp_path, values, expected):
    values = {**values, "podDisruptionBudget": {"enabled": True, **values.get("podDisruptionBudget", {})}}
    docs = resources(tmp_path, values)
    [budget] = kinds(docs, "PodDisruptionBudget")
    assert budget["metadata"]["name"] == "test-spark-on-k8s"
    assert {key: value for key, value in budget["spec"].items() if key != "selector"} == expected
    assert selects(budget["spec"]["selector"]["matchLabels"], api_pod(docs)["metadata"]["labels"])


@pytest.mark.parametrize(
    "replicas,budget,message",
    [
        ({}, {"minAvailable": 1}, "minAvailable 1 permits no voluntary disruption with 1 replica(s)"),
        ({"replicaCount": 3}, {"maxUnavailable": 0}, "maxUnavailable 0 permits"),
        ({"replicaCount": 3}, {"maxUnavailable": "0%"}, "maxUnavailable 0% permits"),
        # Kubernetes rounds 60% of two pods up to two.
        ({"replicaCount": 2}, {"minAvailable": "60%"}, "minAvailable 60% permits"),
        ({"replicaCount": 3}, {"minAvailable": 3}, "minAvailable 3 permits"),
        # Autoscaling can scale down to minReplicas.
        (
            {"replicaCount": 5, "autoscaling": {"enabled": True, "minReplicas": 2}},
            {"minAvailable": 2},
            "minAvailable 2 permits no voluntary disruption with 2 replica(s)",
        ),
    ],
)
def test_pod_disruption_budget_that_blocks_node_drains_is_rejected(tmp_path, replicas, budget, message):
    stderr = rejected(tmp_path, {**replicas, "podDisruptionBudget": {"enabled": True, **budget}})
    assert "podDisruptionBudget." + message in stderr and "blocks node drains" in stderr
    # The History Server applies the same rule to its own replica settings.
    history = {**HISTORY, **replicas, "podDisruptionBudget": {"enabled": True, **budget}}
    stderr = rejected(tmp_path, {"sparkHistory": history})
    assert "sparkHistory.podDisruptionBudget." + message in stderr


def test_history_pod_disruption_budget_uses_history_replicas_and_selector(tmp_path):
    budget = {"enabled": True, "minAvailable": 1}
    assert not kinds(
        resources(tmp_path, {"sparkHistory": {"podDisruptionBudget": budget}}), "PodDisruptionBudget"
    )
    history = {**HISTORY, "replicaCount": 2, "podDisruptionBudget": budget}
    docs = resources(tmp_path, {"sparkHistory": history})
    [budget] = kinds(docs, "PodDisruptionBudget")
    assert budget["metadata"]["name"] == "test-spark-on-k8s-history"
    assert budget["spec"]["minAvailable"] == 1
    assert selects(budget["spec"]["selector"]["matchLabels"], history_pod(docs)["metadata"]["labels"])
    assert not selects(budget["spec"]["selector"]["matchLabels"], api_pod(docs)["metadata"]["labels"])


def test_service_monitor_requires_its_api_and_scrapes_only_the_api_service(tmp_path):
    values = {
        "sparkHistory": HISTORY,
        "metrics": {
            "enabled": True,
            "serviceMonitor": {
                "enabled": True,
                "interval": "30s",
                "scrapeTimeout": "10s",
                "labels": {"release": "prometheus"},
            },
        },
    }
    stderr = rejected(tmp_path, values)
    assert "monitoring.coreos.com/v1" in stderr and "--api-versions" in stderr
    docs = resources(tmp_path, values, extra=SERVICE_MONITOR_API)
    monitor = named(docs, "ServiceMonitor", "test-spark-on-k8s")
    assert monitor["metadata"]["labels"]["release"] == "prometheus"
    assert monitor["spec"]["namespaceSelector"] == {"matchNames": ["control"]}
    [endpoint] = monitor["spec"]["endpoints"]
    assert endpoint["port"] == "http" and endpoint["path"] == "/metrics"
    assert (endpoint["interval"], endpoint["scrapeTimeout"]) == ("30s", "10s")
    assert "authorization" not in endpoint
    # Both Services carry the selector labels, so the scrape keeps only the API Service's name.
    services = kinds(docs, "Service")
    selector = monitor["spec"]["selector"]["matchLabels"]
    assert all(selects(selector, service["metadata"]["labels"]) for service in services)
    [keep] = endpoint["relabelings"]
    assert keep["action"] == "keep" and keep["sourceLabels"] == ["__meta_kubernetes_service_name"]
    api_service = named(docs, "Service", "test-spark-on-k8s")
    assert keep["regex"] == api_service["metadata"]["name"]
    assert any(port["name"] == endpoint["port"] for port in api_service["spec"]["ports"])
    assert environment(docs)["SPARK_ON_K8S_METRICS_ENABLED"] == "true"


def test_service_monitor_authenticates_with_the_remote_token_secret(tmp_path):
    values = {
        "metrics": {"enabled": True, "serviceMonitor": {"enabled": True}},
        "remote": {"enabled": True, "existingSecret": "api-remote", "tokenKey": "operator-token"},
    }
    monitor = named(
        resources(tmp_path, values, extra=SERVICE_MONITOR_API), "ServiceMonitor", "test-spark-on-k8s"
    )
    [endpoint] = monitor["spec"]["endpoints"]
    assert endpoint["authorization"] == {
        "type": "Bearer",
        "credentials": {"name": "api-remote", "key": "operator-token"},
    }
    assert "interval" not in endpoint and "scrapeTimeout" not in endpoint


def test_workers_value_sets_the_api_worker_count_unless_env_overrides_it(tmp_path):
    assert environment(resources(tmp_path, {"workers": 2}))["SPARK_ON_K8S_API_WORKERS"] == "2"
    override = {"name": "SPARK_ON_K8S_API_WORKERS", "value": "8"}
    docs = resources(tmp_path, {"workers": 2, "env": [override]})
    names = [item["name"] for item in api_pod(docs)["spec"]["containers"][0]["env"]]
    assert names.count("SPARK_ON_K8S_API_WORKERS") == 1
    assert environment(docs)["SPARK_ON_K8S_API_WORKERS"] == "8"


def test_remote_block_mounts_only_the_existing_secret_keys_and_sets_submission_env(tmp_path):
    remote = {
        "enabled": True,
        "existingSecret": "api-remote",
        "tokenKey": "operator-token",
        "policyKey": "submission-policy.json",
        "mountPath": "/run/remote",
    }
    docs = resources(tmp_path, {"remote": remote})
    env = environment(docs)
    assert env["SPARK_ON_K8S_SUBMISSION_ENABLED"] == "true"
    assert env["SPARK_ON_K8S_SUBMISSION_TOKEN_FILE"] == "/run/remote/token"
    assert env["SPARK_ON_K8S_SUBMISSION_POLICY_FILE"] == "/run/remote/policy.json"
    spec = api_pod(docs)["spec"]
    assert spec["volumes"] == [
        {
            "name": "remote-submission",
            "secret": {
                "secretName": "api-remote",
                "items": [
                    {"key": "operator-token", "path": "token"},
                    {"key": "submission-policy.json", "path": "policy.json"},
                ],
            },
        }
    ]
    assert spec["containers"][0]["volumeMounts"] == [
        {"name": "remote-submission", "mountPath": "/run/remote", "readOnly": True}
    ]
    # The chart never creates the token Secret.
    assert not kinds(docs, "Secret")


def test_remote_block_keeps_env_overrides_and_other_volumes(tmp_path):
    values = {
        "remote": {"enabled": True, "existingSecret": "api-remote"},
        "env": [{"name": "SPARK_ON_K8S_SUBMISSION_POLICY_FILE", "value": "/etc/policy/approved.json"}],
        "volumes": [{"name": "policy", "configMap": {"name": "approved-policy"}}],
        "volumeMounts": [{"name": "policy", "mountPath": "/etc/policy"}],
    }
    docs = resources(tmp_path, values)
    entries = api_pod(docs)["spec"]["containers"][0]["env"]
    assert len(entries) == len({item["name"] for item in entries})
    env = environment(docs)
    assert env["SPARK_ON_K8S_SUBMISSION_POLICY_FILE"] == "/etc/policy/approved.json"
    assert env["SPARK_ON_K8S_SUBMISSION_TOKEN_FILE"] == "/etc/spark-on-k8s/remote/token"
    spec = api_pod(docs)["spec"]
    assert [volume["name"] for volume in spec["volumes"]] == ["remote-submission", "policy"]
    assert [mount["name"] for mount in spec["containers"][0]["volumeMounts"]] == [
        "remote-submission",
        "policy",
    ]


def test_chart_integration_remote_values_use_the_remote_block(tmp_path):
    result = invoke(tmp_path, {}, extra=["-f", str(REMOTE_FIXTURE)])
    assert result.returncode == 0, result.stderr
    docs = [item for item in yaml.safe_load_all(result.stdout) if item]
    env = environment(docs)
    assert env["SPARK_ON_K8S_SUBMISSION_ENABLED"] == "true"
    volume = api_pod(docs)["spec"]["volumes"][0]
    assert volume["secret"]["secretName"] == "remote-test"
    assert {item["key"] for item in volume["secret"]["items"]} == {"token", "policy.json"}


def test_history_probes_target_the_ui_port_and_can_be_replaced_or_removed(tmp_path):
    docs = resources(tmp_path, {"sparkHistory": HISTORY})
    container = history_pod(docs)["spec"]["containers"][0]
    service = named(docs, "Service", "test-spark-on-k8s-history")
    for probe in ("readinessProbe", "livenessProbe"):
        assert container[probe]["httpGet"] == {"path": "/api/v1/version", "port": 18080}
        assert container[probe]["httpGet"]["port"] == service["spec"]["ports"][0]["targetPort"]
    assert container["livenessProbe"]["failureThreshold"] * container["livenessProbe"]["periodSeconds"] >= 60
    history = {
        **HISTORY,
        "readinessProbe": {"httpGet": None, "tcpSocket": {"port": 18080}},
        "livenessProbe": None,
    }
    container = history_pod(resources(tmp_path, {"sparkHistory": history}))["spec"]["containers"][0]
    assert container["readinessProbe"]["tcpSocket"] == {"port": 18080}
    assert "httpGet" not in container["readinessProbe"]
    assert "livenessProbe" not in container


@pytest.mark.parametrize(
    "values,error",
    [
        ({"workers": 0}, "workers"),
        ({"workers": 65}, "workers"),
        ({"workers": "4"}, "workers"),
        ({"podDisruptionBudget": {"minAvailable": 1, "maxUnavailable": 1}}, "maxUnavailable"),
        ({"podDisruptionBudget": {"maxUnavailable": "half"}}, "maxUnavailable"),
        ({"podDisruptionBudget": {"minAvailable": -1}}, "minAvailable"),
        ({"sparkHistory": {"podDisruptionBudget": {"minAvailable": "101%"}}}, "minAvailable"),
        ({"metrics": {"serviceMonitor": {"enabled": True}}}, "metrics"),
        ({"metrics": {"serviceMonitor": {"interval": "30 seconds"}}}, "interval"),
        ({"networkPolicy": {"ingress": {"from": {"podSelector": {}}}}}, "from"),
        ({"networkPolicy": {"egress": {"kubernetesApi": {"ports": [0]}}}}, "ports"),
        ({"sparkHistory": {"readinessProbe": "/api/v1/version"}}, "readinessProbe"),
        ({"remote": {"enabled": True}}, "existingSecret"),
        ({"remote": {"existingSecret": "Not_A_Secret"}}, "existingSecret"),
        ({"remote": {"tokenKey": "token/file"}}, "tokenKey"),
        ({"remote": {"mountPath": "relative/path"}}, "mountPath"),
    ],
)
@pytest.mark.parametrize("command", ["lint", "template"])
def test_invalid_operations_values_are_rejected_before_install(tmp_path, values, error, command):
    result = invoke(tmp_path, values, command=command)
    assert result.returncode != 0, result.stdout
    assert error in result.stdout + result.stderr


@pytest.mark.parametrize(
    "values,message",
    [
        (
            {
                "remote": {"enabled": True, "existingSecret": "api-remote"},
                "volumes": [{"name": "remote-submission"}],
            },
            "reserved volume name",
        ),
        (
            {
                "remote": {"enabled": True, "existingSecret": "api-remote"},
                "volumeMounts": [{"name": "remote-submission", "mountPath": "/x"}],
            },
            "reserved volume name",
        ),
    ],
)
def test_reserved_remote_volume_name_is_rejected_during_render(tmp_path, values, message):
    assert message in rejected(tmp_path, values)
