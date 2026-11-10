from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
import yaml
from click.testing import CliRunner
from kubernetes import client as k8s
from kubernetes.client.exceptions import ApiException
from spark_on_k8s_dbt.api import BoundedKubernetesClientManager
from spark_on_k8s_dbt.check import CHECK_IDS, REQUIRED_ACCESS
from spark_on_k8s_dbt.cli import main
from urllib3.exceptions import MaxRetryError, NewConnectionError


def write_project(tmp_path, *, output=None, target="dev", project_profile="analytics", **compute):
    kube = tmp_path / "kubeconfig"
    kube.touch()
    settings = {
        "kube_config_file": str(kube),
        "kube_context": "test",
        "image": "spark:test",
        "server_jar": "local:///opt/spark/jars/server.jar",
        "namespace": "spark",
        "service_account": "driver",
        "spark_conf": {
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.warehouse.dir": "hdfs://h/warehouse",
            "spark.hadoop.hive.metastore.uris": "thrift://h:9083",
        },
        **compute,
    }
    output = output or {"type": "spark_on_k8s", "method": "connect", "schema": "analytics"}
    output.setdefault("compute", settings)
    profile = {"outputs": {"dev": output}}
    if target is not None:
        profile["target"] = target
    (tmp_path / "profiles.yml").write_text(yaml.safe_dump({"analytics": profile}))
    (tmp_path / "dbt_project.yml").write_text(
        yaml.safe_dump({"name": "analytics", "version": "1.0", "profile": project_profile})
    )
    return tmp_path


def allowed(body):
    return SimpleNamespace(status=SimpleNamespace(allowed=True, reason=None, evaluation_error=None))


@pytest.fixture
def cluster(monkeypatch):
    """Read-only API doubles: any create, patch or delete call fails the test."""
    raw = Mock()
    raw.configuration.host = "https://cluster.test"
    managers = []

    def create_client(manager):
        managers.append(manager)
        return raw

    core = Mock(spec_set=["read_namespace", "read_namespaced_service_account"])
    core.read_namespace.return_value = SimpleNamespace(
        metadata=SimpleNamespace(deletion_timestamp=None), status=SimpleNamespace(phase="Active")
    )
    authorization = Mock(spec_set=["create_self_subject_access_review"])
    authorization.create_self_subject_access_review.side_effect = allowed
    monkeypatch.setattr(BoundedKubernetesClientManager, "create_client", create_client)
    monkeypatch.setattr(k8s, "CoreV1Api", lambda client: core)
    monkeypatch.setattr(k8s, "AuthorizationV1Api", lambda client: authorization)
    monkeypatch.setattr("spark_on_k8s_dbt.check.shutil.which", lambda name: "/usr/bin/kubectl")
    return SimpleNamespace(raw=raw, managers=managers, core=core, authorization=authorization)


def check(directory, *arguments):
    result = CliRunner().invoke(
        main,
        ["check", "--project-dir", str(directory), "--profiles-dir", str(directory), *arguments],
        catch_exceptions=False,
    )
    return result


def check_json(directory, *arguments):
    result = check(directory, "--output", "json", *arguments)
    return result, json.loads(result.stdout)


def statuses(document):
    return {item["id"]: item["status"] for item in document["checks"]}


def reviewed(authorization):
    return [
        call.kwargs["body"].spec.resource_attributes
        for call in authorization.create_self_subject_access_review.call_args_list
    ]


def test_passing_check_reads_and_reviews_access_without_creating_resources(tmp_path, cluster):
    result, document = check_json(write_project(tmp_path))
    assert result.exit_code == 0, result.output
    assert [item["id"] for item in document["checks"]] == list(CHECK_IDS)
    assert set(statuses(document).values()) == {"pass"}
    assert {key: document[key] for key in ("schema_version", "operation", "status", "error")} == {
        "schema_version": 1,
        "operation": "dbt_check",
        "status": "pass",
        "error": None,
    }
    assert (document["profile"], document["target"], document["method"], document["namespace"]) == (
        "analytics",
        "dev",
        "connect",
        "spark",
    )
    # The adapter's own cluster selection, request budget and retry policy.
    [manager] = cluster.managers
    assert (manager.config_file, manager.context, manager.in_cluster) == (
        str((tmp_path / "kubeconfig").resolve()),
        "test",
        False,
    )
    cluster.core.read_namespace.assert_called_once_with("spark")
    cluster.core.read_namespaced_service_account.assert_called_once_with("driver", "spark")
    attributes = reviewed(cluster.authorization)
    assert [(item.verb, item.resource, item.subresource) for item in attributes] == [
        *((verb, resource, None) for verb, resource in REQUIRED_ACCESS),
        ("create", "pods", "portforward"),
    ]
    assert {item.namespace for item in attributes} == {"spark"}
    names = {(item.verb, item.resource): item.name for item in attributes}
    assert names[("get", "namespaces")] == "spark" and names[("get", "serviceaccounts")] == "driver"
    assert names[("create", "pods")] is None
    cluster.raw.close.assert_called_once_with()


def test_denied_access_and_missing_driver_account_fail_with_next_steps(tmp_path, cluster):
    def review(body):
        attributes = body.spec.resource_attributes
        denied = (attributes.verb, attributes.resource, attributes.subresource) in {
            ("delete", "pods", None),
            ("create", "pods", None),
        }
        reason = "RBAC: no rule" if denied else None
        return SimpleNamespace(
            status=SimpleNamespace(allowed=not denied, reason=reason, evaluation_error=None)
        )

    cluster.authorization.create_self_subject_access_review.side_effect = review
    cluster.core.read_namespaced_service_account.side_effect = ApiException(status=404, reason="Not Found")
    result, document = check_json(write_project(tmp_path))
    assert result.exit_code == 1
    assert document["status"] == "fail"
    failed = {item["id"]: item for item in document["checks"] if item["status"] == "fail"}
    assert set(failed) == {"service_account", "permission.create.pods", "permission.delete.pods"}
    assert (
        "driver service account spark/driver does not exist" in failed["service_account"]["message"].lower()
    )
    assert (
        failed["permission.delete.pods"]["message"]
        == "Denied: delete pods in namespace spark (RBAC: no rule)."
    )
    assert "delete on pods in namespace spark" in failed["permission.delete.pods"]["remediation"]
    assert statuses(document)["permission.create.services"] == "pass"


def test_text_output_lists_failures_with_remediation_and_the_limits_of_a_pass(tmp_path, cluster):
    cluster.core.read_namespace.side_effect = ApiException(status=403, reason="Forbidden")
    result = check(write_project(tmp_path))
    assert result.exit_code == 1
    lines = result.stdout.splitlines()
    assert lines[0] == "Managed dbt check for profile analytics, target dev; nothing was created."
    assert "FAIL namespace: Could not read namespace spark: Kubernetes API 403: Forbidden" in lines
    assert any(
        line.startswith("     Next: Grant the invoking identity get on namespaces/spark") for line in lines
    )
    assert "PASS permission.create.pods: Allowed: create pods in namespace spark." in lines
    assert "1 failed, 18 passed, 0 skipped." in lines
    assert lines[-1].startswith("A passing check does not prove that dbt will succeed")


def test_text_output_collapses_checks_skipped_for_one_reason(tmp_path, cluster):
    cluster.core.read_namespace.side_effect = ApiException(status=401, reason="Unauthorized")
    result = check(write_project(tmp_path))
    assert result.exit_code == 1
    assert "SKIP service_account and 12 more: Skipped: the Kubernetes API did not answer." in result.stdout
    assert "0 failed" not in result.stdout and "1 failed, 5 passed, 13 skipped." in result.stdout


@pytest.mark.parametrize(
    "error",
    [
        MaxRetryError(None, "/api/v1/namespaces/spark", NewConnectionError(None, "Connection refused")),
        ApiException(status=401, reason="Unauthorized"),
        TimeoutError("Managed dbt Kubernetes request deadline expired"),
    ],
)
def test_unusable_cluster_access_skips_dependent_checks(tmp_path, cluster, error):
    cluster.core.read_namespace.side_effect = error
    result, document = check_json(write_project(tmp_path))
    assert result.exit_code == 1
    results = statuses(document)
    assert results["kubernetes_config"] == "pass" and results["namespace"] == "fail"
    assert {results[check_id] for check_id in CHECK_IDS[CHECK_IDS.index("service_account") :]} == {"skip"}
    cluster.core.read_namespaced_service_account.assert_not_called()
    cluster.authorization.create_self_subject_access_review.assert_not_called()
    cluster.raw.close.assert_called_once_with()


@pytest.mark.parametrize(
    "response, message",
    [
        (ApiException(status=404, reason="Not Found"), "Namespace spark does not exist."),
        (
            SimpleNamespace(metadata=SimpleNamespace(deletion_timestamp="2026-11-09T00:00:00Z")),
            "Namespace spark is terminating.",
        ),
    ],
)
def test_unusable_namespace_skips_account_and_access_reviews(tmp_path, cluster, response, message):
    if isinstance(response, Exception):
        cluster.core.read_namespace.side_effect = response
    else:
        cluster.core.read_namespace.return_value = response
    result, document = check_json(write_project(tmp_path))
    assert result.exit_code == 1
    namespace = next(item for item in document["checks"] if item["id"] == "namespace")
    assert namespace["message"] == message
    assert statuses(document)["service_account"] == statuses(document)["permission.get.pods"] == "skip"
    cluster.core.read_namespaced_service_account.assert_not_called()
    cluster.authorization.create_self_subject_access_review.assert_not_called()


def test_unverifiable_access_review_is_not_reported_as_allowed(tmp_path, cluster):
    cluster.authorization.create_self_subject_access_review.side_effect = ApiException(
        status=403, reason="Forbidden"
    )
    result, document = check_json(write_project(tmp_path))
    assert result.exit_code == 1
    reviews = [item for item in document["checks"] if item["id"].startswith("permission.")]
    assert {item["status"] for item in reviews} == {"fail"}
    assert reviews[0]["message"].startswith("Could not verify get namespaces/spark in namespace spark")


def test_invalid_adapter_profile_fails_with_the_adapter_message_before_any_cluster_access(tmp_path, cluster):
    output = {"type": "spark_on_k8s", "method": "connect", "schema": "analytics", "host": "external"}
    result, document = check_json(write_project(tmp_path, output=output))
    assert result.exit_code == 1
    profile = document["checks"][0]
    assert profile["status"] == "fail" and "Do not set host or port" in profile["message"]
    assert set(statuses(document).values()) == {"fail", "skip"}
    assert cluster.managers == []


def test_invalid_compute_fails_offline_and_still_checks_client_libraries(tmp_path, cluster):
    directory = write_project(tmp_path, spark_conf={"spark.sql.catalogImplementation": "hive"})
    result, document = check_json(directory)
    assert result.exit_code == 1
    results = statuses(document)
    assert (results["profile"], results["compute"], results["client_libraries"]) == ("pass", "fail", "pass")
    assert results["kubernetes_config"] == "skip"
    assert "spark.sql.warehouse.dir" in document["checks"][1]["message"]
    assert cluster.managers == []


def test_missing_kubeconfig_is_a_compute_failure(tmp_path, cluster):
    directory = write_project(tmp_path, kube_config_file=str(tmp_path / "missing"))
    result, document = check_json(directory)
    assert result.exit_code == 1
    assert statuses(document)["compute"] == "fail"
    assert str(tmp_path / "missing") in document["checks"][1]["message"]


def test_thrift_compute_uses_the_server_creation_validation(tmp_path, cluster):
    output = {"type": "spark_on_k8s", "method": "thrift", "schema": "analytics"}
    directory = write_project(
        tmp_path,
        output=output,
        spark_conf={
            "spark.sql.catalogImplementation": "hive",
            "spark.sql.warehouse.dir": "hdfs://h/warehouse",
            "spark.hadoop.hive.metastore.uris": "thrift://h:9083",
            "spark.hadoop.hive.server2.thrift.port": "10001",
        },
    )
    result, document = check_json(directory)
    assert result.exit_code == 1
    assert "cannot override managed Thrift setting" in document["checks"][1]["message"]


def test_in_cluster_client_needs_neither_kubectl_nor_port_forward(tmp_path, cluster, monkeypatch):
    monkeypatch.setattr("spark_on_k8s_dbt.check.shutil.which", lambda name: None)
    directory = write_project(tmp_path, in_cluster=True, kube_config_file=None, kube_context=None)
    result, document = check_json(directory)
    assert result.exit_code == 0, result.output
    results = statuses(document)
    assert results["kubectl"] == "skip" and results["permission.create.pods/portforward"] == "skip"
    assert cluster.managers[0].in_cluster is True
    assert ("create", "pods", "portforward") not in {
        (item.verb, item.resource, item.subresource) for item in reviewed(cluster.authorization)
    }


def test_local_client_without_kubectl_fails(tmp_path, cluster, monkeypatch):
    monkeypatch.setattr("spark_on_k8s_dbt.check.shutil.which", lambda name: None)
    result, document = check_json(write_project(tmp_path))
    assert result.exit_code == 1
    assert statuses(document)["kubectl"] == "fail"


@pytest.mark.parametrize(
    "method, module", [("connect", "pyspark.sql.connect.session"), ("thrift", "pyhive.hive")]
)
def test_missing_transport_client_fails(tmp_path, cluster, monkeypatch, method, module):
    import importlib

    original = importlib.import_module

    def import_module(name, *args):
        if name == module:
            raise ImportError(f"No module named {name!r}")
        return original(name, *args)

    monkeypatch.setattr("spark_on_k8s_dbt.check.importlib.import_module", import_module)
    output = {"type": "spark_on_k8s", "method": method, "schema": "analytics"}
    result, document = check_json(write_project(tmp_path, output=output))
    assert result.exit_code == 1
    libraries = document["checks"][2]
    assert libraries["status"] == "fail" and module in libraries["message"]


def test_profile_resolution_matches_dbt_rendering_and_keeps_dbt_events_off_stdout(
    tmp_path, cluster, monkeypatch
):
    monkeypatch.setenv("CHECK_PROFILE_NAME", "analytics")
    directory = write_project(tmp_path, target=None, project_profile="{{ env_var('CHECK_PROFILE_NAME') }}")
    # Without a target, dbt selects "default" and reports it as an event.
    result, document = check_json(directory)
    assert "target not specified in profile 'analytics', using 'default'" in result.stderr
    assert "does not have a target named 'default'" in document["checks"][0]["message"]
    profiles = yaml.safe_load((directory / "profiles.yml").read_text())
    profiles["analytics"]["target"] = "{{ var('selected') }}"
    (directory / "profiles.yml").write_text(yaml.safe_dump(profiles))
    result, document = check_json(directory, "--vars", "{selected: dev}")
    assert result.exit_code == 0, result.output
    assert document["target"] == "dev"


def test_explicit_profile_does_not_need_a_project_and_other_adapters_are_refused(tmp_path, cluster):
    output = {"type": "spark", "method": "thrift", "host": "spark.test", "port": 10000, "schema": "analytics"}
    directory = write_project(tmp_path, output=output)
    (directory / "dbt_project.yml").unlink()
    result, document = check_json(directory, "--profile", "analytics", "--target", "dev")
    assert result.exit_code == 1
    assert document["checks"][0]["message"] == "Target dev uses adapter type spark, not spark_on_k8s."
    result = check(directory)
    assert result.exit_code == 1 and "dbt_project.yml" in result.stdout


def test_missing_profiles_file_and_invalid_vars(tmp_path):
    result, document = check_json(tmp_path, "--profile", "analytics")
    assert result.exit_code == 1
    assert document["checks"][0]["message"].startswith(f"No profiles.yml in {tmp_path}")
    result = check(tmp_path, "--vars", "[1, 2]")
    assert result.exit_code == 2 and "YAML mapping" in result.output


@pytest.mark.parametrize("timeout", ["0", "301", "inf", "nan"])
def test_timeout_that_is_not_a_positive_finite_number_is_an_invalid_option(tmp_path, timeout):
    # NaN compares false against both range bounds, so the range alone accepts it.
    result = check(tmp_path, "--output", "json", "--timeout", timeout)
    assert result.exit_code == 2 and "--timeout" in result.stderr
    assert result.stdout == ""


def test_interrupt_reports_partial_results_in_one_document(tmp_path, cluster):
    cluster.core.read_namespace.side_effect = KeyboardInterrupt
    result, document = check_json(write_project(tmp_path))
    assert result.exit_code == 130
    assert document["error"]["code"] == "interrupted"
    assert [item["id"] for item in document["checks"]] == list(CHECK_IDS)
    assert statuses(document)["kubernetes_config"] == "pass"
    assert statuses(document)["namespace"] == "skip"


def test_wrapper_reports_its_version_and_accepts_short_help():
    from importlib.metadata import version

    from spark_on_k8s_dbt.cli import main

    assert CliRunner().invoke(main, ["--version"]).output.strip().endswith(version("dbt-spark-on-k8s"))
    assert CliRunner().invoke(main, ["-h"]).exit_code == 0
