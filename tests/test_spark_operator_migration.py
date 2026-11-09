from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner
from spark_on_k8s.cli.job import job_cli
from spark_on_k8s.jobs import JobSpec
from spark_on_k8s.spark_operator_migration import (
    STATUSES,
    ManifestError,
    assess_document,
    assess_file,
    assess_manifests,
)

FIXTURES = Path(__file__).parent / "fixtures" / "spark_operator"
SENTINELS = ("AKIAFIXTURESENTINEL1", "fixture-secret-sentinel-2", "fixture-password-sentinel-3")


def load(name: str) -> dict:
    return yaml.safe_load((FIXTURES / name).read_text())


def application(name: str) -> dict:
    return assess_file(FIXTURES / name)["applications"][0]


def findings(result: dict) -> dict[str, dict]:
    return {finding["path"]: finding for finding in result["findings"]}


def leaves(value, path=()):
    if isinstance(value, dict) and value:
        for key, item in value.items():
            yield from leaves(item, (*path, str(key)))
    elif isinstance(value, list) and value:
        for index, item in enumerate(value):
            yield from leaves(item, (*path, str(index)))
    else:
        yield path


def segments(path: str) -> tuple[str, ...]:
    return tuple(field or key for field, key in re.findall(r"([^.\[\]]+)|\[([^\]]*)\]", path))


def plan_resources(draft: dict) -> dict[str, dict]:
    with patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client") as connect:
        plan = JobSpec.from_mapping(copy.deepcopy(draft)).plan()
        connect.assert_not_called()
    resources = {resource["kind"]: resource for resource in plan.resources if resource["kind"] != "ConfigMap"}
    resources["ConfigMaps"] = [resource for resource in plan.resources if resource["kind"] == "ConfigMap"]
    return resources


def driver_conf(pod: dict) -> dict[str, str]:
    args = pod["spec"]["containers"][0]["args"]
    return dict(args[index + 1].split("=", 1) for index, arg in enumerate(args) if arg == "--conf")


@pytest.mark.parametrize("name", sorted(path.name for path in FIXTURES.glob("*.yaml")))
def test_every_source_field_has_exactly_one_accounting_status(name):
    result = application(name)
    paths = [segments(finding["path"]) for finding in result["findings"]]
    assert len(paths) == len(set(paths))
    for leaf in leaves(load(name)):
        assert any(leaf[: len(path)] == path for path in paths), f"{'.'.join(leaf)} has no finding"
    for finding in result["findings"]:
        assert finding["status"] in STATUSES
        # The final accounting pass is a safety net; fixtures must be handled by field rules.
        assert "did not account" not in (finding["note"] or "")
        if finding["status"] in {"approximated", "ignored"}:
            assert finding["note"]
        if finding["status"] == "unsupported":
            assert finding["note"] and finding["alternative"]
    assert result["counts"] == {
        status: sum(item["status"] == status for item in result["findings"]) for status in STATUSES
    }


def test_empty_sections_and_nested_unknown_fields_are_accounted_for():
    document = load("pyspark-pi.yaml")
    document["spec"].update(sparkConf={}, deps={"jars": [], "futureDeps": ["x"]}, volumes=[])
    document["spec"]["driver"]["env"] = []
    document["spec"]["dynamicAllocation"] = {"enabled": False, "futureMode": "fast"}
    result = assess_document(document)
    by_path = findings(result)
    for path in ("spec.sparkConf", "spec.deps.jars", "spec.volumes", "spec.driver.env"):
        assert by_path[path]["status"] == "ignored"
    for path in ("spec.deps.futureDeps", "spec.dynamicAllocation.futureMode"):
        assert by_path[path]["status"] == "unsupported"
        assert by_path[path]["note"].startswith("Unrecognized")
    assert result["migration_ready"] is False


def test_supported_fixture_produces_a_validated_job_and_expected_plan():
    result = application("pyspark-pi.yaml")
    assert result["migration_ready"] is True
    assert result["draft_error"] is None
    assert result["counts"]["unsupported"] == 0
    assert findings(result)["spec.driver.coreLimit"]["status"] == "approximated"
    assert result["draft"]["spec"] == {
        "image": "registry.example.com/spark/pyspark:3.5.9",
        "app_path": "local:///opt/spark/examples/src/main/python/pi.py",
        "app_arguments": ["100"],
        "namespace": "spark-jobs",
        "service_account": "spark-driver",
        "image_pull_policy": "IfNotPresent",
        "driver_resources": {"cpu": 1, "memory": 512, "memory_overhead": 384},
        "executor_resources": {"cpu": 1, "memory": 512, "memory_overhead": 384},
        "executor_instances": {"initial": 2},
        "driver_labels": {"version": "3.5.9", "team": "analytics"},
        "executor_labels": {"version": "3.5.9", "team": "analytics"},
        "spark_conf": {"spark.sql.shuffle.partitions": "8"},
    }
    resources = plan_resources(result["draft"])
    pod = resources["Pod"]
    assert pod["metadata"]["namespace"] == "spark-jobs"
    assert pod["metadata"]["labels"]["team"] == "analytics"
    assert pod["spec"]["serviceAccountName"] == "spark-driver"
    container = pod["spec"]["containers"][0]
    assert container["image"] == "registry.example.com/spark/pyspark:3.5.9"
    assert container["resources"]["limits"] == {"cpu": "1", "memory": "896Mi"}
    assert container["args"][-2:] == ["local:///opt/spark/examples/src/main/python/pi.py", "100"]
    conf = driver_conf(pod)
    assert conf["spark.executor.instances"] == "2"
    assert conf["spark.executor.memoryOverhead"] == "384m"
    assert conf["spark.kubernetes.executor.label.team"] == "analytics"
    assert conf["spark.sql.shuffle.partitions"] == "8"


@pytest.mark.parametrize(
    "restart_policy",
    [
        {"type": "OnFailure", "onFailureRetries": 2},
        {"type": "Always"},
        {"type": "Never", "onFailureRetries": 1},
    ],
)
def test_an_unsupported_lifecycle_setting_is_never_migration_ready(restart_policy):
    document = load("pyspark-pi.yaml")
    document["spec"]["restartPolicy"] = restart_policy
    result = assess_document(document)
    assert result["draft_error"] is None
    assert result["migration_ready"] is False
    retry = [item for item in result["findings"] if item["path"].startswith("spec.restartPolicy")]
    assert any(
        item["status"] == "unsupported" and "Retry from the caller" in item["alternative"] for item in retry
    )


def test_scheduled_application_reports_the_schedule_and_drafts_its_template():
    result = application("scheduled.yaml")
    assert result["kind"] == "ScheduledSparkApplication"
    assert result["migration_ready"] is False
    by_path = findings(result)
    for field in ("schedule", "timeZone", "concurrencyPolicy", "successfulRunHistoryLimit"):
        assert by_path[f"spec.{field}"]["status"] == "unsupported"
    assert "CronJob" in by_path["spec.schedule"]["alternative"]
    assert by_path["spec.template.mainApplicationFile"]["target"] == "spec.app_path"
    assert result["draft"]["metadata"] == {"name": "nightly-report"}
    assert result["draft"]["spec"]["namespace"] == "reporting"
    assert result["draft"]["spec"]["executor_instances"] == {"initial": 2}
    assert any("spec.template only" in note for note in result["notes"])


def test_dependencies_allocation_and_executor_secret_references_translate():
    result = application("scala-with-deps.yaml")
    assert result["migration_ready"] is False
    spec = result["draft"]["spec"]
    assert spec["class_name"] == "com.example.orders.Aggregate"
    assert spec["packages"] == ["org.apache.hadoop:hadoop-aws:3.4.1"]
    assert spec["driver_resources"] == {"cpu": 1, "memory": 2048}
    assert spec["executor_instances"] == {"min": 1, "max": 6, "initial": 2}
    assert spec["spark_conf"]["spark.jars"] == "https://repo.example.com/libs/udf-helpers-1.4.jar"
    assert spec["spark_conf"]["spark.kubernetes.executor.secretKeyRef.WAREHOUSE_TOKEN"] == "warehouse:token"
    assert spec["spark_conf"]["spark.kubernetes.container.image.pullSecrets"] == "registry-credentials"
    assert spec["spark_conf"]["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert spec["driver_pod_template"]["spec"]["imagePullSecrets"] == [{"name": "registry-credentials"}]
    assert spec["driver_pod_template"]["spec"]["containers"] == [
        {"name": "driver", "env": [{"name": "LOG_LEVEL", "value": "INFO"}]}
    ]
    by_path = findings(result)
    assert by_path["spec.deps.jars[0]"]["status"] == "mapped"
    assert by_path["spec.deps.jars[1]"]["status"] == "unsupported"
    assert "local://" in by_path["spec.deps.jars[1]"]["alternative"]
    assert by_path["spec.restartPolicy"]["status"] == "unsupported"
    assert by_path["spec.sparkConf[spark.kubernetes.driver.secrets.db-tls]"]["status"] == "unsupported"
    assert by_path["spec.sparkConf[spark.driver.memory]"]["target"] == "spec.driver_resources.memory"
    driver = plan_resources(result["draft"])["Pod"]
    assert driver["spec"]["imagePullSecrets"] == [{"name": "registry-credentials"}]
    assert driver_conf(driver)["spark.dynamicAllocation.maxExecutors"] == "6"


def test_credentials_are_not_echoed_or_drafted(tmp_path):
    result = application("scala-with-deps.yaml")
    by_path = findings(result)
    for path in (
        "spec.sparkConf[spark.hadoop.fs.s3a.access.key]",
        "spec.hadoopConf[fs.s3a.secret.key]",
        "spec.driver.env[1]",
    ):
        assert by_path[path]["status"] == "unsupported"
        assert "Secret" in by_path[path]["alternative"]
    runner = CliRunner()
    file = str(FIXTURES / "scala-with-deps.yaml")
    outputs = [
        runner.invoke(job_cli, ["assess-operator", file, "--output", "json"]),
        runner.invoke(job_cli, ["assess-operator", file, "--draft-dir", str(tmp_path)]),
    ]
    draft = (tmp_path / "orders-aggregation.yaml").read_text()
    for text in [draft, *(item.stdout + item.stderr for item in outputs)]:
        assert not any(sentinel in text for sentinel in SENTINELS)
    assert all(item.exit_code == 0 for item in outputs)


@pytest.mark.parametrize(
    "spec_update, path",
    [
        ({"arguments": ["--password", "plain-arg-sentinel"]}, "spec.arguments"),
        ({"arguments": ["--token=plain-arg-sentinel"]}, "spec.arguments"),
        ({"mainApplicationFile": "https://user:plain-arg-sentinel@repo/app.py"}, "spec.mainApplicationFile"),
        (
            {"deps": {"pyFiles": ["s3a://bucket/lib.zip?X-Amz-Signature=plain-arg-sentinel"]}},
            "spec.deps.pyFiles",
        ),
        ({"executor": {"envVars": {"API_KEY": "plain-arg-sentinel"}}}, "spec.executor.envVars[API_KEY]"),
        ({"driver": {"javaOptions": "-Xss4m -Ddb.password=plain-arg-sentinel"}}, "spec.driver.javaOptions"),
        (
            {
                "driver": {
                    "sidecars": [{"name": "proxy", "image": "p", "env": [{"name": "TOKEN", "value": "x"}]}]
                }
            },
            "spec.driver.sidecars[0]",
        ),
    ],
)
def test_credential_patterns_in_other_fields_are_withheld(spec_update, path):
    document = load("pyspark-pi.yaml")
    for key, value in spec_update.items():
        if isinstance(value, dict) and isinstance(document["spec"].get(key), dict):
            document["spec"][key].update(value)
        else:
            document["spec"][key] = value
    result = assess_document(document)
    assert findings(result)[path]["status"] == "unsupported"
    assert "plain-arg-sentinel" not in json.dumps(result)
    assert result["migration_ready"] is False


def test_volumes_sidecars_and_operator_features_become_templates_or_findings():
    result = application("volumes-sidecars.yaml")
    by_path = findings(result)
    assert result["migration_ready"] is False
    assert by_path["spec.volumes[2]"]["status"] == "ignored"
    for path in ("spec.monitoring", "spec.sparkUIOptions", "spec.driver.futureOperatorField"):
        assert by_path[path]["status"] == "unsupported"
    assert by_path["spec.driver.futureOperatorField"]["note"].startswith("Unrecognized")
    assert by_path["spec.driver.sidecars[0]"]["status"] == "approximated"
    assert by_path["spec.driver.secrets[0]"]["status"] == "approximated"
    spec = result["draft"]["spec"]
    driver_template = spec["driver_pod_template"]["spec"]
    assert [container["name"] for container in driver_template["containers"]] == ["driver", "log-shipper"]
    assert {volume["name"] for volume in driver_template["volumes"]} == {
        "shared-data",
        "scratch",
        "feature-config-vol",
        "gcp-key-vol",
    }
    assert driver_template["securityContext"] == {"runAsNonRoot": True, "fsGroup": 185}
    executor_template = spec["executor_pod_template"]["spec"]
    assert executor_template["containers"][0] == {
        "name": "executor",
        "volumeMounts": [{"name": "shared-data", "mountPath": "/data"}],
    }
    assert "nodeAffinity" in executor_template["affinity"]
    assert spec["spark_conf"]["spark.kubernetes.executor.secrets.gcp-key"] == "/var/secrets/google"
    resources = plan_resources(result["draft"])
    pod = resources["Pod"]
    assert [container["name"] for container in pod["spec"]["containers"]] == ["driver", "log-shipper"]
    assert [container["name"] for container in pod["spec"]["initContainers"]] == ["fetch-model"]
    assert len(resources["ConfigMaps"]) == 1


def test_images_resources_names_and_conflicts():
    document = load("pyspark-pi.yaml")
    document["metadata"]["name"] = "Nightly.PI_Report"
    document["spec"]["driver"]["image"] = "registry.example.com/spark/driver:1"
    document["spec"]["executor"]["memory"] = "1536k"
    document["spec"]["sparkConf"]["spark.driver.memory"] = "1g"
    result = assess_document(document)
    by_path = findings(result)
    assert result["draft"]["metadata"]["name"] == "nightly-pi-report"
    assert "Renamed to nightly-pi-report" in by_path["metadata.name"]["note"]
    spec = result["draft"]["spec"]
    assert spec["image"] == "registry.example.com/spark/driver:1"
    assert spec["spark_conf"]["spark.kubernetes.executor.container.image"] == document["spec"]["image"]
    assert spec["executor_resources"]["memory"] == 2
    assert by_path["spec.executor.memory"]["status"] == "approximated"
    conflict = by_path["spec.sparkConf[spark.driver.memory]"]
    assert conflict["status"] == "unsupported"
    assert "spec.driver.memory" in conflict["note"]
    assert spec["driver_resources"]["memory"] == 512


def test_dynamic_allocation_starts_with_the_largest_count():
    document = load("pyspark-pi.yaml")
    document["spec"]["dynamicAllocation"] = {"enabled": True, "minExecutors": 1, "maxExecutors": 4}
    result = assess_document(document)
    assert result["draft"]["spec"]["executor_instances"] == {"min": 1, "max": 4, "initial": 2}
    assert findings(result)["spec.executor.instances"]["status"] == "approximated"
    document["spec"]["dynamicAllocation"] = {"enabled": False, "maxExecutors": 4}
    result = assess_document(document)
    assert result["draft"]["spec"]["executor_instances"] == {"initial": 2}
    assert findings(result)["spec.dynamicAllocation.maxExecutors"]["status"] == "ignored"


def test_a_draft_that_fails_offline_validation_is_not_emitted():
    document = load("pyspark-pi.yaml")
    del document["spec"]["image"]
    result = assess_document(document)
    assert result["draft"] is None
    assert result["draft_error"] == "spec.image is required."
    assert result["migration_ready"] is False
    document = load("pyspark-pi.yaml")
    document["spec"]["dynamicAllocation"] = {"enabled": True, "maxExecutors": 1}
    result = assess_document(document)
    assert result["draft"] is None
    assert "allocation bounds" in result["draft_error"]


def test_other_documents_are_skipped_and_reported():
    content = "\n---\n".join(
        [
            "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: settings}",
            (FIXTURES / "pyspark-pi.yaml").read_text(),
            "apiVersion: sparkoperator.k8s.io/v1beta1\nkind: SparkApplication\nmetadata: {name: old}",
        ]
    )
    result = assess_manifests(content)
    assert result["migration_ready"] is True
    assert [item["document"] for item in result["applications"]] == [2]
    assert [(item["document"], item["kind"]) for item in result["skipped"]] == [
        (1, "ConfigMap"),
        (3, "SparkApplication"),
    ]


@pytest.mark.parametrize(
    "content, message",
    [
        ("apiVersion: v1\nkind: ConfigMap\n", "No sparkoperator.k8s.io/v1beta2"),
        ("spec: [unclosed\npassword: secret-in-broken-yaml", "Invalid YAML at line"),
        ("a: &x {b: 1}\nc: *x\n", "aliases are not supported"),
        ("a: 1\na: 2\n", "unique strings"),
        ("- just\n- a list\n", "is not a mapping"),
        ("apiVersion: sparkoperator.k8s.io/v1beta2\nkind: SparkApplication\nspec: {}\n", "metadata.name"),
    ],
)
def test_unreadable_input_is_an_error(content, message):
    with pytest.raises(ManifestError, match=message) as error:
        assess_manifests(content)
    assert "secret-in-broken-yaml" not in str(error.value)


def test_cli_json_contract_and_exit_codes(tmp_path):
    runner = CliRunner()
    result = runner.invoke(
        job_cli, ["assess-operator", str(FIXTURES / "volumes-sidecars.yaml"), "--output", "json"]
    )
    assert result.exit_code == 0, result.output
    document = json.loads(result.stdout)
    assert document["schema_version"] == 1
    assert document["operation"] == "assess_operator"
    assert document["error"] is None
    assert document["migration_ready"] is False
    assert document["skipped"] == []
    assert set(document["applications"][0]) >= {
        "document",
        "kind",
        "name",
        "namespace",
        "migration_ready",
        "counts",
        "findings",
        "notes",
        "draft",
        "draft_error",
        "draft_file",
    }
    broken = tmp_path / "broken.yaml"
    broken.write_text("apiVersion: v1\nkind: Service\n")
    for output in ("json", "text"):
        failed = runner.invoke(job_cli, ["assess-operator", str(broken), "--output", output])
        assert failed.exit_code == 1
        if output == "json":
            error = json.loads(failed.stdout)
            assert error["applications"] is None
            assert error["error"]["code"] == "operation_error"
        else:
            assert failed.stdout == ""
            assert failed.stderr.startswith("Error: No sparkoperator.k8s.io/v1beta2")
    missing = runner.invoke(job_cli, ["assess-operator", str(tmp_path / "missing.yaml")])
    assert missing.exit_code == 2


def test_cli_text_report_and_draft_files(tmp_path):
    runner = CliRunner()
    file = str(FIXTURES / "pyspark-pi.yaml")
    result = runner.invoke(job_cli, ["assess-operator", file, "--draft-dir", str(tmp_path)])
    assert result.exit_code == 0, result.output
    assert result.stdout.startswith("SparkApplication spark-jobs/pyspark-pi (document 1): migration-ready")
    assert "APPROXIMATED spec.driver.coreLimit -> spec.driver_resources.cpu" in result.stdout
    draft = tmp_path / "pyspark-pi.yaml"
    content = draft.read_text()
    assert content.startswith("# DRAFT converted by `spark-on-k8s job assess-operator`")
    assert f"Draft written to {draft}" in result.stdout
    assert runner.invoke(job_cli, ["validate", str(draft)]).exit_code == 0
    again = runner.invoke(
        job_cli, ["assess-operator", file, "--draft-dir", str(tmp_path), "--output", "json"]
    )
    assert again.exit_code == 1
    assert "already exists" in json.loads(again.stdout)["error"]["message"]
    assert draft.read_text() == content
