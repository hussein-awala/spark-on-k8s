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


def test_cli_creates_a_missing_draft_directory(tmp_path):
    # The migration guide passes a new directory, as in `--draft-dir drafts/`.
    drafts = tmp_path / "drafts" / "nested"
    result = CliRunner().invoke(
        job_cli, ["assess-operator", str(FIXTURES / "pyspark-pi.yaml"), "--draft-dir", str(drafts)]
    )
    assert result.exit_code == 0, result.output
    assert (drafts / "pyspark-pi.yaml").is_file()


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


# Field rules, one table row per branch. Patches merge into the PySpark fixture.

REMOVE = object()


def merge(base: dict, patch: dict) -> dict:
    for key, value in patch.items():
        if value is REMOVE:
            base.pop(key, None)
        elif isinstance(value, dict) and isinstance(base.get(key), dict):
            merge(base[key], value)
        else:
            base[key] = value
    return base


def assess_patch(patch: dict, fixture: str = "pyspark-pi.yaml") -> dict:
    return assess_document(merge(load(fixture), patch))


def spec(**fields) -> dict:
    return {"spec": fields}


def driver(**fields) -> dict:
    return spec(driver=fields)


def executor(**fields) -> dict:
    return spec(executor=fields)


EXECUTOR_CONF = "spec.spark_conf[spark.kubernetes.executor"
DRIVER_TEMPLATE = "spec.driver_pod_template.spec"
EXECUTOR_TEMPLATE = "spec.executor_pod_template.spec"
FIELD_RULES = [
    # Application fields.
    (spec(proxyUser=None), "spec.proxyUser", "ignored", None),
    (spec(managedBy="team"), "spec.managedBy", "unsupported", None),
    (spec(mode="in-cluster-client"), "spec.mode", "approximated", None),
    (driver(image="registry.example.com/spark/pyspark:3.5.9"), "spec.image", "mapped", "spec.image"),
    (spec(driver={"image": "d:1"}, executor={"image": "e:1"}), "spec.image", "ignored", None),
    (
        spec(driver={"image": "d:1"}, executor={"image": "e:1"}),
        "spec.executor.image",
        "mapped",
        "spec.spark_conf[spark.kubernetes.executor.container.image]",
    ),
    (
        executor(image="registry.example.com/spark/pyspark:3.5.9"),
        "spec.executor.image",
        "mapped",
        "spec.image",
    ),
    (spec(mainApplicationFile="/opt/app.py"), "spec.mainApplicationFile", "approximated", "spec.app_path"),
    (spec(mainClass="com.example.Main"), "spec.mainClass", "mapped", "spec.class_name"),
    (
        spec(hadoopConf={"fs.s3a.endpoint": "http://minio:9000"}),
        "spec.hadoopConf[fs.s3a.endpoint]",
        "mapped",
        "spec.spark_conf[spark.hadoop.fs.s3a.endpoint]",
    ),
    (spec(nodeSelector={"pool": "spark"}), "spec.nodeSelector[pool]", "mapped", None),
    (
        spec(nodeSelector={"pool": "a"}, driver={"nodeSelector": {"pool": "b"}}),
        "spec.nodeSelector[pool]",
        "approximated",
        "spec.executor_node_selector[pool]",
    ),
    (spec(restartPolicy={"type": "Never", "backoff": 1}), "spec.restartPolicy.backoff", "unsupported", None),
    (
        spec(dynamicAllocation={"shuffleTrackingTimeout": 60000}),
        "spec.dynamicAllocation.shuffleTrackingTimeout",
        "mapped",
        "spec.spark_conf[spark.dynamicAllocation.shuffleTracking.timeout]",
    ),
    (
        spec(dynamicAllocation={"shuffleTrackingEnabled": True}),
        "spec.dynamicAllocation.shuffleTrackingEnabled",
        "mapped",
        "spec.spark_conf[spark.dynamicAllocation.shuffleTracking.enabled]",
    ),
    (spec(deps={"excludePackages": ["a:b"]}), "spec.deps.excludePackages", "mapped", None),
    (
        spec(deps={"files": ["local:///opt/f.txt"]}),
        "spec.deps.files[0]",
        "mapped",
        "spec.spark_conf[spark.files]",
    ),
    (spec(volumes=[{"name": "v", "emptyDir": {}}] * 2), "spec.volumes[0]", "ignored", None),
    (spec(batchScheduler="volcano"), "spec.batchScheduler", "unsupported", None),
    # Spark properties.
    *(
        (spec(sparkConf={key: value}), f"spec.sparkConf[{key}]", status, target)
        for key, value, status, target in [
            ("spark.driver.cores", "1", "mapped", "spec.driver_resources.cpu"),
            ("spark.executor.memory", "512m", "mapped", "spec.executor_resources.memory"),
            ("spark.executor.instances", "2", "mapped", "spec.executor_instances.initial"),
            ("spark.kubernetes.driver.label.tier", "gold", "mapped", "spec.driver_labels[tier]"),
            ("spark.kubernetes.driver.annotation.note", "x", "mapped", "spec.driver_annotations[note]"),
            (
                "spark.kubernetes.node.selector.disk",
                "ssd",
                "mapped",
                "spec.driver_node_selector[disk], spec.executor_node_selector[disk]",
            ),
            ("spark.kubernetes.authenticate.driver.serviceAccountName", "spark-driver", "mapped", None),
            ("spark.kubernetes.executor.podTemplateFile", "/opt/t.yaml", "approximated", None),
            ("spark.kubernetes.executor.secretKeyRef.DB", "db:pw", "mapped", None),
            ("spark.kubernetes.driverEnv.MODE", "batch", "mapped", None),
            ("spark.sql.shuffle.partitions", 16, "mapped", None),
            ("spark.app.name", "other", "unsupported", None),
            ("spark.kubernetes.memoryOverheadFactor", "0.2", "unsupported", None),
            ("spark.kubernetes.driver.volumes.pvc.data.mount.path", "/data", "unsupported", None),
        ]
    ),
    (
        spec(
            sparkConf={"spark.dynamicAllocation.enabled": "true", "spark.dynamicAllocation.maxExecutors": "4"}
        ),
        "spec.sparkConf[spark.dynamicAllocation.maxExecutors]",
        "mapped",
        "spec.executor_instances.max",
    ),
    # Driver and executor fields.
    (driver(cores=REMOVE, coreRequest="1500m"), "spec.driver.coreRequest", "approximated", None),
    (driver(coreRequest="500m"), "spec.driver.coreRequest", "approximated", "spec.driver_resources.cpu"),
    (executor(coreRequest="500m"), "spec.executor.coreRequest", "mapped", f"{EXECUTOR_CONF}.request.cores]"),
    (executor(coreLimit="2"), "spec.executor.coreLimit", "mapped", f"{EXECUTOR_CONF}.limit.cores]"),
    (driver(gpu={"name": "nvidia.com/gpu", "quantity": 1}), "spec.driver.gpu", "unsupported", None),
    (executor(hostNetwork=True), "spec.executor.hostNetwork", "unsupported", None),
    (driver(memoryLimit="2g"), "spec.driver.memoryLimit", "unsupported", None),
    (driver(podName=None), "spec.driver.podName", "ignored", None),
    (
        driver(configMaps=[{"name": "cfg", "path": "/etc/cfg"}]),
        "spec.driver.configMaps[0]",
        "mapped",
        f"{DRIVER_TEMPLATE}.containers[driver].volumeMounts",
    ),
    (driver(secrets=[{"name": "s", "path": "/s"}]), "spec.driver.secrets[0]", "mapped", None),
    (
        executor(secrets=[{"name": "tok", "path": "/t", "secretType": "HadoopDelegationToken"}]),
        "spec.executor.secrets[0]",
        "approximated",
        f"{EXECUTOR_CONF}.secrets.tok]",
    ),
    (
        driver(envVars={"A": "1"}),
        "spec.driver.envVars[A]",
        "mapped",
        f"{DRIVER_TEMPLATE}.containers[driver].env[A]",
    ),
    (
        executor(envVars={"A": "1"}),
        "spec.executor.envVars[A]",
        "mapped",
        "spec.spark_conf[spark.executorEnv.A]",
    ),
    (
        driver(env=[{"name": "A", "value": "1"}], envVars={"A": "1"}),
        "spec.driver.envVars[A]",
        "mapped",
        f"{DRIVER_TEMPLATE}.containers[driver].env[A]",
    ),
    (
        driver(envSecretKeyRefs={"DB": {"name": "db", "key": "pw"}}),
        "spec.driver.envSecretKeyRefs[DB]",
        "mapped",
        f"{DRIVER_TEMPLATE}.containers[driver].env[DB]",
    ),
    (
        executor(envSecretKeyRefs={"DB": {"name": "db", "key": "pw"}}),
        "spec.executor.envSecretKeyRefs[DB]",
        "mapped",
        f"{EXECUTOR_CONF}.secretKeyRef.DB]",
    ),
    (
        executor(env=[{"name": "C", "valueFrom": {"configMapKeyRef": {"name": "c", "key": "k"}}}]),
        "spec.executor.env[0]",
        "mapped",
        f"{EXECUTOR_TEMPLATE}.containers[executor].env[C]",
    ),
    (driver(envFrom=[{"secretRef": {"name": "s"}}]), "spec.driver.envFrom[0]", "mapped", None),
    (driver(annotations={"a": "b"}), "spec.driver.annotations[a]", "mapped", "spec.driver_annotations[a]"),
    (
        executor(nodeSelector={"p": "q"}),
        "spec.executor.nodeSelector[p]",
        "mapped",
        "spec.executor_node_selector[p]",
    ),
    *(
        (driver(**{field: value}), f"spec.driver.{field}", "mapped", target)
        for field, value, target in [
            ("affinity", {"nodeAffinity": {}}, f"{DRIVER_TEMPLATE}.affinity"),
            ("tolerations", [{"key": "a", "operator": "Exists"}], f"{DRIVER_TEMPLATE}.tolerations"),
            ("podSecurityContext", {"fsGroup": 185}, f"{DRIVER_TEMPLATE}.securityContext"),
            ("securityContext", {"runAsUser": 185}, f"{DRIVER_TEMPLATE}.containers[driver].securityContext"),
            ("schedulerName", "volcano", f"{DRIVER_TEMPLATE}.schedulerName"),
            ("priorityClassName", "high", f"{DRIVER_TEMPLATE}.priorityClassName"),
            ("dnsConfig", {"nameservers": ["1.1.1.1"]}, f"{DRIVER_TEMPLATE}.dnsConfig"),
            ("hostAliases", [{"ip": "10.0.0.1", "hostnames": ["db"]}], f"{DRIVER_TEMPLATE}.hostAliases"),
            ("shareProcessNamespace", True, f"{DRIVER_TEMPLATE}.shareProcessNamespace"),
            ("terminationGracePeriodSeconds", 30, f"{DRIVER_TEMPLATE}.terminationGracePeriodSeconds"),
            ("lifecycle", {"preStop": {"exec": {"command": ["true"]}}}, None),
            ("ports", [{"name": "m", "containerPort": 9090, "protocol": "TCP"}], None),
            ("javaOptions", "-XX:+UseG1GC", "spec.spark_conf[spark.driver.extraJavaOptions]"),
        ]
    ),
    (
        driver(initContainers=[{"name": "init", "image": "i:1"}]),
        "spec.driver.initContainers[0]",
        "mapped",
        f"{DRIVER_TEMPLATE}.initContainers[init]",
    ),
    (
        executor(sidecars=[{"name": "side", "image": "s:1"}]),
        "spec.executor.sidecars[0]",
        "mapped",
        f"{EXECUTOR_TEMPLATE}.containers[side]",
    ),
    (
        executor(serviceAccount="exec"),
        "spec.executor.serviceAccount",
        "mapped",
        "spec.spark_conf[spark.kubernetes.authenticate.executor.serviceAccountName]",
    ),
    (
        executor(deleteOnTermination=False),
        "spec.executor.deleteOnTermination",
        "mapped",
        f"{EXECUTOR_CONF}.deleteOnTermination]",
    ),
    *(
        (driver(**{field: value}), f"spec.driver.{field}", "unsupported", None)
        for field, value in [
            ("template", {"spec": {}}),
            ("kubernetesMaster", "https://k8s"),
            ("serviceLabels", {"a": "b"}),
            ("serviceAnnotations", {"a": "b"}),
            ("podName", "fixed"),
        ]
    ),
    (
        driver(volumeMounts=[{"name": "missing", "mountPath": "/m"}]),
        "spec.driver.volumeMounts[0]",
        "unsupported",
        None,
    ),
    # Metadata and document fields.
    (
        {"metadata": {"labels": {"kueue.x-k8s.io/queue-name": "q"}}},
        "metadata.labels[kueue.x-k8s.io/queue-name]",
        "unsupported",
        None,
    ),
    # Only the exact Kueue prefix is recognized; a longer domain is an ordinary label.
    (
        {"metadata": {"labels": {"kueue.x-k8s.io.example.com/queue-name": "q"}}},
        "metadata.labels[kueue.x-k8s.io.example.com/queue-name]",
        "mapped",
        None,
    ),
    ({"metadata": {"labels": {"version": "x"}}}, "metadata.labels[version]", "ignored", None),
    ({"metadata": {"annotations": {"owner": "a"}}}, "metadata.annotations[owner]", "unsupported", None),
    (
        {"metadata": {"annotations": {"kubectl.kubernetes.io/last-applied-configuration": "{}"}}},
        "metadata.annotations[kubectl.kubernetes.io/last-applied-configuration]",
        "ignored",
        None,
    ),
    ({"metadata": {"uid": "1234"}}, "metadata.uid", "ignored", None),
    (
        {"metadata": {"ownerReferences": [{"kind": "ScheduledSparkApplication"}]}},
        "metadata.ownerReferences",
        "unsupported",
        None,
    ),
    ({"metadata": {"clusterName": "c"}}, "metadata.clusterName", "unsupported", None),
    ({"status": {"applicationState": {"state": "COMPLETED"}}}, "status", "ignored", None),
    ({"extra": 1}, "extra", "unsupported", None),
]


@pytest.mark.parametrize("patch_, path, status, target", FIELD_RULES)
def test_field_rules(patch_, path, status, target):
    finding = findings(assess_patch(copy.deepcopy(patch_)))[path]
    assert finding["status"] == status, finding
    if target is not None:
        assert finding["target"] == target
    if status == "unsupported":
        assert finding["note"] and finding["alternative"]
    if status == "mapped":
        assert finding["target"] or finding["note"]


def test_translated_values_reach_the_draft():
    result = assess_patch(
        merge(
            spec(
                sparkConf={"spark.sql.shuffle.partitions": 16, "spark.dynamicAllocation.enabled": "true"},
                dynamicAllocation={"shuffleTrackingTimeout": 60000, "maxExecutors": 4},
                deps={"excludePackages": ["a:b", "c:d"]},
                imagePullSecrets=["regcred", "regcred"],
            ),
            driver(cores=REMOVE, coreRequest="1500m", envFrom=[{"secretRef": {"name": "s"}}] * 2),
        )
    )
    draft = result["draft"]["spec"]
    assert draft["driver_resources"]["cpu"] == 2
    assert draft["executor_instances"] == {"min": 0, "max": 4, "initial": 2}
    assert draft["spark_conf"]["spark.sql.shuffle.partitions"] == "16"
    assert draft["spark_conf"]["spark.dynamicAllocation.shuffleTracking.timeout"] == "60000ms"
    assert draft["spark_conf"]["spark.jars.excludes"] == "a:b,c:d"
    template = draft["driver_pod_template"]["spec"]
    assert template["imagePullSecrets"] == [{"name": "regcred"}]
    assert template["containers"][0]["envFrom"] == [{"secretRef": {"name": "s"}}]


INVALID = [
    (spec(driver="x"), "spec.driver"),
    (spec(image=""), "spec.image"),
    (spec(imagePullPolicy="Sometimes"), "spec.imagePullPolicy"),
    (spec(imagePullSecrets="regcred"), "spec.imagePullSecrets"),
    (spec(mainClass=5), "spec.mainClass"),
    (spec(mainApplicationFile=""), "spec.mainApplicationFile"),
    (spec(arguments="a b"), "spec.arguments"),
    (spec(sparkConf=["x"]), "spec.sparkConf"),
    (spec(hadoopConf="x"), "spec.hadoopConf"),
    (spec(sparkConf={"spark.x": {"nested": 1}}), "spec.sparkConf[spark.x]"),
    (spec(sparkConf={"spark.driver.cores": "0"}), "spec.sparkConf[spark.driver.cores]"),
    (spec(sparkConf={"spark.driver.memory": "lots"}), "spec.sparkConf[spark.driver.memory]"),
    (
        spec(sparkConf={"spark.dynamicAllocation.enabled": "maybe"}),
        "spec.sparkConf[spark.dynamicAllocation.enabled]",
    ),
    (spec(sparkConf={"spark.executor.instances": "two"}), "spec.sparkConf[spark.executor.instances]"),
    (spec(dynamicAllocation="on"), "spec.dynamicAllocation"),
    (spec(dynamicAllocation={"enabled": "yes"}), "spec.dynamicAllocation.enabled"),
    (spec(dynamicAllocation={"maxExecutors": -1}), "spec.dynamicAllocation.maxExecutors"),
    (
        spec(dynamicAllocation={"shuffleTrackingTimeout": "1m"}),
        "spec.dynamicAllocation.shuffleTrackingTimeout",
    ),
    (
        spec(dynamicAllocation={"shuffleTrackingEnabled": "true"}),
        "spec.dynamicAllocation.shuffleTrackingEnabled",
    ),
    (spec(deps=["a.jar"]), "spec.deps"),
    (spec(deps={"jars": "a.jar"}), "spec.deps.jars"),
    (spec(restartPolicy="Never"), "spec.restartPolicy"),
    (spec(restartPolicy={"type": "Sometimes"}), "spec.restartPolicy.type"),
    (spec(nodeSelector={"a": 1}), "spec.nodeSelector"),
    (spec(volumes={"name": "v"}), "spec.volumes"),
    (spec(volumes=[{"name": "v"}, {"name": "v"}]), "spec.volumes[1]"),
    (spec(sparkVersion=3), "spec.sparkVersion"),
    (driver(cores=0), "spec.driver.cores"),
    (driver(coreLimit="lots"), "spec.driver.coreLimit"),
    (driver(memory=512), "spec.driver.memory"),
    (driver(image=""), "spec.driver.image"),
    (driver(configMaps="cfg"), "spec.driver.configMaps"),
    (driver(configMaps=[{"name": "cfg"}]), "spec.driver.configMaps[0]"),
    (driver(secrets="s"), "spec.driver.secrets"),
    (driver(secrets=[{"name": "s", "path": "/s", "secretType": "Other"}]), "spec.driver.secrets[0]"),
    (driver(env="A=1"), "spec.driver.env"),
    (driver(env=[{"value": "x"}]), "spec.driver.env[0]"),
    (driver(envVars={"A": 1}), "spec.driver.envVars"),
    (driver(envSecretKeyRefs="s"), "spec.driver.envSecretKeyRefs"),
    (driver(envSecretKeyRefs={"A": {"name": "s"}}), "spec.driver.envSecretKeyRefs[A]"),
    (driver(envFrom=["s"]), "spec.driver.envFrom"),
    (driver(labels={"a": 1}), "spec.driver.labels"),
    (driver(volumeMounts="v"), "spec.driver.volumeMounts"),
    (driver(volumeMounts=[{"name": "v"}]), "spec.driver.volumeMounts[0]"),
    (driver(affinity=[]), "spec.driver.affinity"),
    (driver(terminationGracePeriodSeconds=True), "spec.driver.terminationGracePeriodSeconds"),
    (driver(sidecars="s"), "spec.driver.sidecars"),
    (driver(sidecars=[{"image": "s:1"}]), "spec.driver.sidecars[0]"),
    (driver(serviceAccount=""), "spec.driver.serviceAccount"),
    (driver(javaOptions=""), "spec.driver.javaOptions"),
    (executor(instances=-1), "spec.executor.instances"),
    (executor(deleteOnTermination="no"), "spec.executor.deleteOnTermination"),
    ({"metadata": {"namespace": 5}}, "metadata.namespace"),
    ({"metadata": {"labels": "team"}}, "metadata.labels"),
    ({"metadata": {"annotations": ["a"]}}, "metadata.annotations"),
]


@pytest.mark.parametrize("patch_, path", INVALID)
def test_invalid_values_are_unsupported_without_echoing_them(patch_, path):
    result = assess_patch(copy.deepcopy(patch_))
    finding = findings(result)[path]
    assert finding["status"] == "unsupported"
    assert finding["note"].startswith("Unexpected value; expected ")
    assert result["migration_ready"] is False


CONFLICTS = [
    (
        spec(sparkConf={"spark.kubernetes.authenticate.driver.serviceAccountName": "other"}),
        "spec.sparkConf[spark.kubernetes.authenticate.driver.serviceAccountName]",
        "Conflicts with spec.driver.serviceAccount",
    ),
    (
        spec(sparkConf={"spark.executor.instances": "5"}),
        "spec.sparkConf[spark.executor.instances]",
        "Conflicts with spec.executor.instances",
    ),
    (
        spec(sparkConf={"spark.jars": "x.jar"}, deps={"jars": ["local:///a.jar"]}),
        "spec.deps.jars",
        "Conflicts with spec.sparkConf[spark.jars]",
    ),
    (
        driver(env=[{"name": "A", "value": "1"}], envVars={"A": "2"}),
        "spec.driver.envVars[A]",
        "already has an entry with name 'A'",
    ),
    (
        driver(configMaps=[{"name": "a", "path": "/etc/x"}, {"name": "b", "path": "/etc/x"}]),
        "spec.driver.configMaps[1]",
        "already has an entry with mountPath '/etc/x'",
    ),
    (
        spec(
            volumes=[{"name": "cfg-vol", "emptyDir": {}}],
            driver={
                "volumeMounts": [{"name": "cfg-vol", "mountPath": "/a"}],
                "configMaps": [{"name": "cfg", "path": "/b"}],
            },
        ),
        "spec.driver.configMaps[0]",
        "different volume named 'cfg-vol'",
    ),
    (
        driver(sidecars=[{"name": "driver", "image": "x"}]),
        "spec.driver.sidecars[0]",
        "container named 'driver'",
    ),
    (
        driver(initContainers=[{"name": "i", "image": "a"}, {"name": "i", "image": "b"}]),
        "spec.driver.initContainers[1]",
        "entry with name 'i'",
    ),
]


@pytest.mark.parametrize("patch_, path, message", CONFLICTS)
def test_conflicting_sources_are_unsupported(patch_, path, message):
    result = assess_patch(copy.deepcopy(patch_))
    finding = findings(result)[path]
    assert finding["status"] == "unsupported"
    assert message in finding["note"]
    assert result["migration_ready"] is False


CREDENTIALS = [
    (
        spec(
            volumes=[
                {"name": "d", "csi": {"driver": "s3", "volumeAttributes": {"url": "https://u:hunter2@s3"}}}
            ],
            driver={"volumeMounts": [{"name": "d", "mountPath": "/d"}]},
        ),
        ["spec.driver.volumeMounts[0]", "spec.volumes[0]"],
    ),
    (
        driver(lifecycle={"preStop": {"exec": {"command": ["curl", "https://u:hunter2@hook"]}}}),
        ["spec.driver.lifecycle"],
    ),
    (spec(deps={"repositories": ["https://u:hunter2@repo"]}), ["spec.deps.repositories"]),
    (driver(envVars={"DB_PASSWORD": "hunter2"}), ["spec.driver.envVars[DB_PASSWORD]"]),
    (spec(arguments=["--db-token", "hunter2"]), ["spec.arguments"]),
]


@pytest.mark.parametrize("patch_, paths", CREDENTIALS)
def test_credentials_in_any_field_are_withheld(patch_, paths):
    result = assess_patch(copy.deepcopy(patch_))
    for path in paths:
        assert findings(result)[path]["note"].startswith("The value looks like a credential")
    assert "hunter2" not in json.dumps(result)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("512m", (512, True)),
        ("2g", (2048, True)),
        ("1536k", (2, False)),
        ("1048576b", (1, True)),
        ("7", (7, True)),
    ],
)
def test_spark_memory_strings(value, expected):
    document = load("pyspark-pi.yaml")
    document["spec"]["driver"]["memory"] = value
    result = assess_document(document)
    assert result["draft"]["spec"]["driver_resources"]["memory"] == expected[0]
    assert findings(result)["spec.driver.memory"]["status"] == ("mapped" if expected[1] else "approximated")


def test_scheduled_application_null_and_unknown_fields():
    result = assess_patch(
        {"spec": {"suspend": True, "timeZone": None, "jitter": "5m"}}, fixture="scheduled.yaml"
    )
    by_path = findings(result)
    assert by_path["spec.suspend"]["status"] == "unsupported"
    assert "CronJob spec.suspend" in by_path["spec.suspend"]["alternative"]
    assert by_path["spec.timeZone"]["status"] == "ignored"
    assert by_path["spec.jitter"]["note"].startswith("Unrecognized")


def test_notes_name_missing_namespace_and_service_account():
    result = assess_patch({"metadata": {"namespace": REMOVE}, "spec": {"driver": {"serviceAccount": REMOVE}}})
    notes = " ".join(result["notes"])
    assert "no namespace" in notes and "no driver service account" in notes
    assert "namespace" not in result["draft"]["spec"]


def test_the_final_accounting_pass_reports_fields_a_rule_missed(monkeypatch):
    from spark_on_k8s import spark_operator_migration as migration

    def noop(a, path, value, spec):
        pass

    monkeypatch.setitem(migration._APPLICATION_FIELDS, "mainClass", noop)
    monkeypatch.setitem(migration._APPLICATION_FIELDS, "sparkConf", noop)
    result = assess_patch(spec(mainClass="Main"))
    by_path = findings(result)
    for path in ("spec.mainClass", "spec.sparkConf[spark.sql.shuffle.partitions]"):
        assert by_path[path]["status"] == "unsupported"
        assert by_path[path]["note"] == migration._UNACCOUNTED
    assert result["migration_ready"] is False


@pytest.mark.parametrize(
    "content, message",
    [
        (
            "apiVersion: sparkoperator.k8s.io/v1beta2\nkind: SparkApplication\n"
            "metadata: {name: a}\nspec: []\n",
            "spec must be an object",
        ),
        (
            "apiVersion: sparkoperator.k8s.io/v1beta2\nkind: ScheduledSparkApplication\n"
            "metadata: {name: a}\nspec: {schedule: '@daily'}\n",
            "spec.template must be an object",
        ),
        ("a: " + "x" * (1024 * 1024), "no larger than 1 MiB"),
    ],
)
def test_structurally_invalid_documents_are_errors(content, message):
    with pytest.raises(ManifestError, match=message):
        assess_manifests(content)


def test_cli_text_report_with_skipped_documents_and_partial_drafts(tmp_path):
    valid = load("pyspark-pi.yaml")
    invalid = merge(load("pyspark-pi.yaml"), {"metadata": {"name": "no-image"}, "spec": {"image": REMOVE}})
    manifests = tmp_path / "manifests.yaml"
    # The leading empty document is ignored but still counts as a document position.
    manifests.write_text(
        "---\n---\n" + yaml.safe_dump_all([{"apiVersion": "v1", "kind": "ConfigMap"}, valid, invalid])
    )
    runner = CliRunner()
    report = runner.invoke(job_cli, ["assess-operator", str(manifests)])
    assert report.exit_code == 0, report.output
    assert "Draft: valid offline; write it with --draft-dir." in report.stdout
    assert "Draft: not produced: spec.image is required." in report.stdout
    assert "Skipped document 2 (v1 ConfigMap)" in report.stdout
    drafts = tmp_path / "drafts"
    drafts.mkdir()
    written = runner.invoke(job_cli, ["assess-operator", str(manifests), "--draft-dir", str(drafts)])
    assert written.exit_code == 0, written.output
    assert sorted(path.name for path in drafts.iterdir()) == ["pyspark-pi.yaml"]


def test_cli_refuses_two_drafts_for_one_file(tmp_path):
    first = load("pyspark-pi.yaml")
    second = merge(load("pyspark-pi.yaml"), {"metadata": {"namespace": "other"}})
    manifests = tmp_path / "manifests.yaml"
    manifests.write_text(yaml.safe_dump_all([first, second]))
    result = CliRunner().invoke(job_cli, ["assess-operator", str(manifests), "--draft-dir", str(tmp_path)])
    assert result.exit_code == 1
    assert "Two drafts would be written" in result.stderr
    assert not (tmp_path / "pyspark-pi.yaml").exists()
