from __future__ import annotations

import copy
import json
from unittest.mock import MagicMock, patch

import pytest
import yaml
from click.testing import CliRunner
from kubernetes import client as k8s
from spark_on_k8s.cli.job import job_cli
from spark_on_k8s.client import SparkOnK8S
from spark_on_k8s.jobs import JobSpec, JobValidationError
from spark_on_k8s.utils.configuration import Configuration


def definition(**spec):
    return {
        "apiVersion": "spark-on-k8s/v1",
        "kind": "SparkJob",
        "metadata": {"name": "daily-etl"},
        "spec": {"image": "spark:4.0.5", "app_path": "local:///opt/spark/jobs/etl.py", **spec},
    }


def test_profiles_merge_maps_replace_lists_and_do_not_mutate_input():
    document = definition(spark_conf={"spark.sql.shuffle.partitions": 4}, packages=["dev:jar:1"])
    document["profiles"] = {
        "prod": {
            "spec": {
                "namespace": "production",
                "packages": [],
                "spark_conf": {"spark.sql.adaptive.enabled": True},
                "executor_instances": {"min": 2, "max": 5},
            }
        }
    }
    original = copy.deepcopy(document)
    job = JobSpec.from_mapping(document, profile="prod")
    kwargs = job.to_submit_kwargs()
    assert kwargs["spark_conf"] == {"spark.sql.shuffle.partitions": 4, "spark.sql.adaptive.enabled": True}
    assert kwargs["packages"] == []
    assert kwargs["executor_instances"].initial == 2
    assert kwargs["namespace"] == "production"
    kwargs["spark_conf"]["added"] = "later"
    assert job.spec["spark_conf"] == document["spec"]["spark_conf"] | {"spark.sql.adaptive.enabled": True}
    assert document == original


@pytest.mark.parametrize(
    "spec",
    [
        {"imag": "typo"},
        {"image": ""},
        {"app_path": None},
        {"namespace": "bad/name"},
        {"driver_resources": {"cpu": True}},
        {"executor_resources": {"memory": 0}},
        {"executor_instances": {"min": 3, "max": 2}},
        {"executor_instances": {"min": 2, "initial": 1}},
        {"executor_instances": {"initial": 3, "max": 2}},
        {"executor_instances": {"executor_allocation_ratio": 0}},
        {"executor_instances": {"executor_allocation_ratio": float("nan")}},
        {"spark_conf": {"x": float("inf")}},
        {"spark_conf": {"x": {"a": "b"}}},
        {"driver_labels": {"x": 4}},
        {"packages": "one,two"},
        {"startup_timeout": -1},
        {"max_runtime": 0},
        {"max_runtime": "1h"},
        {"max_runtime": True},
        {"max_runtime": 31 * 24 * 3600},
        {"ui_reverse_proxy": "false"},
        {"image_pull_policy": "sometimes"},
        {"app_waiter": "forever"},
        {"executor_pod_template": [], "executor_pod_template_path": "/tmp/template"},
    ],
)
def test_invalid_settings_are_rejected_before_cluster_access(spec):
    with patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client") as connect:
        with pytest.raises(JobValidationError):
            JobSpec.from_mapping(definition(**spec))
        connect.assert_not_called()


@pytest.mark.parametrize(
    "change",
    [
        {"apiVersion": "v999"},
        {"kind": "Pod"},
        {"metadata": {"name": "123"}},
        {"metadata": {"name": "valid", "typo": "x"}},
        {"kubernetes": {"in_cluster": "yes"}},
        {"kubernetes": {"in_cluster": True, "context": "prod"}},
        {"profiles": {"bad": {"typo": 1}}},
    ],
)
def test_invalid_document(change):
    with pytest.raises(JobValidationError):
        JobSpec.from_mapping(definition() | change)


def test_missing_profile_is_an_error():
    with pytest.raises(JobValidationError, match="Unknown profile"):
        JobSpec.from_mapping(definition(), profile="missing")


@pytest.mark.parametrize(
    "content",
    [
        "a: 1\na: 2",
        "a: &value 1\nb: *value",
        "!!python/object:object {}",
        "---\na: 1\n---\nb: 2",
        "[]",
        "1: a",
        "a: [",
        "x" * (1024 * 1024 + 1),
    ],
)
def test_ambiguous_unsafe_or_oversized_files_are_rejected(tmp_path, content):
    file = tmp_path / "job.yaml"
    file.write_text(content)
    with pytest.raises(JobValidationError):
        JobSpec.from_file(file)


def test_relative_files_resolve_from_job_directory(tmp_path, monkeypatch):
    directory = tmp_path / "project"
    directory.mkdir()
    template = "apiVersion: v1\nkind: Pod\nspec:\n  containers:\n    - name: executor\n"
    (directory / "executor.yaml").write_text(template)
    file = directory / "job.yaml"
    file.write_text(
        yaml.safe_dump(
            definition(executor_pod_template="executor.yaml")
            | {"kubernetes": {"config_file": "kubeconfig", "context": "dev"}}
        )
    )
    monkeypatch.chdir(tmp_path)
    job = JobSpec.from_file(file)
    assert job.spec["executor_pod_template"] == template
    assert job.kubernetes["config_file"] == str(directory / "kubeconfig")
    assert job.client().k8s_client_manager.context == "dev"
    with pytest.raises(JobValidationError, match="kubernetes_conn_id"):
        job.to_operator_kwargs()


def test_job_defaults_do_not_inherit_ambient_submission_settings(monkeypatch):
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_SECRET_ENV_VAR", {"TOKEN": "ambient-secret"})
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_EXECUTOR_TEMPLATE", "not a pod")
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_EXECUTOR_POD_TEMPLATE_PATH", "wrong-path")
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_NAMESPACE", "wrong-namespace")
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_CONTEXT", "wrong-context")
    job = JobSpec.from_mapping(definition())
    with patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client") as connect:
        result = job.plan()
        connect.assert_not_called()
    assert result.namespace == "default"
    assert job.client().k8s_client_manager.context is None
    assert "ambient-secret" not in repr(result.resources)
    assert {resource["kind"] for resource in result.resources} == {"Pod", "Service"}
    assert job.to_operator_kwargs() == job.to_submit_kwargs()


def test_plan_and_submission_build_identical_pod_without_mutating_inputs():
    manager = MagicMock()
    spark = SparkOnK8S(k8s_client_manager=manager)
    volumes, mounts = [], []
    kwargs = {
        "image": "spark:4.0.5",
        "app_path": "local:///job.py",
        "app_name": "job",
        "app_id_suffix": lambda: "-test",
        "namespace": "test",
        "volumes": volumes,
        "driver_volume_mounts": mounts,
        "driver_ephemeral_configmaps_volumes": [
            {"name": "script", "mount_path": "/jobs", "sources": [{"name": "job.py", "text": "print(1)"}]}
        ],
        "secret_values": {"TOKEN": "private"},
        "app_waiter": "no_wait",
    }
    plan = spark.plan_app(**kwargs)
    manager.client.assert_not_called()
    with patch("spark_on_k8s.client.k8s.CoreV1Api") as api:
        api.return_value.create_namespaced_pod.return_value.metadata.uid = "real-uid"
        spark.submit_app(**kwargs)
        submitted = api.return_value.create_namespaced_pod.call_args.kwargs["body"]
    pod = next(item for item in plan.resources if item["kind"] == "Pod")
    with k8s.ApiClient() as serializer:
        expected = serializer.sanitize_for_serialization(submitted)
    assert pod == {**expected, "apiVersion": "v1", "kind": "Pod"}
    assert not volumes and not mounts


def test_preview_redacts_payloads_and_arguments_but_keeps_structure():
    spark = SparkOnK8S()
    plan = spark.plan_app(
        image="spark:4.0.5",
        app_name="redaction",
        app_path="local:///job.py",
        spark_conf={"spark.hadoop.fs.s3a.secret.key": "secret-value"},
        secret_values={"TOKEN": "token-value"},
        app_arguments=["positional-secret"],
        driver_ephemeral_configmaps_volumes=[
            {"name": "sql", "mount_path": "/sql", "sources": [{"name": "job.sql", "text": "private-sql"}]}
        ],
    )
    displayed = json.dumps(plan.to_dict())
    for secret in ("secret-value", "token-value", "positional-secret", "private-sql"):
        assert secret not in displayed
        assert secret not in repr(plan)
    assert "local:///job.py" in displayed
    assert "<redacted" in displayed
    assert any(resource["kind"] == "Secret" for resource in plan.resources)


def test_cli_validate_and_plan_are_offline_and_machine_readable(tmp_path):
    file = tmp_path / "job.yaml"
    file.write_text(yaml.safe_dump(definition()))
    runner = CliRunner()
    with patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client") as connect:
        valid = runner.invoke(job_cli, ["validate", str(file)])
        result = runner.invoke(job_cli, ["plan", str(file), "--output", "json"])
        connect.assert_not_called()
    assert valid.exit_code == 0, valid.output
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["preview"] is True
    assert result.stderr == ""
    invalid = runner.invoke(job_cli, ["plan", str(file), "--profile", "missing"])
    assert invalid.exit_code != 0
    assert "Unknown profile" in invalid.output


def test_preview_redacts_init_commands_and_jdbc_credentials():
    plan = SparkOnK8S().plan_app(
        image="spark:4.0.5",
        app_name="private",
        app_path="local:///job.py",
        spark_conf={"spark.datasource.url": "jdbc:postgresql://db/data?user=alice&password=JDBC_SECRET"},
        driver_init_containers=[
            k8s.V1Container(
                name="init",
                image="helper",
                command=["sh", "-c", "INIT_SECRET"],
                args=["--password", "ARG_SECRET"],
            )
        ],
    )
    displayed = json.dumps(plan.to_dict())
    for secret in ("JDBC_SECRET", "INIT_SECRET", "ARG_SECRET"):
        assert secret not in displayed
    assert "local:///job.py" in displayed


@pytest.mark.parametrize("status,code", [("Succeeded", 0), ("Failed", 3)])
def test_job_submit_uses_automation_receipts_and_workload_exit_codes(tmp_path, status, code):
    from spark_on_k8s.utils.run import RunHandle
    from spark_on_k8s.utils.spark_app_status import SparkAppStatus

    file = tmp_path / "job.yaml"
    file.write_text(yaml.safe_dump(definition(app_waiter="wait")))
    spark = MagicMock()
    spark.last_submitted_run = RunHandle(
        namespace="default", app_id="daily-etl-test", driver_pod="daily-etl-test-driver"
    )
    spark.app_manager.app_status.return_value = SparkAppStatus(status)
    with patch.object(JobSpec, "client", return_value=spark):
        result = CliRunner().invoke(job_cli, ["submit", str(file), "--output", "json", "--timeout", "5"])
    assert result.exit_code == code, result.output
    payload = json.loads(result.stdout)
    assert payload["run"]["driver_pod"] == "daily-etl-test-driver"
    assert payload["status"] == status
    assert spark.submit_app.call_args.kwargs["app_waiter"] == "no_wait"
    assert spark.submit_app.call_args.kwargs["secret_values"] == {}
    assert spark.app_manager.wait_for_app.call_args.kwargs["timeout"] <= 5


@pytest.mark.parametrize("output", ["json", "text"])
def test_job_submit_load_failure_keeps_the_submission_output_contract(tmp_path, output):
    file = tmp_path / "job.yaml"
    file.write_text(yaml.safe_dump(definition()))
    with patch.object(JobSpec, "client") as client:
        result = CliRunner().invoke(
            job_cli, ["submit", str(file), "--profile", "missing", "--output", output]
        )
    assert result.exit_code == 1, result.output
    client.assert_not_called()
    if output == "json":
        assert json.loads(result.stdout) == {
            "schema_version": 1,
            "operation": "submit",
            "namespace": "",
            "error": {"code": "operation_error", "message": "Unknown profile: missing."},
            "run": None,
            "status": None,
        }
        assert result.stderr == ""
    else:
        assert result.stdout == ""
        assert result.stderr == "Error: Unknown profile: missing.\n"


def test_driver_template_in_job_plan_is_offline_and_sidecar_args_are_redacted(tmp_path):
    template = {
        "apiVersion": "v1",
        "kind": "Pod",
        "spec": {
            "containers": [
                {"name": "sidecar", "image": "helper", "args": ["SIDECAR_SECRET"]},
                {"name": "custom-driver", "image": "ignored"},
            ]
        },
    }
    (tmp_path / "driver.yaml").write_text(yaml.safe_dump(template))
    file = tmp_path / "job.yaml"
    file.write_text(
        yaml.safe_dump(definition(driver_pod_template="driver.yaml", driver_container_name="custom-driver"))
    )
    job = JobSpec.from_file(file)
    with patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client") as connect:
        plan = job.plan()
        connect.assert_not_called()
    pod = next(item for item in plan.resources if item["kind"] == "Pod")
    assert pod["spec"]["containers"][0]["name"] == "custom-driver"
    assert plan.driver_pod == pod["metadata"]["name"]
    assert "SIDECAR_SECRET" not in json.dumps(plan.to_dict())


def test_planning_does_not_clear_previous_run_identity():
    from spark_on_k8s.utils.run import RunHandle

    spark = SparkOnK8S()
    run = RunHandle(namespace="default", app_id="existing", driver_pod="existing-driver")
    spark.last_submitted_run = run
    spark.last_submitted_app_id = run.app_id
    spark.plan_app(image="spark:test", app_path="local:///job.py")
    assert spark.last_submitted_run is run
    assert spark.last_submitted_app_id == "existing"


def test_required_fields_and_conflicting_template_sources_are_rejected():
    missing = definition()
    del missing["spec"]["image"]
    with pytest.raises(JobValidationError, match="image is required"):
        JobSpec.from_mapping(missing)
    with pytest.raises(JobValidationError, match="non-empty string"):
        JobSpec.from_mapping(definition(executor_instances={"scheduler_backlog_timeout": ""}))
    with pytest.raises(JobValidationError, match="not both"):
        JobSpec.from_mapping(
            definition(executor_pod_template={"kind": "Pod"}, executor_pod_template_path="s3a://template")
        )


@pytest.mark.parametrize("command", ["validate", "plan"])
def test_cli_rejects_invalid_pod_before_accessing_cluster(tmp_path, command):
    file = tmp_path / "job.yaml"
    file.write_text(yaml.safe_dump(definition(driver_pod_template={"apiVersion": "v1", "kind": "Service"})))
    with patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client") as connect:
        result = CliRunner().invoke(job_cli, [command, str(file)])
        connect.assert_not_called()
    assert result.exit_code == 1
    assert "must have apiVersion: v1 and kind: Pod" in result.stderr
    assert "Traceback" not in result.output


def test_python_job_submission_forwards_portable_settings():
    job = JobSpec.from_mapping(definition(namespace="analysis", app_arguments=["one", "two"]))
    client = MagicMock()
    client.submit_app.return_value = "daily-etl-driver"
    with patch.object(JobSpec, "client", return_value=client):
        assert job.submit() == "daily-etl-driver"
    kwargs = client.submit_app.call_args.kwargs
    assert kwargs["namespace"] == "analysis"
    assert kwargs["app_arguments"] == ["one", "two"]
    assert kwargs["secret_values"] == {}


def test_plan_redacts_literal_environment_values():
    plan = SparkOnK8S().plan_app(
        image="spark:test",
        app_path="local:///job.py",
        driver_init_containers=[
            k8s.V1Container(
                name="init", image="helper", env=[k8s.V1EnvVar(name="ORDINARY_NAME", value="PRIVATE_VALUE")]
            )
        ],
    )
    assert "PRIVATE_VALUE" not in json.dumps(plan.to_dict())


@pytest.mark.parametrize(
    "key",
    [
        "spark.hadoop.fs.azure.account.key.store.dfs.core.windows.net",
        "spark.hadoop.fs.azure.sas.data.store.blob.core.windows.net",
        "spark.hadoop.fs.s3a.encryption.key",
        "spark.hadoop.fs.s3a.server-side-encryption.key",
        "spark.openlineage.transport.auth.apiKey",
        "spark.openlineage.transport.headers.Authorization",
        "spark.es.net.http.auth.pass",
        "spark.redis.auth",
    ],
)
def test_plan_redacts_common_storage_and_service_credential_properties(key):
    plan = SparkOnK8S().plan_app(
        image="spark:test",
        app_path="local:///job.py",
        spark_conf={
            key: "PRIVATE_VALUE",
            "spark.io.encryption.keySizeBits": "256",
            "spark.hadoop.fs.azure.account.auth.type": "OAuth",
        },
    )
    shown = plan.to_dict()
    driver = next(resource for resource in shown["resources"] if resource["kind"] == "Pod")["spec"][
        "containers"
    ][0]
    assert f"{key}=<redacted>" in driver["args"]
    assert "PRIVATE_VALUE" not in json.dumps(shown)
    # Non-secret neighbours stay reviewable.
    assert "spark.io.encryption.keySizeBits=256" in driver["args"]
    assert "spark.hadoop.fs.azure.account.auth.type=OAuth" in driver["args"]


def test_plan_redacts_signed_url_parameters():
    jars = (
        "https://store.blob.core.windows.net/jars/a.jar?sv=2024-01-01&sig=SAS_SIGNATURE,"
        "https://bucket.s3.amazonaws.com/b.jar?X-Amz-Credential=AKIA&X-Amz-Signature=S3_SIGNATURE"
        "&X-Amz-Security-Token=S3_SESSION"
    )
    plan = SparkOnK8S().plan_app(
        image="spark:test", app_path="local:///job.py", spark_conf={"spark.jars": jars}
    )
    displayed = json.dumps(plan.to_dict())
    for secret in ("SAS_SIGNATURE", "S3_SIGNATURE", "S3_SESSION"):
        assert secret not in displayed
    assert "https://store.blob.core.windows.net/jars/a.jar?sv=2024-01-01&sig=<redacted>" in displayed


def test_plan_redacts_literal_executor_environment_properties():
    plan = SparkOnK8S().plan_app(
        image="spark:test",
        app_path="local:///job.py",
        spark_conf={"spark.executorEnv.DB_PASS": "PRIVATE_VALUE", "spark.sql.shuffle.partitions": "8"},
    )
    shown = plan.to_dict()
    driver = next(resource for resource in shown["resources"] if resource["kind"] == "Pod")["spec"][
        "containers"
    ][0]
    assert "spark.executorEnv.DB_PASS=<redacted>" in driver["args"]
    assert "spark.sql.shuffle.partitions=8" in driver["args"]
    assert "PRIVATE_VALUE" not in json.dumps(shown)


def test_max_runtime_is_planned_forwarded_and_removable_by_a_profile():
    document = definition(max_runtime=7200)
    document["profiles"] = {"interactive": {"spec": {"max_runtime": None}}}
    job = JobSpec.from_mapping(document)
    assert job.to_submit_kwargs()["max_runtime"] == 7200
    pod = next(item for item in job.plan().to_dict()["resources"] if item["kind"] == "Pod")
    assert pod["spec"]["activeDeadlineSeconds"] == 7200
    assert job.to_operator_kwargs()["max_runtime"] == 7200
    unbounded = JobSpec.from_mapping(document, profile="interactive")
    pod = next(item for item in unbounded.plan().resources if item["kind"] == "Pod")
    assert "activeDeadlineSeconds" not in pod["spec"]


def test_invalid_max_runtime_profile_is_a_structured_validation_failure(tmp_path):
    document = definition(max_runtime=3600)
    document["profiles"] = {"typo": {"spec": {"max_runtime": 3_600_000_000}}}
    file = tmp_path / "job.yaml"
    file.write_text(yaml.safe_dump(document))
    result = CliRunner().invoke(job_cli, ["validate", str(file), "--all-profiles", "--output", "json"])
    assert result.exit_code == 1
    report = json.loads(result.stdout)
    assert [item["valid"] for item in report["profiles"]] == [True, False]
    assert report["error"]["code"] == "invalid_job"
    assert "spec.max_runtime" in report["error"]["message"]


@pytest.mark.parametrize("command", ["plan", "submit"])
def test_cli_reports_an_undecodable_template_file_without_a_traceback(tmp_path, command):
    (tmp_path / "driver.yaml").write_bytes(b"apiVersion: v1\nkind: Pod\n# caf\xe9\n")
    file = tmp_path / "job.yaml"
    file.write_text(yaml.safe_dump(definition(driver_pod_template="driver.yaml")))
    with patch.object(JobSpec, "client") as client:
        result = CliRunner().invoke(job_cli, [command, str(file), "--output", "json"])
    assert result.exit_code == 1, result.output
    assert isinstance(result.exception, SystemExit), repr(result.exception)
    client.assert_not_called()
    if command == "submit":
        receipt = json.loads(result.stdout)
        assert receipt["error"]["code"] == "operation_error"
        assert "utf-8" in receipt["error"]["message"]
        assert receipt["run"] is None
        assert result.stderr == ""
    else:
        assert result.stdout == ""
        assert result.stderr.startswith("Error: ")
