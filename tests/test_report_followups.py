from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pytest
import yaml
from click.testing import CliRunner
from kubernetes import client as k8s
from spark_on_k8s.cli.automation import execute_submission
from spark_on_k8s.cli.job import job_cli
from spark_on_k8s.cli.receipts import read_receipt, receipt_writer
from spark_on_k8s.client import ExecutorInstances, SparkOnK8S
from spark_on_k8s.jobs import JobSpec
from spark_on_k8s.resource_plan import check_budget
from spark_on_k8s.sql import prepare_sql_options
from spark_on_k8s.utils.app_manager import SparkAppManager
from spark_on_k8s.utils.run import RunHandle


def run_handle():
    return RunHandle(namespace="spark", app_id="job", driver_pod="job-driver", driver_uid="original-uid")


def test_receipt_is_written_before_wait_and_survives_interruption(tmp_path):
    destination = tmp_path / "run.json"
    spark = Mock(last_submitted_run=run_handle())

    def interrupted(**kwargs):
        assert read_receipt(destination) == run_handle()
        raise KeyboardInterrupt

    spark.app_manager.wait_for_app.side_effect = interrupted
    import click

    with pytest.raises(click.exceptions.Exit) as error:
        execute_submission(spark, {"namespace": "spark", "app_waiter": "wait"}, receipt_file=destination)
    assert error.value.exit_code == 130
    assert read_receipt(destination) == run_handle()
    assert not list(tmp_path.glob(".spark-receipt-*"))
    assert destination.stat().st_mode & 0o777 == 0o600


def test_existing_receipt_prevents_submission(tmp_path):
    destination = tmp_path / "run.json"
    destination.write_text("keep")
    spark = Mock(last_submitted_run=None)
    import click

    with pytest.raises(click.exceptions.Exit):
        execute_submission(spark, {}, receipt_file=destination)
    spark.submit_app.assert_not_called()
    assert destination.read_text() == "keep"


def test_receipt_publish_never_overwrites_racing_writer(tmp_path):
    destination = tmp_path / "run.json"
    with pytest.raises(FileExistsError), receipt_writer(destination) as save:
        destination.write_text("concurrent")
        save(run_handle())
    assert destination.read_text() == "concurrent"
    assert not list(tmp_path.glob(".spark-receipt-*"))


def test_failed_submission_leaves_no_receipt(tmp_path):
    destination = tmp_path / "run.json"
    spark = Mock(last_submitted_run=None)
    spark.submit_app.side_effect = ValueError("submission failed")
    import click

    with pytest.raises(click.exceptions.Exit):
        execute_submission(spark, {}, receipt_file=destination)
    assert not destination.exists()
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize(
    "document",
    [
        {},
        {"schema_version": 2},
        {"schema_version": 1, "run": {}},
        {"schema_version": 1, "run": {"namespace": []}},
    ],
)
def test_cancellation_rejects_invalid_receipts(tmp_path, document):
    path = tmp_path / "receipt.json"
    path.write_text(json.dumps(document))
    with pytest.raises(ValueError):
        read_receipt(path)


def test_cancellation_uses_uid_precondition():
    manager = SparkAppManager(k8s_client_manager=MagicMock())
    api = Mock()
    api.read_namespaced_pod.return_value = k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            uid="original-uid",
            labels={"spark-app-id": "job", "spark-role": "driver"},
        )
    )
    with patch.object(k8s, "CoreV1Api", return_value=api):
        manager.cancel_run(run_handle())
    kwargs = api.delete_namespaced_pod.call_args.kwargs
    assert kwargs["body"].preconditions.uid == "original-uid"
    assert kwargs["body"].propagation_policy == "Foreground"
    assert kwargs["name"] == "job-driver"


@pytest.mark.parametrize(
    "uid,labels",
    [
        ("replacement", {"spark-app-id": "job", "spark-role": "driver"}),
        ("original-uid", {"spark-app-id": "other", "spark-role": "driver"}),
        ("original-uid", {"spark-app-id": "job", "spark-role": "executor"}),
    ],
)
def test_cancellation_rejects_replacement_or_wrong_role(uid, labels):
    manager = SparkAppManager(k8s_client_manager=MagicMock())
    api = Mock()
    api.read_namespaced_pod.return_value = k8s.V1Pod(metadata=k8s.V1ObjectMeta(uid=uid, labels=labels))
    with patch.object(k8s, "CoreV1Api", return_value=api), pytest.raises(ValueError, match="does not match"):
        manager.cancel_run(run_handle())
    api.delete_namespaced_pod.assert_not_called()


def test_legacy_delete_rejects_ambiguous_driver_selection():
    manager = SparkAppManager(k8s_client_manager=MagicMock())
    api = Mock()
    api.list_namespaced_pod.return_value = SimpleNamespace(items=[Mock(), Mock()])
    with patch.object(k8s, "CoreV1Api", return_value=api), pytest.raises(ValueError, match="Multiple driver"):
        manager.delete_app(namespace="spark", app_id="job")
    assert api.list_namespaced_pod.call_args.kwargs["label_selector"] == "spark-app-id=job,spark-role=driver"
    api.delete_namespaced_pod.assert_not_called()


def job_document():
    return {
        "apiVersion": "spark-on-k8s/v1",
        "kind": "SparkJob",
        "metadata": {"name": "test"},
        "spec": {"image": "spark:test", "app_path": "local:///test.py"},
    }


def test_all_profiles_reports_valid_and_invalid_profiles_without_cluster(tmp_path):
    document = job_document()
    document["profiles"] = {
        "dev": {"spec": {"executor_instances": {"initial": 1}}},
        "prod": {"spec": {"executor_instances": {"initial": 9, "max": 3}}},
    }
    path = tmp_path / "job.yaml"
    path.write_text(yaml.safe_dump(document))
    with patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client") as connect:
        result = CliRunner().invoke(job_cli, ["validate", str(path), "--all-profiles", "--output", "json"])
    assert result.exit_code == 1
    payload = json.loads(result.stdout)
    assert [(p["profile"], p["valid"]) for p in payload["profiles"]] == [
        (None, True),
        ("dev", True),
        ("prod", False),
    ]
    connect.assert_not_called()


def test_all_profiles_resolves_relative_templates(tmp_path):
    document = job_document()
    document["profiles"] = {"prod": {"spec": {"driver_pod_template": "missing.yaml"}}}
    path = tmp_path / "job.yaml"
    path.write_text(yaml.safe_dump(document))
    assert JobSpec.validate_profiles(path)[1]["valid"] is False


def plan(**kwargs):
    return SparkOnK8S().plan_app(image="spark:test", app_path="local:///job.py", **kwargs).capacity


def test_resource_plan_static_requests_and_overrides():
    envelope = plan(
        spark_conf={
            "spark.kubernetes.executor.request.cores": "250m",
            "spark.executor.memory": "2g",
            "spark.executor.memoryOverhead": "512m",
        }
    )
    assert envelope["executor_requests"]["memory_mib"] == 2560
    assert envelope["total_requests"]["maximum"] == {"cpu_cores": 1.5, "memory_mib": 6656}
    assert check_budget(envelope, max_cpu=1.5)["passed"]
    assert not check_budget(envelope, max_memory_mib=6000)["passed"]


def test_resource_plan_dynamic_bounds_and_unbounded_peak():
    envelope = plan(executor_instances=ExecutorInstances(min=1, initial=2, max=4))
    assert envelope["executor_counts"] == {"minimum": 1, "initial": 2, "maximum": 4}
    assert envelope["total_requests"]["maximum"]["cpu_cores"] == 5
    unbounded = plan(executor_instances=ExecutorInstances(min=1, initial=2))
    assert unbounded["total_requests"]["maximum"] is None
    assert unbounded["total_requests"]["initial"] is not None
    assert not check_budget(unbounded, max_cpu=100)["passed"]


def test_resource_plan_pyspark_offheap_and_unsupported_template():
    envelope = plan(
        spark_conf={
            "spark.memory.offHeap.enabled": "true",
            "spark.memory.offHeap.size": "1048576",
            "spark.executor.pyspark.memory": "1g",
        }
    )
    assert envelope["executor_requests"]["memory_mib"] == 2561
    unsupported = plan(executor_pod_template_path="local:///executor.yaml")
    assert unsupported["total_requests"]["maximum"] is None
    assert any("template" in reason for reason in unsupported["unknown"])


@pytest.mark.parametrize(
    "conf",
    [
        {"spark.executor.memory": "bad"},
        {"spark.executor.instances": "-1"},
        {"spark.dynamicAllocation.enabled": "invalid"},
    ],
)
def test_invalid_resource_configuration_is_unknown(conf):
    envelope = plan(spark_conf=conf)
    assert envelope["total_requests"]["maximum"] is None
    assert envelope["unknown"]


def test_budget_cli_fails_with_redacted_plan(tmp_path):
    path = tmp_path / "job.yaml"
    path.write_text(yaml.safe_dump(job_document()))
    result = CliRunner().invoke(job_cli, ["plan", str(path), "--max-cpu", "1", "--output", "json"])
    assert result.exit_code == 1
    assert json.loads(result.stdout)["budget"]["passed"] is False


def test_sql_locations_preserve_file_name_and_multiline_statement_offsets(tmp_path):
    path = tmp_path / "queries.sql"
    path.write_text("\nSELECT 'a;b';\n\nSELECT 2;\nSELECT 3")
    options = prepare_sql_options(sql_file=path)
    sources = options["driver_ephemeral_configmaps_volumes"][0]["sources"]
    locations = json.loads(next(source["text"] for source in sources if source["name"] == "locations.json"))
    assert locations["source"] == "queries.sql"
    assert [item["line"] for item in locations["statements"]] == [2, 4, 5]


@pytest.mark.parametrize(
    "conf",
    [
        {"spark.executor.instances": "9" * 400},
        {"spark.executor.memory": "9" * 308 + "m"},
        {"spark.executor.memory": "1" + "0" * 304 + "t"},
        {"spark.kubernetes.executor.request.cores": "1e308"},
        {"spark.memory.offHeap.enabled": "invalid"},
    ],
)
def test_resource_overflow_and_invalid_flags_remain_unknown_and_json_safe(conf):
    envelope = plan(spark_conf=conf)
    assert envelope["total_requests"]["maximum"] is None
    assert envelope["unknown"]
    assert not check_budget(envelope, max_cpu=1000)["passed"]
    json.dumps(envelope, allow_nan=False)


@pytest.mark.parametrize("failure_stage", ["patch_namespaced_secret", "create_namespaced_service"])
@pytest.mark.parametrize("output", ["json", "text"])
def test_partial_submission_preserves_uid_phase_and_receipt(tmp_path, capsys, failure_stage, output):
    import click
    from kubernetes.client.exceptions import ApiException

    destination = tmp_path / "partial.json"
    spark = SparkOnK8S(k8s_client_manager=MagicMock())
    api = Mock()
    api.create_namespaced_pod.return_value = k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(name="job-driver", uid="original-uid")
    )
    getattr(api, failure_stage).side_effect = ApiException(status=403, reason="Forbidden")
    with patch.object(k8s, "CoreV1Api", return_value=api), pytest.raises(click.exceptions.Exit):
        execute_submission(
            spark,
            {
                "image": "spark:test",
                "app_path": "local:///job.py",
                "namespace": "spark",
                "app_name": "job",
                "app_id_suffix": lambda: "",
                "app_waiter": "wait",
                "secret_values": {"password": "private-secret"},
            },
            output=output,
            receipt_file=destination,
        )
    captured = capsys.readouterr()
    if output == "json":
        result = json.loads(captured.out)
        assert result["run"] == run_handle().to_dict()
        assert result["submission_phase"] == "driver_created"
        assert result["error"]["code"] == "api_error"
    else:
        assert (
            "Driver created: job-driver in spark (application job); submission did not complete."
            in captured.err
        )
        assert "Kubernetes API 403: Forbidden" in captured.err
        assert "Submitted job" not in captured.out
    assert read_receipt(destination) == run_handle()
    api.delete_namespaced_pod.assert_not_called()
    assert "private-secret" not in captured.out + captured.err


@pytest.mark.parametrize("submission_fails", [True, False])
@pytest.mark.parametrize("output", ["json", "text"])
def test_receipt_io_failure_retains_known_identity_and_primary_error(
    tmp_path, capsys, submission_fails, output
):
    import click
    from kubernetes.client.exceptions import ApiException

    spark = Mock(last_submitted_run=run_handle(), last_submission_phase="driver_created")
    if submission_fails:
        spark.submit_app.side_effect = ApiException(status=403, reason="Forbidden")
    with (
        patch("spark_on_k8s.cli.receipts.os.link", side_effect=PermissionError("cannot publish")),
        pytest.raises(click.exceptions.Exit),
    ):
        execute_submission(
            spark,
            {"namespace": "spark", "app_waiter": "wait"},
            output=output,
            receipt_file=tmp_path / "run.json",
        )
    captured = capsys.readouterr()
    if output == "json":
        result = json.loads(captured.out)
        assert result["run"] == run_handle().to_dict()
        assert result["receipt_error"]["code"] == "receipt_write_error"
        assert result["error"]["code"] == ("api_error" if submission_fails else "operation_error")
    else:
        assert "Could not save receipt for job-driver in spark (application job)." in captured.err
        expected = "Kubernetes API 403: Forbidden" if submission_fails else "cannot publish"
        assert f"Error: {expected}" in captured.err
        assert ("Driver created:" in captured.err) is submission_fails
        assert "Submitted job" not in captured.out
    spark.app_manager.wait_for_app.assert_not_called()
    assert not list(tmp_path.iterdir())


def test_receipt_unwritable_parent_is_checked_before_submission(tmp_path):
    import click

    spark = Mock(last_submitted_run=None)
    with pytest.raises(click.exceptions.Exit):
        execute_submission(spark, {}, receipt_file=tmp_path / "missing" / "run.json")
    spark.submit_app.assert_not_called()


def test_dangling_receipt_symlink_is_not_replaced(tmp_path):
    destination = tmp_path / "run.json"
    destination.symlink_to(tmp_path / "absent.json")
    with pytest.raises(FileExistsError), receipt_writer(destination):
        pytest.fail("Existing symlink must be rejected")
    assert destination.is_symlink()


def test_unconfirmed_driver_creation_does_not_publish_or_cancel_a_run(tmp_path, capsys):
    import click
    from kubernetes.client.exceptions import ApiException

    spark = SparkOnK8S(k8s_client_manager=MagicMock())
    spark.last_submitted_run = run_handle()
    api = Mock()
    api.create_namespaced_pod.side_effect = ApiException(status=500, reason="Uncertain create result")
    destination = tmp_path / "run.json"
    with patch.object(k8s, "CoreV1Api", return_value=api), pytest.raises(click.exceptions.Exit):
        execute_submission(
            spark,
            {"image": "spark:test", "app_path": "local:///job.py", "app_waiter": "no_wait"},
            output="json",
            receipt_file=destination,
        )
    result = json.loads(capsys.readouterr().out)
    assert result["run"] is None
    assert result["submission_phase"] == "creating_resources"
    assert not destination.exists()
    api.delete_namespaced_pod.assert_not_called()
    api.create_namespaced_pod.assert_called_once()


def test_sql_source_metadata_excludes_parent_paths_and_control_characters():
    from spark_on_k8s.sql import prepare_sql_configmap

    sources = prepare_sql_configmap("SELECT 1", source_name="C:\\private\\customer\\query\nname.sql")[
        "sources"
    ]
    locations = json.loads(next(item["text"] for item in sources if item["name"] == "locations.json"))
    assert locations["source"] == "query_name.sql"
    assert "private" not in json.dumps(locations)


def test_all_profiles_and_one_profile_are_mutually_exclusive_before_planning(tmp_path):
    path = tmp_path / "job.yaml"
    path.write_text(yaml.safe_dump(job_document()))
    with patch.object(JobSpec, "from_file") as load, patch.object(JobSpec, "validate_profiles") as matrix:
        result = CliRunner().invoke(job_cli, ["validate", str(path), "--all-profiles", "--profile", "prod"])
    assert result.exit_code == 2
    assert "Choose --profile or --all-profiles, not both" in result.stderr
    load.assert_not_called()
    matrix.assert_not_called()


@pytest.mark.parametrize("quantity", [float("nan"), float("inf"), -1])
def test_nonfinite_or_negative_driver_request_does_not_produce_passing_capacity(quantity):
    from spark_on_k8s.client import PodResources

    envelope = plan(driver_resources=PodResources(cpu=quantity))
    assert envelope["driver_requests"] is None
    assert envelope["total_requests"]["maximum"] is None
    assert not check_budget(envelope, max_cpu=100)["passed"]
    json.dumps(envelope, allow_nan=False)


def test_conflicting_effective_executor_bounds_cannot_pass_budget():
    envelope = plan(
        spark_conf={
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.minExecutors": "4",
            "spark.dynamicAllocation.maxExecutors": "3",
        }
    )
    assert envelope["executor_counts"] is None
    assert envelope["total_requests"]["maximum"] is None
    assert not check_budget(envelope, max_cpu=100, max_memory_mib=100_000)["passed"]


@pytest.mark.parametrize("extra", ["sidecar", "init", "overhead"])
def test_unaccounted_driver_template_resources_fail_closed_for_budget(extra):
    spec = {"containers": [{"name": "driver"}]}
    companion = {
        "name": "metrics",
        "image": "metrics:test",
        "resources": {"requests": {"cpu": "250m", "memory": "64Mi"}},
    }
    if extra == "sidecar":
        spec["containers"].append(companion)
    elif extra == "init":
        spec["initContainers"] = [companion]
    else:
        spec["overhead"] = {"cpu": "250m", "memory": "64Mi"}
    envelope = plan(driver_pod_template={"apiVersion": "v1", "kind": "Pod", "spec": spec})
    assert envelope["driver_requests"] is not None
    assert envelope["total_requests"]["maximum"] is None
    assert not check_budget(envelope, max_cpu=100)["passed"]
    assert any("separate capacity review" in message for message in envelope["unknown"])


@pytest.mark.parametrize("limit", [0, -1, float("nan"), float("inf")])
def test_invalid_budget_is_rejected_instead_of_becoming_a_pass(limit):
    envelope = plan()
    with pytest.raises(ValueError):
        check_budget(envelope, max_cpu=limit)
