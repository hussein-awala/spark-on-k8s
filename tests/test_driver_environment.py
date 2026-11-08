from __future__ import annotations

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner
from kubernetes import client as k8s
from spark_on_k8s.client import SparkOnK8S
from spark_on_k8s.jobs import JobSpec
from spark_on_k8s.utils.configuration import Configuration

PREFIX = "spark.kubernetes.driverEnv."
OPTIONS = {
    "image": "spark:test",
    "app_path": "local:///opt/spark/job.py",
    "app_name": "environment",
    "namespace": "analytics",
    "service_account": "spark",
    "app_id_suffix": lambda: "",
    "app_waiter": "no_wait",
}


@pytest.fixture
def submission(monkeypatch):
    manager = MagicMock()
    api = MagicMock()
    api.create_namespaced_pod.return_value = SimpleNamespace(
        metadata=SimpleNamespace(name="environment-driver", uid="driver-uid")
    )
    monkeypatch.setattr(k8s, "CoreV1Api", MagicMock(return_value=api))
    return SimpleNamespace(sdk=SparkOnK8S(k8s_client_manager=manager), manager=manager, api=api)


def driver(plan):
    return next(resource for resource in plan.resources if resource["kind"] == "Pod")["spec"]["containers"][0]


def test_properties_populate_driver_environment_and_preserve_spark_arguments(submission):
    values = {
        "DATABASE_NAME": "dbname",
        "EMPTY": "",
        "ENABLED": True,
        "DISABLED": False,
        "COUNT": 0,
        "RATIO": 0.25,
        "dotted.name": "dots",
        "hyphen-name": "hyphens",
        "1START": "digit",
        "WITH SPACE": "space",
        "COMPLEX": "spaces = values\nand newlines",
    }
    expected = {**values, "ENABLED": "true", "DISABLED": "false", "COUNT": "0", "RATIO": "0.25"}
    conf = {
        **{PREFIX + name: value for name, value in values.items()},
        "spark.executorEnv.EXECUTOR_ONLY": "executor-value",
        "spark.driverEnv.OTHER_PREFIX": "unchanged",
        "spark.kubernetes.driverEnv": "unchanged",
        "spark.sql.shuffle.partitions": 3,
    }
    before = deepcopy(conf)
    plan = submission.sdk.plan_app(**OPTIONS, spark_conf=conf)
    container = driver(plan)
    assert {entry["name"]: entry["value"] for entry in container["env"] if "value" in entry} == expected
    assert container["env"][-1] == {
        "name": "SPARK_DRIVER_BIND_ADDRESS",
        "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}},
    }
    for name, value in expected.items():
        assert f"{PREFIX}{name}={value}" in container["args"]
    assert "spark.executorEnv.EXECUTOR_ONLY=executor-value" in container["args"]
    assert "spark.driverEnv.OTHER_PREFIX=unchanged" in container["args"]
    assert "spark.kubernetes.driverEnv=unchanged" in container["args"]
    assert "spark.sql.shuffle.partitions=3" in container["args"]
    assert conf == before
    submission.manager.client.assert_not_called()


def test_resolved_configuration_defaults_supply_driver_environment(submission, monkeypatch):
    conf = {PREFIX + "DATABASE_NAME": "configured"}
    monkeypatch.setattr(Configuration, "SPARK_ON_K8S_SPARK_CONF", conf)
    plan = submission.sdk.plan_app(**OPTIONS)
    assert {"name": "DATABASE_NAME", "value": "configured"} in driver(plan)["env"]
    assert conf == {PREFIX + "DATABASE_NAME": "configured"}
    submission.manager.client.assert_not_called()


def test_explicit_properties_replace_template_sources_and_preserve_secret_imports(submission):
    template = {
        "apiVersion": "v1",
        "kind": "Pod",
        "spec": {
            "containers": [
                {
                    "name": "sidecar",
                    "image": "helper",
                    "env": [{"name": "DATABASE_NAME", "value": "sidecar"}],
                },
                {
                    "name": "spark-main",
                    "env": [
                        {
                            "name": "DATABASE_NAME",
                            "valueFrom": {"secretKeyRef": {"name": "old", "key": "db"}},
                        },
                        {"name": "TEMPLATE_ONLY", "value": "retained"},
                        {"name": "SPARK_DRIVER_BIND_ADDRESS", "value": "wrong"},
                    ],
                    "envFrom": [{"configMapRef": {"name": "settings"}}],
                },
            ]
        },
    }
    before = deepcopy(template)
    plan = submission.sdk.plan_app(
        **OPTIONS,
        spark_conf={PREFIX + "DATABASE_NAME": "explicit"},
        driver_pod_template=template,
        driver_container_name="spark-main",
        secret_values={"DATABASE_NAME": "secret-value"},
        driver_env_vars_from_secrets=["existing-secret"],
    )
    container = driver(plan)
    variables = {entry["name"]: entry for entry in container["env"]}
    assert variables == {
        "DATABASE_NAME": {"name": "DATABASE_NAME", "value": "explicit"},
        "TEMPLATE_ONLY": {"name": "TEMPLATE_ONLY", "value": "retained"},
        "SPARK_DRIVER_BIND_ADDRESS": {
            "name": "SPARK_DRIVER_BIND_ADDRESS",
            "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}},
        },
    }
    assert container["envFrom"] == [
        {"configMapRef": {"name": "settings"}},
        {"secretRef": {"name": "environment"}},
        {"secretRef": {"name": "existing-secret"}},
    ]
    assert (
        "spark.kubernetes.executor.secretKeyRef.DATABASE_NAME=environment:DATABASE_NAME" in container["args"]
    )
    pod = next(resource for resource in plan.resources if resource["kind"] == "Pod")
    assert pod["spec"]["containers"][1] == before["spec"]["containers"][0]
    assert template == before
    submission.manager.client.assert_not_called()


@pytest.mark.parametrize(
    "name", ["", "HAS=EQUALS", "NEW\nLINE", "CONTROL\x00", "DEL\x7f", "UNICODEé", "SPARK_DRIVER_BIND_ADDRESS"]
)
def test_invalid_or_managed_names_fail_before_any_cluster_operations(submission, name):
    with pytest.raises(ValueError) as error:
        submission.sdk.submit_app(
            **OPTIONS,
            spark_conf={PREFIX + name: "private-sentinel"},
            secret_values={"TOKEN": "another-private-value"},
        )
    assert "private-sentinel" not in str(error.value)
    assert "another-private-value" not in str(error.value)
    submission.manager.client.assert_not_called()
    assert submission.api.mock_calls == []


@pytest.mark.parametrize(
    "value",
    [
        None,
        ["private-sentinel"],
        {"nested": "private-sentinel"},
        b"private-sentinel",
        float("nan"),
        float("inf"),
        float("-inf"),
        "private-sentinel\0",
    ],
)
def test_invalid_values_fail_before_any_cluster_operations_without_disclosure(submission, value):
    with pytest.raises(ValueError) as error:
        submission.sdk.submit_app(
            **OPTIONS,
            spark_conf={PREFIX + "DATABASE_NAME": value},
            secret_values={"TOKEN": "another-private-value"},
        )
    assert "private-sentinel" not in str(error.value)
    assert "another-private-value" not in str(error.value)
    submission.manager.client.assert_not_called()
    assert submission.api.mock_calls == []


def test_plan_redacts_environment_values_in_both_pod_and_spark_arguments(submission):
    plan = submission.sdk.plan_app(**OPTIONS, spark_conf={PREFIX + "DATABASE_NAME": "private-database-value"})
    shown = plan.to_dict()
    shown_driver = next(resource for resource in shown["resources"] if resource["kind"] == "Pod")["spec"][
        "containers"
    ][0]
    assert {"name": "DATABASE_NAME", "value": "<redacted>"} in shown_driver["env"]
    assert PREFIX + "DATABASE_NAME=<redacted>" in shown_driver["args"]
    assert "private-database-value" not in json.dumps(shown)
    assert {"name": "DATABASE_NAME", "value": "private-database-value"} in driver(plan)["env"]
    assert PREFIX + "DATABASE_NAME=private-database-value" in driver(plan)["args"]


def test_cli_conf_reaches_the_submitted_driver_pod(submission, monkeypatch):
    from spark_on_k8s.cli.app import app_cli

    monkeypatch.setattr("spark_on_k8s.client.KubernetesClientManager", lambda: submission.manager)
    result = CliRunner().invoke(
        app_cli,
        [
            "submit",
            "--image",
            "spark:test",
            "--path",
            "local:///job.py",
            "--name",
            "environment",
            "--conf",
            PREFIX + "DATABASE_NAME=cli=value",
            "--conf",
            PREFIX + "EMPTY=",
            "--conf",
            "spark.executorEnv.EXECUTOR_ONLY=executor-value",
            "--output",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    pod = submission.api.create_namespaced_pod.call_args.kwargs["body"]
    with k8s.ApiClient() as serializer:
        container = serializer.sanitize_for_serialization(pod)["spec"]["containers"][0]
    assert {"name": "DATABASE_NAME", "value": "cli=value"} in container["env"]
    assert {"name": "EMPTY", "value": ""} in container["env"]
    assert all(variable["name"] != "EXECUTOR_ONLY" for variable in container["env"])
    assert "spark.executorEnv.EXECUTOR_ONLY=executor-value" in container["args"]
    assert json.loads(result.stdout)["run"]["driver_uid"] == "driver-uid"


def test_job_yaml_properties_reach_offline_driver_plan(tmp_path, monkeypatch):
    connect = MagicMock(side_effect=AssertionError("Planning must not contact Kubernetes"))
    monkeypatch.setattr("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client", connect)
    job = tmp_path / "job.yaml"
    job.write_text(
        """apiVersion: spark-on-k8s/v1
kind: SparkJob
metadata:
  name: environment
spec:
  image: spark:test
  app_path: local:///job.py
  spark_conf:
    spark.kubernetes.driverEnv.DATABASE_NAME: yaml-database
    spark.kubernetes.driverEnv.EMPTY: ''
    spark.kubernetes.driverEnv.ENABLED: true
    spark.kubernetes.driverEnv.COUNT: 3
    spark.executorEnv.EXECUTOR_ONLY: executor-value
"""
    )
    plan = JobSpec.from_file(job).plan()
    assert {entry["name"]: entry["value"] for entry in driver(plan)["env"] if "value" in entry} == {
        "DATABASE_NAME": "yaml-database",
        "EMPTY": "",
        "ENABLED": "true",
        "COUNT": "3",
    }
    assert "spark.executorEnv.EXECUTOR_ONLY=executor-value" in driver(plan)["args"]
    assert "yaml-database" not in json.dumps(plan.to_dict())
    connect.assert_not_called()
