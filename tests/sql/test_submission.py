from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from unittest import mock

import pytest
from spark_on_k8s.client import SparkOnK8S
from spark_on_k8s.sql import SQL_APP_PATH, SQL_ARGUMENTS, prepare_sql_configmap


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT 'a;b'; SELECT 2;", ["SELECT 'a;b'", "SELECT 2"]),
        ('SELECT "a;b"; SELECT `c;d` FROM t;', ['SELECT "a;b"', "SELECT `c;d` FROM t"]),
        ("SELECT 'it''s;a';", ["SELECT 'it''s;a'"]),
        (
            "/* ignored; */ SELECT 1; -- also; ignored\nSELECT 2;",
            ["/* ignored; */ SELECT 1; -- also; ignored", "SELECT 2"],
        ),
    ],
)
def test_sql_statement_boundaries(sql, expected):
    configmap = prepare_sql_configmap(sql)
    assert json.loads(configmap["sources"][1]["text"]) == expected
    runner = Path(configmap["sources"][0]["text_path"])
    assert runner.is_file() and runner.parent.name == "sql"


@pytest.mark.parametrize("sql", ["", " ", "-- only a comment;", "/* no query */; ;"])
def test_empty_submission_is_rejected(sql):
    with pytest.raises(ValueError, match="at least one statement"):
        prepare_sql_configmap(sql)


def test_missing_optional_parser_has_actionable_message(monkeypatch):
    monkeypatch.setitem(sys.modules, "sqlparse", None)
    with pytest.raises(ImportError, match=r"spark-on-k8s\[sql\]"):
        prepare_sql_configmap("SELECT 1")


@pytest.mark.parametrize("inputs", [{}, {"sql": "SELECT 1", "sql_file": "queries.sql"}])
def test_sdk_requires_one_sql_source(inputs):
    with mock.patch.object(SparkOnK8S, "submit_app", autospec=True) as submit:
        with pytest.raises(ValueError, match="exactly one"):
            SparkOnK8S().submit_sql(**inputs)
    submit.assert_not_called()


@pytest.mark.parametrize("managed_option", ["app_path", "app_arguments", "class_name"])
def test_sdk_rejects_overriding_runner_contract(managed_option):
    with mock.patch.object(SparkOnK8S, "submit_app", autospec=True) as submit:
        with pytest.raises(ValueError, match="manages these options"):
            SparkOnK8S().submit_sql(sql="SELECT 1", **{managed_option: "override"})
    submit.assert_not_called()


def test_sdk_reads_local_file_preserves_options_and_additional_configmaps(tmp_path):
    queries = tmp_path / "queries.sql"
    queries.write_text("SELECT 'café;value'; SELECT 2", encoding="utf-8")
    configmaps = [{"mount_path": "/settings", "sources": [{"name": "setting", "text": "value"}]}]
    client = SparkOnK8S()
    with mock.patch.object(SparkOnK8S, "submit_app", autospec=True, return_value="sql-driver") as submit:
        result = client.submit_sql(
            sql_file=queries,
            image="spark:4.3.0",
            namespace="analytics",
            service_account="spark",
            app_waiter="wait",
            spark_conf={"spark.sql.shuffle.partitions": "2"},
            driver_ephemeral_configmaps_volumes=configmaps,
        )
    assert result == "sql-driver"
    options = submit.call_args.kwargs
    assert options["app_path"] == SQL_APP_PATH
    assert options["app_arguments"] == SQL_ARGUMENTS
    assert options["class_name"] is None
    assert options["image"] == "spark:4.3.0"
    assert options["namespace"] == "analytics"
    assert options["app_waiter"] == "wait"
    assert options["spark_conf"] == {"spark.sql.shuffle.partitions": "2"}
    assert len(configmaps) == 1
    assert options["driver_ephemeral_configmaps_volumes"][0] == configmaps[0]
    sources = options["driver_ephemeral_configmaps_volumes"][1]["sources"]
    assert json.loads(sources[1]["text"]) == ["SELECT 'café;value'", "SELECT 2"]


def test_sdk_rejects_configmap_mount_collision():
    with mock.patch.object(SparkOnK8S, "submit_app", autospec=True) as submit:
        with pytest.raises(ValueError, match="reserves the /configmap"):
            SparkOnK8S().submit_sql(
                sql="SELECT 1", driver_ephemeral_configmaps_volumes=[{"mount_path": "/configmap/"}]
            )
    submit.assert_not_called()


def test_core_client_and_cli_help_do_not_import_airflow_or_sqlparse():
    code = """
import importlib.abc
import sys
class BlockOptional(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname.split('.')[0] in {'airflow', 'sqlparse', 'pyspark'}:
            raise ImportError('Optional dependency must not be imported: ' + fullname)
sys.meta_path.insert(0, BlockOptional())
from spark_on_k8s.client import SparkOnK8S
from spark_on_k8s.cli import main
sys.argv = ['spark-on-k8s', 'sql', 'submit', '--help']
main()
"""
    result = subprocess.run([sys.executable, "-B", "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert "--file" in result.stdout and "--sql" in result.stdout


def test_sdk_rejects_volume_mount_collision():
    from kubernetes import client as k8s

    with mock.patch.object(SparkOnK8S, "submit_app", autospec=True) as submit:
        with pytest.raises(ValueError, match="reserves the /configmap"):
            SparkOnK8S().submit_sql(
                sql="SELECT 1",
                driver_volume_mounts=[k8s.V1VolumeMount(name="settings", mount_path="/configmap")],
            )
    submit.assert_not_called()


def test_prepared_options_are_independent_copies():
    from spark_on_k8s.sql import prepare_sql_options

    configmaps = [{"mount_path": "/settings", "sources": [{"name": "x", "text": "original"}]}]
    conf = {"spark.sql.shuffle.partitions": "2"}
    prepared = prepare_sql_options(
        sql="SELECT 1", driver_ephemeral_configmaps_volumes=configmaps, spark_conf=conf
    )
    prepared["driver_ephemeral_configmaps_volumes"][0]["sources"][0]["text"] = "changed"
    prepared["spark_conf"]["spark.sql.shuffle.partitions"] = "99"
    assert configmaps[0]["sources"][0]["text"] == "original"
    assert conf["spark.sql.shuffle.partitions"] == "2"


def test_sql_dry_run_is_offline_and_preserves_managed_runner_mount():
    from spark_on_k8s.planning import ApplicationPlan

    template = {
        "apiVersion": "v1",
        "kind": "Pod",
        "spec": {
            "containers": [
                {"name": "driver", "volumeMounts": [{"name": "template", "mountPath": "/configmap"}]}
            ],
            "volumes": [{"name": "template", "emptyDir": {}}],
        },
    }
    with mock.patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client") as connect:
        plan = SparkOnK8S().submit_sql(
            sql="SELECT 'private-query-value'",
            image="spark:test",
            namespace="analytics",
            dry_run=True,
            driver_pod_template=template,
        )
    connect.assert_not_called()
    assert isinstance(plan, ApplicationPlan)
    configmap = next(resource for resource in plan.resources if resource["kind"] == "ConfigMap")
    pod = next(resource for resource in plan.resources if resource["kind"] == "Pod")
    mounts = [
        mount for mount in pod["spec"]["containers"][0]["volumeMounts"] if mount["mountPath"] == "/configmap"
    ]
    assert len(mounts) == 1
    assert mounts[0]["name"] == configmap["metadata"]["name"]
    assert template["spec"]["containers"][0]["volumeMounts"][0]["name"] == "template"
    assert "private-query-value" not in json.dumps(plan.to_dict())


def test_non_string_sql_is_rejected_without_submission():
    with mock.patch.object(SparkOnK8S, "submit_app", autospec=True) as submit:
        with pytest.raises(TypeError, match="SQL must be a string"):
            SparkOnK8S().submit_sql(sql=["SELECT 1"])
    submit.assert_not_called()


def test_missing_sql_file_is_rejected_without_submission(tmp_path):
    with mock.patch.object(SparkOnK8S, "submit_app", autospec=True) as submit:
        with pytest.raises(FileNotFoundError):
            SparkOnK8S().submit_sql(sql_file=tmp_path / "missing.sql")
    submit.assert_not_called()


def test_preparation_preserves_notset_identity_for_default_resolution():
    from spark_on_k8s.sql import prepare_sql_options
    from spark_on_k8s.utils.types import NOTSET

    prepared = prepare_sql_options(
        sql="SELECT 1",
        spark_conf=NOTSET,
        driver_volume_mounts=NOTSET,
        driver_ephemeral_configmaps_volumes=NOTSET,
    )
    assert prepared["spark_conf"] is NOTSET
    assert prepared["driver_volume_mounts"] is NOTSET
    assert len(prepared["driver_ephemeral_configmaps_volumes"]) == 1


def test_empty_sql_compatibility_mode_preserves_airflow_noop():
    configmap = prepare_sql_configmap("/* only comments */;", allow_empty=True)
    assert json.loads(configmap["sources"][1]["text"]) == []


@pytest.mark.parametrize(
    "path", ["/configmap/queries.json", "/configmap/spark_sql.py", "//configmap/./queries.json"]
)
@pytest.mark.parametrize("source", ["volume", "configmap", "template"])
def test_nested_mounts_cannot_shadow_sql_or_runner(path, source):
    from kubernetes import client as k8s
    from spark_on_k8s.sql import prepare_sql_options

    if source == "volume":
        options = {"driver_volume_mounts": [k8s.V1VolumeMount(name="override", mount_path=path)]}
    elif source == "configmap":
        options = {"driver_ephemeral_configmaps_volumes": [{"mount_path": path, "sources": []}]}
    else:
        options = {
            "driver_pod_template": {
                "apiVersion": "v1",
                "kind": "Pod",
                "spec": {
                    "containers": [
                        {"name": "driver", "volumeMounts": [{"name": "override", "mountPath": path}]}
                    ]
                },
            }
        }
    with pytest.raises(ValueError, match="descendants"):
        prepare_sql_options(sql="SELECT 1", **options)


def test_only_selected_template_driver_has_reserved_mounts(tmp_path):
    import yaml
    from spark_on_k8s.sql import prepare_sql_options

    template = {
        "apiVersion": "v1",
        "kind": "Pod",
        "spec": {
            "containers": [
                {
                    "name": "metrics",
                    "image": "metrics:test",
                    "volumeMounts": [{"name": "settings", "mountPath": "/configmap/queries.json"}],
                },
                {"name": "custom-driver"},
            ]
        },
    }
    file = tmp_path / "driver.yaml"
    file.write_text(yaml.safe_dump(template))
    prepared = prepare_sql_options(
        sql="SELECT 1", driver_pod_template=file, driver_container_name="custom-driver"
    )
    file.unlink()
    assert prepared["driver_pod_template"] == template
    assert prepared["driver_pod_template"] is not template


def test_sole_template_container_is_the_sql_driver():
    from spark_on_k8s.sql import prepare_sql_options

    template = {
        "apiVersion": "v1",
        "kind": "Pod",
        "spec": {
            "containers": [
                {"name": "custom", "volumeMounts": [{"name": "x", "mountPath": "/configmap/queries.json"}]}
            ]
        },
    }
    with pytest.raises(ValueError, match="descendants"):
        prepare_sql_options(sql="SELECT 1", driver_pod_template=template)


def test_ambiguous_template_selection_still_fails_before_kubernetes():
    template = {
        "apiVersion": "v1",
        "kind": "Pod",
        "spec": {
            "containers": [{"name": "one", "image": "spark:test"}, {"name": "two", "image": "spark:test"}]
        },
    }
    with mock.patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client") as connect:
        with pytest.raises(ValueError, match="ambiguous"):
            SparkOnK8S().submit_sql(sql="SELECT 1", image="spark:test", driver_pod_template=template)
    connect.assert_not_called()
