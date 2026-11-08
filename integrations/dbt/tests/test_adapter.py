from __future__ import annotations

import multiprocessing
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from dbt.adapters.contracts.connection import Connection, ConnectionState, LazyHandle
from dbt.adapters.spark import SparkAdapter
from dbt.adapters.spark.connections import SparkConnectionManager
from dbt.adapters.spark_on_k8s import Plugin
from dbt.adapters.spark_on_k8s.connections import ManagedPyhiveConnectionWrapper, SparkOnK8SConnectionManager
from dbt.adapters.spark_on_k8s.credentials import SparkOnK8SCredentials
from dbt.adapters.spark_on_k8s.impl import SparkOnK8SAdapter
from dbt_common.exceptions import DbtRuntimeError
from spark_on_k8s_dbt.lifecycle import Endpoint, invocation_scope


def credentials(**overrides):
    return SparkOnK8SCredentials(schema="analytics", compute={"image": "fixture"}, **overrides)


def manager(creds, owner):
    with invocation_scope(owner):
        return SparkOnK8SConnectionManager(SimpleNamespace(credentials=creds), multiprocessing.get_context())


def connection(creds, name="model"):
    return Connection(type="spark_on_k8s", name=name, credentials=creds)


def test_profile_deserialization_and_spark_parent_plugin():
    creds = SparkOnK8SCredentials.from_dict(
        {"schema": "analytics", "method": "connect", "compute": {"image": "test"}}
    )
    assert creds.type == "spark_on_k8s"
    assert creds.host == "managed"
    assert creds.port == 0
    assert creds.query_retries == 0
    assert creds.server_side_parameters["spark.sql.ansi.enabled"] == "false"
    assert creds._connection_keys() == ("method", "schema", "user")
    assert Plugin.dependencies == ["spark"]
    assert issubclass(SparkOnK8SAdapter, SparkAdapter)
    assert SparkOnK8SAdapter.type() == "spark_on_k8s"


@pytest.mark.parametrize(
    "values, message",
    [
        ({"method": "session"}, "only method"),
        ({"method": "connect", "query_timeout": 30}, "query_timeout"),
        ({"query_retries": 1}, "must be 0"),
        ({"host": "external"}, "Do not set host"),
        ({"port": 10000}, "Do not set host"),
        ({"auth": "KERBEROS"}, "auth: NONE"),
        ({"token": "secret"}, "auth: NONE"),
        ({"password": "secret"}, "auth: NONE"),
        ({"use_ssl": True}, "auth: NONE"),
    ],
)
def test_invalid_profile_settings_fail_early(values, message):
    with pytest.raises(DbtRuntimeError, match=message):
        credentials(**values)


@pytest.mark.parametrize("schema", [None, "", "  "])
def test_schema_is_required(schema):
    with pytest.raises(DbtRuntimeError, match="non-empty schema"):
        SparkOnK8SCredentials(schema=schema, compute={"image": "test"})


@pytest.mark.parametrize("compute", [None, {}, []])
def test_compute_mapping_is_required(compute):
    with pytest.raises(DbtRuntimeError, match="compute mapping"):
        SparkOnK8SCredentials(schema="test", compute=compute)


def test_plain_dbt_cannot_bypass_invocation_owner():
    with pytest.raises(DbtRuntimeError, match="spark-on-k8s-dbt run"):
        SparkOnK8SConnectionManager(SimpleNamespace(credentials=credentials()), multiprocessing.get_context())


def test_connect_open_is_lazy_and_captures_owner_before_worker_threads(monkeypatch):
    profile = credentials(method="connect")
    owner = Mock()
    owner.ensure_endpoint.return_value = Endpoint("localhost", 1234, Mock())
    wrapper = Mock()
    constructor = Mock(return_value=wrapper)
    monkeypatch.setattr("dbt.adapters.spark_on_k8s.connections.ConnectConnectionWrapper", constructor)
    connections = manager(profile, owner)
    owner.ensure_endpoint.assert_not_called()
    conn = connection(profile)
    connections.open(conn)
    assert conn.state == ConnectionState.OPEN
    assert conn.handle is wrapper
    assert connections.open(conn) is conn
    owner.ensure_endpoint.assert_called_once_with("connect", profile.compute)
    constructor.assert_called_once_with(
        owner.ensure_endpoint.return_value.connect_profile, profile.server_side_parameters
    )
    connections.set_thread_connection(conn)
    connections.cleanup_all()
    wrapper.close.assert_called_once_with()
    owner.close.assert_not_called()
    # dbt docs generate does more work after its compilation task closes clients.
    second = connection(profile, "catalog")
    connections.open(second)
    assert owner.ensure_endpoint.call_count == 2
    assert second.state == ConnectionState.OPEN


def test_thrift_endpoint_does_not_mutate_shared_credentials(monkeypatch):
    profile = credentials()
    owner = Mock()
    owner.ensure_endpoint.return_value = Endpoint("127.0.0.1", 34123)
    raw = Mock()

    def spark_open(cls, conn):
        assert conn.credentials.host == "127.0.0.1"
        assert conn.credentials.port == 34123
        conn.handle = SimpleNamespace(handle=raw)
        conn.state = ConnectionState.OPEN
        return conn

    monkeypatch.setattr(SparkConnectionManager, "open", classmethod(spark_open))
    connections = manager(profile, owner)
    first, second = connection(profile), connection(profile, "other-thread")
    connections.open(first)
    connections.open(second)
    assert profile.host == "managed" and profile.port == 0
    assert first.credentials is not profile and first.credentials is not second.credentials
    assert isinstance(first.handle, ManagedPyhiveConnectionWrapper)
    assert first.handle.query_retries == 0
    first.credentials.compute["image"] = "changed"
    assert profile.compute == {"image": "fixture"}


def test_failed_open_sets_failure_state(monkeypatch):
    owner = Mock()
    owner.ensure_endpoint.return_value = Endpoint("localhost", 1234, Mock())
    monkeypatch.setattr(
        "dbt.adapters.spark_on_k8s.connections.ConnectConnectionWrapper",
        Mock(side_effect=ValueError("failed")),
    )
    profile = credentials(method="connect")
    conn = connection(profile)
    with pytest.raises(ValueError, match="failed"):
        manager(profile, owner).open(conn)
    assert conn.state == ConnectionState.FAIL


def test_pyhive_wrapper_preserves_numeric_precision_and_fetch_limit():
    raw = Mock()
    wrapper = ManagedPyhiveConnectionWrapper(raw)
    cursor = wrapper.cursor()
    large = 2**60 + 1
    amount = Decimal("123456789012345678.123456")
    timestamp = datetime(2026, 1, 2, 3, 4, 5, 123456)
    assert wrapper._fix_binding(large) == large
    assert wrapper._fix_binding(amount) == "123456789012345678.123456"
    assert wrapper._fix_binding(timestamp) is timestamp
    assert wrapper._fix_binding(None) is None
    cursor.fetchmany(2)
    raw.cursor.return_value.fetchmany.assert_called_once_with(2)
    cursor.fetchone()
    raw.cursor.return_value.fetchone.assert_called_once_with()


def test_python_models_are_explicitly_rejected():
    adapter = SparkOnK8SAdapter.__new__(SparkOnK8SAdapter)
    with pytest.raises(DbtRuntimeError, match="SQL models only"):
        adapter.submit_python_job({"config": {}}, "def model(): pass")
    assert adapter.python_submission_helpers == {}


def test_cleanup_does_not_resolve_failed_lazy_connection_again():
    owner = Mock()
    owner.ensure_endpoint.side_effect = RuntimeError("startup failed")
    profile = credentials(method="connect")
    connections = manager(profile, owner)
    conn = connection(profile)
    conn.handle = LazyHandle(connections.open)
    connections.set_thread_connection(conn)
    with pytest.raises(RuntimeError, match="startup failed"):
        _ = conn.handle
    connections.cleanup_all()
    owner.ensure_endpoint.assert_called_once_with("connect", profile.compute)
    assert conn.state == ConnectionState.CLOSED


def test_cancel_does_not_resolve_unused_lazy_connections():
    owner = Mock()
    profile = credentials(method="connect")
    connections = manager(profile, owner)
    conn = connection(profile, "unused-worker")
    conn.handle = LazyHandle(connections.open)
    connections.thread_connections[(9999, 9999)] = conn
    assert connections.cancel_open() == ["unused-worker"]
    owner.ensure_endpoint.assert_not_called()
    connections.cleanup_all()
    owner.ensure_endpoint.assert_not_called()


def test_cancel_during_startup_prevents_opening_the_ready_endpoint(monkeypatch):
    owner = Mock()
    profile = credentials(method="connect")
    connections = manager(profile, owner)

    def ensure_endpoint(method, compute):
        connections.cancel_open()
        return Endpoint("localhost", 1234, Mock())

    owner.ensure_endpoint.side_effect = ensure_endpoint
    constructor = Mock()
    monkeypatch.setattr("dbt.adapters.spark_on_k8s.connections.ConnectConnectionWrapper", constructor)
    with pytest.raises(DbtRuntimeError, match="invocation was cancelled"):
        connections.open(connection(profile))
    constructor.assert_not_called()
    with pytest.raises(DbtRuntimeError, match="invocation was cancelled"):
        connections.open(connection(profile, "later"))
    assert owner.ensure_endpoint.call_count == 1


def test_cancel_targets_open_worker_connections_only():
    owner = Mock()
    profile = credentials(method="connect")
    connections = manager(profile, owner)
    conn = connection(profile, "running-worker")
    conn.state, conn.handle = ConnectionState.OPEN, Mock()
    connections.thread_connections[(9999, 9999)] = conn
    assert connections.cancel_open() == ["running-worker"]
    conn.handle.cancel.assert_called_once_with()
    owner.close.assert_not_called()


def test_cancelled_thrift_connection_cannot_submit_another_statement():
    raw = Mock()
    wrapper = ManagedPyhiveConnectionWrapper(raw)
    wrapper.cancel()
    with pytest.raises(DbtRuntimeError, match="connection was cancelled"):
        wrapper.execute("INSERT INTO t VALUES (1)")
    raw.cursor.assert_not_called()


@pytest.mark.parametrize("materialization", ["table", "incremental"])
def test_dbt_rejects_python_before_hooks_or_materialization(tmp_path, monkeypatch, materialization):
    from dbt.cli.main import dbtRunner
    from dbt.task.run import ModelRunner
    from dbt_common.exceptions import DbtValidationError

    (tmp_path / "models").mkdir()
    (tmp_path / "dbt_project.yml").write_text(
        "name: language_guard\nversion: 1.0.0\nconfig-version: 2\nprofile: language_guard\n"
    )
    (tmp_path / "profiles.yml").write_text(
        "language_guard:\n  target: local\n  outputs:\n    local:\n"
        "      type: spark_on_k8s\n      method: connect\n      schema: example\n"
        "      compute: {image: fixture}\n      threads: 1\n"
    )
    (tmp_path / "models" / "unsupported.py").write_text(
        "def model(dbt, session):\n"
        f"    dbt.config(materialized='{materialization}')\n"
        "    return session.range(1)\n"
    )
    owner = Mock()
    with invocation_scope(owner):
        result = dbtRunner().invoke(
            [
                "parse",
                "--project-dir",
                str(tmp_path),
                "--profiles-dir",
                str(tmp_path),
                "--no-partial-parse",
                "--no-send-anonymous-usage-stats",
            ]
        )
    assert result.success, result.exception
    owner.ensure_endpoint.assert_not_called()
    manifest = result.result
    node = manifest.nodes["model.language_guard.unsupported"]
    macro = manifest.find_materialization_macro_by_name("language_guard", materialization, "spark_on_k8s")
    assert macro.package_name == "dbt_spark_on_k8s"
    assert macro.supported_languages == ["sql"]
    runner = ModelRunner.__new__(ModelRunner)
    runner.config = SimpleNamespace(project_name="language_guard")
    runner.adapter = Mock()
    runner.adapter.type.return_value = "spark_on_k8s"
    runner._execute_model = Mock()
    monkeypatch.setattr("dbt.task.run.generate_runtime_model_context", lambda *args: {"config": {}})
    with pytest.raises(DbtValidationError, match="only supports languages.*sql"):
        runner.execute(node, manifest)
    runner.adapter.pre_model_hook.assert_not_called()
    runner.adapter.drop_relation.assert_not_called()
    runner._execute_model.assert_not_called()


@pytest.mark.parametrize("method", ["connect", "thrift"])
@pytest.mark.parametrize(
    "key",
    [
        "spark.sql.warehouse.dir",
        "spark.sql.catalogImplementation",
        "spark.sql.defaultCatalog",
        "spark.sql.catalog.custom",
        "spark.sql.extensions",
        "spark.sql.hive.metastore.version",
        "spark.sql.hive.thriftServer.singleSession",
        "spark.hadoop.hive.metastore.uris",
        "hive.metastore.uris",
        "spark.remote",
        "spark.master",
        "spark.kubernetes.namespace",
        "spark.connect.grpc.binding.port",
        "spark.executor.instances",
    ],
)
def test_session_parameters_cannot_override_compute_configuration(method, key):
    with pytest.raises(DbtRuntimeError, match="compute.spark_conf"):
        credentials(method=method, server_side_parameters={key: "unrelated"})


def test_runtime_sql_parameters_remain_supported():
    creds = credentials(
        method="connect",
        server_side_parameters={
            "spark.sql.shuffle.partitions": "4",
            "spark.sql.session.timeZone": "UTC",
        },
    )
    assert creds.server_side_parameters == {
        "spark.sql.shuffle.partitions": "4",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.ansi.enabled": "false",
    }


def test_registered_adapter_run_metadata_uses_explicit_package_name():
    from dbt.adapters.factory import get_adapter_class_by_name, load_plugin
    from dbt.task.run import _get_adapter_info

    load_plugin("spark_on_k8s")
    adapter_class = get_adapter_class_by_name("spark_on_k8s")
    assert adapter_class is SparkOnK8SAdapter
    assert adapter_class.type() == "spark_on_k8s"
    adapter = adapter_class.__new__(adapter_class)
    config = SimpleNamespace(_extra={"file_format": "iceberg"})
    result = SimpleNamespace(node=SimpleNamespace(config=config))
    metadata = _get_adapter_info(adapter, result)
    assert metadata["adapter_name"] == "spark_on_k8s"
    assert metadata["adapter_version"] == "0.1.0a1"
    assert metadata["base_adapter_version"]
    assert metadata["model_adapter_details"] == {
        "adapter_type": "spark_on_k8s",
        "table_format": "iceberg",
    }
