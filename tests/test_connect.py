from __future__ import annotations

import json
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner
from spark_on_k8s.cli.connect import connect_cli
from spark_on_k8s.connect import ConnectClientUnavailable, ConnectError, ConnectProfile


@pytest.mark.parametrize(
    "settings",
    [
        {"endpoint": "https://spark.example"},
        {"endpoint": "sc://user:secret@spark.example"},
        {"endpoint": "sc://spark.example/;token=private"},
        {"endpoint": "sc://spark.example?token=x"},
        {"endpoint": "sc://spark.example/path"},
        {"endpoint": "sc://spark.example#fragment"},
        {"endpoint": "sc://spark.example:99999"},
        {"endpoint": "sc://spark.example:0"},
        {"endpoint": "sc://"},
        {"endpoint": "sc://spa rk"},
        {"endpoint": None},
        {"endpoint": "sc://spark.example", "tls": False},
        {"endpoint": "sc://spark.example", "tls": "true"},
        {"endpoint": "sc://spark.example", "token_env": "bad-name"},
    ],
)
def test_bad_profiles_are_rejected(settings):
    with pytest.raises(ConnectError):
        ConnectProfile(**settings)


def test_profile_resolves_credentials_without_storing_them(monkeypatch, tmp_path):
    file = tmp_path / "connect.toml"
    file.write_text('[profiles.production]\nendpoint = "sc://spark.example:443"\ntoken_env = "TEST_TOKEN"\n')
    profile = ConnectProfile.from_file(file, profile="production")
    monkeypatch.setenv("TEST_TOKEN", "private;token=with/&encoding")
    assert (
        profile.connection_string()
        == "sc://spark.example:443/;use_ssl=true;token=private%3Btoken%3Dwith%2F%26encoding"
    )
    assert "private" not in repr(profile)
    monkeypatch.delenv("TEST_TOKEN")
    with pytest.raises(ConnectError, match="empty or missing"):
        profile.connection_string()


@pytest.mark.parametrize(
    "content",
    [
        '[profiles.default]\nendpoint="sc://localhost"\ntoken="literal-secret"',
        '[profiles.default]\nendpoint="sc://localhost"\nendpoint="sc://other"',
        '[profiles.other]\nendpoint="sc://localhost"',
        "profiles = []",
        "[profiles.default",
        "x" * (1024 * 1024 + 1),
    ],
)
def test_invalid_toml_is_rejected_without_echoing_content(tmp_path, content):
    file = tmp_path / "connect.toml"
    file.write_text(content)
    with pytest.raises(ConnectError) as error:
        ConnectProfile.from_file(file)
    assert "literal-secret" not in str(error.value)


def test_loopback_and_explicit_insecure_network():
    for endpoint in ["sc://localhost", "sc://127.0.0.1:15002", "sc://[::1]:15002"]:
        assert "use_ssl=false" in ConnectProfile(endpoint=endpoint, tls=False).connection_string()
    assert ConnectProfile(endpoint="sc://spark.internal", tls=False, allow_insecure=True)


@pytest.mark.parametrize("fail", [False, True])
def test_session_is_independent_and_always_closed(monkeypatch, fail):
    session = MagicMock()
    builder = MagicMock()
    builder.remote.return_value.create.return_value = session
    module = SimpleNamespace(SparkSession=SimpleNamespace(builder=builder))
    monkeypatch.setitem(sys.modules, "pyspark.sql.connect.session", module)
    profile = ConnectProfile(endpoint="sc://localhost", tls=False)
    if fail:
        with pytest.raises(RuntimeError, match="user code"):
            with profile.session() as spark:
                assert spark is session
                raise RuntimeError("user code")
    else:
        with profile.session() as spark:
            assert spark is session
    builder.remote.assert_called_once_with("sc://localhost/;use_ssl=false")
    builder.remote.return_value.getOrCreate.assert_not_called()
    session.stop.assert_called_once()


def test_probe_bounds_process_and_does_not_put_credentials_in_arguments(monkeypatch):
    monkeypatch.setenv("TEST_TOKEN", "never-in-argv")
    profile = ConnectProfile(endpoint="sc://spark.example", token_env="TEST_TOKEN")
    result = {"schema_version": 1, "status": "ok", "spark_version": "4.3.0"}
    with patch(
        "spark_on_k8s.connect.subprocess.run",
        return_value=SimpleNamespace(returncode=0, stdout=json.dumps(result)),
    ) as run:
        assert profile.probe(timeout=2) == result
    assert run.call_args.kwargs["timeout"] == 2
    assert "never-in-argv" not in repr(run.call_args)
    assert json.loads(run.call_args.kwargs["input"])["token_env"] == "TEST_TOKEN"
    with patch("spark_on_k8s.connect.subprocess.run", side_effect=subprocess.TimeoutExpired("probe", 2)):
        with pytest.raises(TimeoutError):
            profile.probe(timeout=2)


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan"), True])
def test_invalid_probe_timeout(timeout):
    with pytest.raises(ConnectError):
        ConnectProfile(endpoint="sc://localhost", tls=False).probe(timeout=timeout)


def test_failed_probe_never_prints_worker_stderr():
    with patch(
        "spark_on_k8s.connect.subprocess.run",
        return_value=SimpleNamespace(returncode=1, stdout="", stderr="private-token"),
    ):
        with pytest.raises(ConnectError) as error:
            ConnectProfile(endpoint="sc://localhost", tls=False).probe()
    assert "private-token" not in str(error.value)


def test_cli_reports_timeout_as_json(tmp_path):
    file = tmp_path / "connect.toml"
    file.write_text('[profiles.default]\nendpoint="sc://localhost"\ntls=false\n')
    with patch.object(ConnectProfile, "probe", side_effect=TimeoutError("Check timed out")):
        result = CliRunner().invoke(connect_cli, ["check", str(file), "--output", "json"])
    assert result.exit_code == 5
    assert json.loads(result.stdout)["status"] == "error"


@pytest.mark.parametrize("value", ["inf", "nan"])
def test_cli_nonfinite_timeout_is_a_usage_error_before_any_check(tmp_path, value):
    file = tmp_path / "connect.toml"
    file.write_text('[profiles.default]\nendpoint="sc://localhost"\ntls=false\n')
    with patch.object(ConnectProfile, "probe") as probe:
        result = CliRunner().invoke(connect_cli, ["check", str(file), "--timeout", value, "--output", "json"])
    assert result.exit_code == 2, result.output
    assert result.stdout == ""
    probe.assert_not_called()


def test_session_creation_errors_do_not_echo_connection_credentials(monkeypatch):
    builder = MagicMock()
    builder.remote.return_value.create.side_effect = RuntimeError("PRIVATE_CONNECTION")
    module = SimpleNamespace(SparkSession=SimpleNamespace(builder=builder))
    monkeypatch.setitem(sys.modules, "pyspark.sql.connect.session", module)
    with pytest.raises(ConnectError) as error, ConnectProfile(endpoint="sc://localhost", tls=False).session():
        pass
    assert "PRIVATE_CONNECTION" not in str(error.value)


def test_missing_client_has_actionable_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "pyspark.sql.connect.session", None)
    with (
        pytest.raises(ConnectError, match="Install spark-on-k8s"),
        ConnectProfile(endpoint="sc://localhost").session(),
    ):
        pass


@pytest.mark.parametrize("stdout", ["not json", "[]", '{"schema_version":99,"status":"ok"}'])
def test_probe_rejects_invalid_worker_results(stdout):
    with patch(
        "spark_on_k8s.connect.subprocess.run", return_value=SimpleNamespace(returncode=0, stdout=stdout)
    ):
        with pytest.raises(ConnectError, match="invalid response"):
            ConnectProfile(endpoint="sc://localhost", tls=False).probe()


def test_probe_missing_credentials_fails_without_opening_session(monkeypatch):
    monkeypatch.delenv("CONNECT_TEST_MISSING_TOKEN", raising=False)
    with pytest.raises(ConnectError, match="token environment variable is empty or missing") as error:
        ConnectProfile(endpoint="sc://localhost", token_env="CONNECT_TEST_MISSING_TOKEN").probe(timeout=10)
    assert not isinstance(error.value, ConnectClientUnavailable)


def test_probe_worker_reports_a_missing_client_actionably(monkeypatch, capsys):
    import io

    from spark_on_k8s.connect import _probe_worker

    monkeypatch.setitem(sys.modules, "pyspark.sql.connect.session", None)
    monkeypatch.setattr(sys, "stdin", io.StringIO('{"endpoint":"sc://localhost","tls":false}'))
    assert _probe_worker() == 1
    result = json.loads(capsys.readouterr().out)
    assert result["code"] == "client_unavailable"
    assert "Install spark-on-k8s[connect]" in result["error"]


def test_probe_raises_client_unavailable_from_the_worker_result(tmp_path):
    worker = {
        "schema_version": 1,
        "status": "error",
        "error": "Install the client",
        "code": "client_unavailable",
    }
    with patch(
        "spark_on_k8s.connect.subprocess.run",
        return_value=SimpleNamespace(returncode=1, stdout=json.dumps(worker)),
    ):
        with pytest.raises(ConnectClientUnavailable, match="Install the client"):
            ConnectProfile(endpoint="sc://localhost", tls=False).probe()
    file = tmp_path / "connect.toml"
    file.write_text('[profiles.default]\nendpoint="sc://localhost"\ntls=false\n')
    with patch.object(ConnectProfile, "probe", side_effect=ConnectClientUnavailable("Install the client")):
        result = CliRunner().invoke(connect_cli, ["check", str(file), "--output", "json"])
    assert result.exit_code == 1
    assert json.loads(result.stdout)["error"] == {"code": "operation_error", "message": "Install the client"}


@pytest.mark.parametrize("failure", [False, True])
def test_probe_worker_result_and_errors_are_stable(monkeypatch, capsys, failure):
    import io
    from contextlib import contextmanager

    from spark_on_k8s.connect import _probe_worker

    session = MagicMock()
    session.range.return_value.count.return_value = 1
    session.version = "4.3.0"

    @contextmanager
    def fake_session(self):
        if failure:
            raise RuntimeError("PRIVATE_ERROR")
        yield session

    monkeypatch.setattr(ConnectProfile, "session", fake_session)
    monkeypatch.setattr(sys, "stdin", io.StringIO('{"endpoint":"sc://localhost"}'))
    assert _probe_worker() == (1 if failure else 0)
    output = capsys.readouterr().out
    result = json.loads(output)
    assert result["status"] == ("error" if failure else "ok")
    assert "PRIVATE_ERROR" not in output


@pytest.mark.parametrize("output", ["text", "json"])
def test_cli_success_and_invalid_profile(tmp_path, output):
    file = tmp_path / "connect.toml"
    file.write_text('[profiles.default]\nendpoint="sc://localhost"\ntls=false\n')
    with patch.object(
        ConnectProfile, "probe", return_value={"schema_version": 1, "status": "ok", "spark_version": "4.3.0"}
    ):
        result = CliRunner().invoke(connect_cli, ["check", str(file), "--output", output])
    assert result.exit_code == 0
    assert "4.3.0" in result.stdout
    file.write_text("not TOML")
    invalid = CliRunner().invoke(connect_cli, ["check", str(file), "--output", output])
    assert invalid.exit_code == 1
    assert "Invalid Connect profile" in invalid.output


def test_user_id_is_encoded_and_invalid_text_is_rejected(tmp_path):
    assert (
        "user_id=alice%3Brole%3Dadmin"
        in ConnectProfile(endpoint="sc://spark", user_id="alice;role=admin").connection_string()
    )
    with pytest.raises(ConnectError, match="user_id"):
        ConnectProfile(endpoint="sc://spark", user_id="")
    file = tmp_path / "bad.toml"
    file.write_bytes(b"\xff")
    with pytest.raises(ConnectError, match="Invalid Connect profile TOML"):
        ConnectProfile.from_file(file)


@pytest.mark.parametrize("body_error", [False, True])
def test_session_release_error_preserves_application_failure_and_hides_credentials(monkeypatch, body_error):
    session = MagicMock()
    session.stop.side_effect = RuntimeError("PRIVATE_RELEASE_TOKEN")
    builder = MagicMock()
    builder.remote.return_value.create.return_value = session
    monkeypatch.setitem(
        sys.modules,
        "pyspark.sql.connect.session",
        SimpleNamespace(SparkSession=SimpleNamespace(builder=builder)),
    )
    original = RuntimeError("application failed")
    with pytest.raises(RuntimeError if body_error else ConnectError) as raised:
        with ConnectProfile(endpoint="sc://localhost", tls=False).session():
            if body_error:
                raise original
    if body_error:
        assert raised.value is original
        assert "release also failed" in raised.value.__notes__[0]
    assert "PRIVATE_RELEASE_TOKEN" not in str(raised.value)
    session.stop.assert_called_once()


def test_probe_expected_app_id_is_sent_and_validated_without_changing_default_result():
    profile = ConnectProfile(endpoint="sc://localhost", tls=False)
    payload = {"schema_version": 1, "status": "ok", "spark_version": "4.0.5", "app_id": "owned-server"}
    with patch(
        "spark_on_k8s.connect.subprocess.run",
        return_value=SimpleNamespace(returncode=0, stdout=json.dumps(payload)),
    ) as run:
        assert profile.probe(expected_app_id="owned-server")["app_id"] == "owned-server"
        assert json.loads(run.call_args.kwargs["input"])["expected_app_id"] == "owned-server"
        with pytest.raises(ConnectError, match="invalid response"):
            profile.probe(expected_app_id="different-server")
    with pytest.raises(ConnectError, match="expected_app_id"):
        profile.probe(expected_app_id="invalid;token=x")


@pytest.mark.parametrize("actual", ["owned-server", "wrong-server"])
def test_probe_worker_checks_application_identity_before_query(monkeypatch, capsys, actual):
    import io
    from contextlib import contextmanager

    from spark_on_k8s.connect import _probe_worker

    session = MagicMock()
    session.conf.get.return_value = actual
    session.range.return_value.count.return_value = 1
    session.version = "4.0.5"

    @contextmanager
    def fake_session(self):
        yield session

    monkeypatch.setattr(ConnectProfile, "session", fake_session)
    monkeypatch.setattr(
        sys, "stdin", io.StringIO('{"endpoint":"sc://localhost","expected_app_id":"owned-server"}')
    )
    result = _probe_worker()
    output = json.loads(capsys.readouterr().out)
    assert result == (0 if actual == "owned-server" else 1)
    if actual == "owned-server":
        assert output["app_id"] == "owned-server"
        session.range.assert_called_once_with(1)
    else:
        session.range.assert_not_called()
        assert "wrong-server" not in json.dumps(output)
