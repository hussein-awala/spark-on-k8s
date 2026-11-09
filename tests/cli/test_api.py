from __future__ import annotations

import importlib.util
import subprocess
import sys
from unittest import mock

import pytest
from click.testing import CliRunner

TEST_HOST = "localhost"
TEST_PORT = 8000
TEST_WORKERS = 8
TEST_LOG_LEVEL = "critical"
TEST_LIMIT_CONCURRENCY = 10000


@pytest.mark.parametrize(
    "host, port, workers, log_level, limit_concurrency",
    [
        pytest.param(None, None, None, None, None, id="default conf"),
        pytest.param(TEST_HOST, TEST_PORT, TEST_WORKERS, None, None, id="common conf"),
        pytest.param(
            TEST_HOST, TEST_PORT, TEST_WORKERS, TEST_LOG_LEVEL, TEST_LIMIT_CONCURRENCY, id="override all conf"
        ),
    ],
)
@mock.patch("uvicorn.run")
def test_list(mock_uvicorn_run, host, port, workers, log_level, limit_concurrency):
    from spark_on_k8s.api.configuration import APIConfiguration
    from spark_on_k8s.cli.api import start

    args = []
    if host is not None:
        args.extend(["--host", host])
        expected_host = host
    else:
        expected_host = APIConfiguration.SPARK_ON_K8S_API_HOST
    if port is not None:
        args.extend(["--port", port])
        expected_port = port
    else:
        expected_port = APIConfiguration.SPARK_ON_K8S_API_PORT
    if workers is not None:
        args.extend(["--workers", workers])
        expected_workers = workers
    else:
        expected_workers = APIConfiguration.SPARK_ON_K8S_API_WORKERS
    if log_level is not None:
        args.extend(["--log-level", log_level])
        expected_log_level = log_level
    else:
        expected_log_level = APIConfiguration.SPARK_ON_K8S_API_LOG_LEVEL
    if limit_concurrency is not None:
        args.extend(["--limit-concurrency", limit_concurrency])
        expected_limit_concurrency = limit_concurrency
    else:
        expected_limit_concurrency = APIConfiguration.SPARK_ON_K8S_API_LIMIT_CONCURRENCY
    result = CliRunner().invoke(start, args)
    assert result.exit_code == 0
    mock_uvicorn_run.assert_called_with(
        "spark_on_k8s.api.main:app",
        host=expected_host,
        port=expected_port,
        workers=expected_workers,
        log_level=expected_log_level,
        limit_concurrency=expected_limit_concurrency,
        root_path=APIConfiguration.SPARK_ON_K8S_API_ROOT_PATH,
    )


def start_command(monkeypatch, root_path_environment=None):
    """Load a private CLI copy so the environment is read as at process startup."""
    if root_path_environment is None:
        monkeypatch.delenv("SPARK_ON_K8S_API_ROOT_PATH", raising=False)
    else:
        monkeypatch.setenv("SPARK_ON_K8S_API_ROOT_PATH", root_path_environment)
    # Reloading the shared modules would leak this environment into later tests and leave
    # other importers holding a different APIConfiguration class.
    modules = {}
    for name in ("spark_on_k8s.api.configuration", "spark_on_k8s.cli.api"):
        spec = importlib.util.find_spec(name)
        modules[name] = importlib.util.module_from_spec(spec)
        monkeypatch.setitem(sys.modules, name, modules[name])
        spec.loader.exec_module(modules[name])
    return modules["spark_on_k8s.cli.api"].start


@pytest.mark.parametrize(
    "environment, args, expected",
    [
        pytest.param(None, [], "", id="default serves the API at the root"),
        pytest.param("/platform/spark", [], "/platform/spark", id="environment"),
        pytest.param(None, ["--root-path", "/team/spark-api"], "/team/spark-api", id="flag"),
        pytest.param("/platform/spark", ["--root-path", "/team"], "/team", id="flag overrides environment"),
        pytest.param("/platform/spark", ["--root-path", ""], "", id="flag clears environment"),
    ],
)
@mock.patch("uvicorn.run")
def test_start_root_path(mock_uvicorn_run, monkeypatch, environment, args, expected):
    result = CliRunner().invoke(start_command(monkeypatch, environment), args)

    assert result.exit_code == 0, result.output
    assert mock_uvicorn_run.call_args.kwargs["root_path"] == expected


@pytest.mark.parametrize(
    "root_path",
    [
        "platform/spark",
        "/platform/spark/",
        "/",
        "//gateway.example/spark",
        "/platform//spark",
        "/platform/spark?team=a",
        "/platform/spark#console",
        "/platform spark",
        "/platform/\tspark",
        "/plateforme/étincelle",
    ],
)
@mock.patch("uvicorn.run")
def test_start_rejects_invalid_root_path_flag(mock_uvicorn_run, monkeypatch, root_path):
    result = CliRunner().invoke(start_command(monkeypatch), ["--root-path", root_path])

    assert result.exit_code == 2
    assert "Invalid value for '--root-path'" in result.output
    assert "/platform/spark" in result.output
    assert "SPARK_ON_K8S_API_ROOT_PATH" not in result.output
    mock_uvicorn_run.assert_not_called()


@mock.patch("uvicorn.run")
def test_start_rejects_invalid_root_path_environment(mock_uvicorn_run, monkeypatch):
    result = CliRunner().invoke(start_command(monkeypatch, "platform/spark/"), [])

    assert result.exit_code == 2
    assert "Invalid value for '--root-path': 'platform/spark/' (from SPARK_ON_K8S_API_ROOT_PATH)" in (
        result.output
    )
    mock_uvicorn_run.assert_not_called()


_BLOCK_API_EXTRA = """
import importlib.abc
import sys

class BlockApiExtra(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname.split(".")[0] in {"fastapi", "starlette", "httpx", "kubernetes_asyncio", "uvicorn"}:
            raise ModuleNotFoundError(f"No module named {fullname!r}")

sys.meta_path.insert(0, BlockApiExtra())
from spark_on_k8s.cli import main
sys.argv = ["spark-on-k8s", *sys.argv[1:]]
main()
"""


@pytest.mark.parametrize(
    "arguments, exit_code, expected",
    [
        (["api", "start", "--port", "8001", "--workers", "1"], 1, "pip install 'spark-on-k8s[api]'"),
        (["api", "--help"], 0, "requires the 'api' extra"),
    ],
)
def test_base_installation_explains_the_missing_api_extra(arguments, exit_code, expected):
    result = subprocess.run(
        [sys.executable, "-B", "-c", _BLOCK_API_EXTRA, *arguments], capture_output=True, text=True
    )
    assert result.returncode == exit_code, result.stdout + result.stderr
    assert expected in result.stdout + result.stderr
    assert "No such command" not in result.stderr
    assert "Traceback" not in result.stderr


@mock.patch.dict("sys.modules", {"uvicorn": None})
def test_start_without_server_dependency_is_a_cli_error():
    from spark_on_k8s.cli.api import start

    result = CliRunner().invoke(start, [])
    assert result.exit_code == 1
    assert "pip install 'spark-on-k8s[api]'" in result.output
