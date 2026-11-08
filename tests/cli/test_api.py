from __future__ import annotations

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
    )


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
