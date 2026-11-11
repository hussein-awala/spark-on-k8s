from __future__ import annotations

import os
import subprocess
import sys

import pytest

# A fresh interpreter: once any test imports spark_on_k8s.cli, the package exposes it as an attribute.
_LOAD = """
from importlib.metadata import distribution
scripts = [entry for entry in distribution("spark-on-k8s").entry_points if entry.group == "console_scripts"]
assert [entry.name for entry in scripts] == ["spark-on-k8s"], scripts
main = scripts[0].load()
from spark_on_k8s.cli import main as expected
assert main is expected, main
"""


def test_console_script_entry_point_loads_by_its_specification():
    # Launchers that resolve entry points with importlib (rather than pip's generated wrapper)
    # import the module part and then read attributes; the package does not import its CLI.
    result = subprocess.run([sys.executable, "-I", "-c", _LOAD], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_cli_runs_as_a_module():
    result = subprocess.run(
        [sys.executable, "-m", "spark_on_k8s.cli", "--help"], capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr
    assert "app" in result.stdout


def test_cli_reports_its_version_and_accepts_short_help():
    from importlib.metadata import version

    from click.testing import CliRunner
    from spark_on_k8s.cli import cli

    assert CliRunner().invoke(cli, ["--version"]).output.strip().endswith(version("spark-on-k8s"))
    short = CliRunner().invoke(cli, ["-h"])
    assert short.exit_code == 0 and "without an operator" in short.output


@pytest.mark.parametrize(
    "variable,value,message",
    [
        ("SPARK_ON_K8S_DRIVER_CPU", "two", "SPARK_ON_K8S_DRIVER_CPU must be an integer; got 'two'"),
        # A CI variable that expands to an empty string.
        ("SPARK_ON_K8S_STARTUP_TIMEOUT", "", "SPARK_ON_K8S_STARTUP_TIMEOUT must be an integer; got ''"),
        # JSON variables can hold secrets, so their value is never echoed.
        ("SPARK_ON_K8S_SECRET_ENV_VAR", "token=abc", "SPARK_ON_K8S_SECRET_ENV_VAR must be a JSON object"),
        ("SPARK_ON_K8S_API_PORT", "", "SPARK_ON_K8S_API_PORT must be an integer; got ''"),
    ],
)
def test_a_malformed_variable_is_one_error_line_even_for_help(variable, value, message):
    env = {**os.environ, variable: value}
    result = subprocess.run(
        [sys.executable, "-m", "spark_on_k8s.cli", "--help"], capture_output=True, text=True, env=env
    )
    assert result.returncode == 1
    assert result.stderr == f"Error: {message}\n"
    assert "abc" not in result.stderr
