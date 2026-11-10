from __future__ import annotations

import subprocess
import sys

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
