"""The bounded Connect check must import the installation and client the caller imports.

The check runs in a child interpreter. Each test lays out spark-on-k8s and a stand-in Connect client
the way an installer would, and calls `ConnectProfile.probe` from a fresh interpreter.
"""

from __future__ import annotations

import json
import os
import subprocess
import venv
from pathlib import Path

import spark_on_k8s

PACKAGE = Path(spark_on_k8s.__file__).resolve().parent

FAKE_SESSION = """
class _Frame:
    def count(self):
        return 1


class _Conf:
    def get(self, key):
        return "spark-app"


class SparkSession:
    version = "9.9.0"
    conf = _Conf()

    class builder:
        @staticmethod
        def remote(url):
            return SparkSession.builder

        @staticmethod
        def create():
            return SparkSession()

    def range(self, count):
        return _Frame()

    def stop(self):
        pass
"""

# The caller first proves that it can open a session itself, then runs the bounded check.
CALLER = """
import json
from pyspark.version import __version__ as own
from spark_on_k8s.connect import ConnectProfile

profile = ConnectProfile(endpoint="sc://localhost:1", tls=False)
with profile.session() as session:
    assert session.version == "9.9.0"
try:
    outcome = {"probe": profile.probe(timeout=60)}
except Exception as error:
    outcome = {"error": type(error).__name__, "message": str(error)}
print(json.dumps({"caller_client": own, **outcome}))
"""


def passed(client: str) -> dict:
    return {
        "caller_client": client,
        "probe": {
            "schema_version": 1,
            "status": "ok",
            "spark_version": "9.9.0",
            "client_version": client,
            "warnings": [],
        },
    }


def client(root: Path, version: str) -> Path:
    connect = root / "pyspark" / "sql" / "connect"
    connect.mkdir(parents=True)
    for directory in (root / "pyspark", root / "pyspark" / "sql", connect):
        (directory / "__init__.py").write_text("")
    (root / "pyspark" / "version.py").write_text(f'__version__ = "{version}"\n')
    (connect / "session.py").write_text(FAKE_SESSION)
    return root


def install(root: Path, package: Path) -> Path:
    """Lay out spark-on-k8s and a stand-in Connect client the way an installer would."""
    root.mkdir(parents=True)
    (root / "spark_on_k8s").symlink_to(package, target_is_directory=True)
    metadata = root / "spark_on_k8s-0.0.0.dist-info"
    metadata.mkdir()
    (metadata / "METADATA").write_text("Metadata-Version: 2.1\nName: spark-on-k8s\nVersion: 0.0.0\n")
    return client(root, "9.9.9")


def environment_with_site_packages(tmp_path: Path) -> Path:
    venv.EnvBuilder(with_pip=False, symlinks=True).create(tmp_path / "venv")
    python = tmp_path / "venv" / "bin" / "python"
    purelib = subprocess.run(
        [str(python), "-c", "import sysconfig; print(sysconfig.get_paths()['purelib'])"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    for entry in install(tmp_path / "target", PACKAGE).iterdir():
        (Path(purelib) / entry.name).symlink_to(entry, target_is_directory=True)
    return python


def run_caller(python: str | Path, cwd: Path, *flags: str, **environment: str) -> dict:
    env = {key: value for key, value in os.environ.items() if not key.startswith("PYTHON")}
    result = subprocess.run(
        [str(python), *flags, "-c", CALLER],
        capture_output=True,
        text=True,
        cwd=cwd,
        env=env | environment,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_probe_works_when_the_caller_imports_through_pythonpath(tmp_path):
    # A clean interpreter: nothing is importable except through PYTHONPATH (`pip install --target`).
    venv.EnvBuilder(with_pip=False, symlinks=True).create(tmp_path / "venv")
    root = install(tmp_path / "target", PACKAGE)

    result = run_caller(tmp_path / "venv" / "bin" / "python", tmp_path, PYTHONPATH=str(root))

    assert result == passed("9.9.9")


def test_probe_reports_the_client_the_caller_imports(tmp_path):
    # Another PySpark on PYTHONPATH shadows site-packages for the caller; the check must use and
    # report that client, so the version-skew warning (1.2 against a 9.9 server) is raised.
    python = environment_with_site_packages(tmp_path)
    other = client(tmp_path / "other", "1.2.3")

    result = run_caller(python, tmp_path, PYTHONPATH=str(other))

    assert result["caller_client"] == "1.2.3"
    assert result["probe"]["client_version"] == "1.2.3"
    assert [warning["code"] for warning in result["probe"]["warnings"]] == ["version_skew"]


def test_probe_works_from_site_packages(tmp_path):
    python = environment_with_site_packages(tmp_path)

    assert run_caller(python, tmp_path) == passed("9.9.9")


def test_an_isolated_caller_keeps_an_isolated_child(tmp_path):
    python = environment_with_site_packages(tmp_path)
    other = client(tmp_path / "other", "1.2.3")

    assert run_caller(python, tmp_path, "-I", PYTHONPATH=str(other)) == passed("9.9.9")


def test_the_child_never_imports_from_the_working_directory(tmp_path):
    python = environment_with_site_packages(tmp_path)
    planted = tmp_path / "planted"
    planted.mkdir()
    (planted / "tomllib.py").write_text("raise SystemExit('planted module executed')\n")
    (planted / "pyspark").mkdir()
    (planted / "pyspark" / "__init__.py").write_text("raise SystemExit('planted module executed')\n")

    # The caller is isolated so that only the child could pick the planted modules up.
    assert run_caller(python, planted, "-P") == passed("9.9.9")
