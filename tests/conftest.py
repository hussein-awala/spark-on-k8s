from __future__ import annotations

import subprocess
from importlib.metadata import PackageNotFoundError, version

import pytest


def _airflow_is_installed() -> bool:
    # Namespace directories (including tests/airflow) are not an Airflow installation.
    # Inspect metadata without importing Airflow and configuring global logging.
    for distribution in ("apache-airflow", "apache-airflow-core"):
        try:
            version(distribution)
        except PackageNotFoundError:
            continue
        return True
    return False


AIRFLOW_INSTALLED = _airflow_is_installed()


@pytest.fixture(scope="session")
def root_path() -> str:
    return (
        subprocess.Popen(["git", "rev-parse", "--show-toplevel"], stdout=subprocess.PIPE)
        .communicate()[0]
        .rstrip()
        .decode("utf-8")
    )
