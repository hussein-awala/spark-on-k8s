from __future__ import annotations

import runpy
import shutil
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
check_project = runpy.run_path(str(ROOT / "scripts" / "dbt_release.py"))["check_project"]


@pytest.fixture
def project(tmp_path):
    """A copy of the adapter's version-bearing files that tests can edit."""
    source = ROOT / "integrations" / "dbt"
    for path in (
        "pyproject.toml",
        "src/dbt/adapters/spark_on_k8s/__version__.py",
        "src/spark_on_k8s_dbt/__init__.py",
    ):
        (tmp_path / path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(source / path, tmp_path / path)
    return tmp_path


def set_sdk_requirement(project, requirement):
    pyproject = project / "pyproject.toml"
    text = pyproject.read_text()
    current = next(line for line in text.splitlines() if line.strip().startswith('"spark-on-k8s'))
    pyproject.write_text(text.replace(current, f'    "{requirement}",'))


def version_of(project):
    return next(
        line.split('"')[1]
        for line in (project / "pyproject.toml").read_text().splitlines()
        if line.startswith("version")
    )


def test_released_sdk_requirement_with_consistent_versions_is_publishable(project):
    set_sdk_requirement(project, "spark-on-k8s[connect]>=0.13.0,<0.14")
    assert check_project(version_of(project), project) == []


def test_repository_adapter_versions_agree(project):
    errors = check_project(version_of(project), project)
    assert not [error for error in errors if "declares" in error]


@pytest.mark.parametrize(
    "requirement",
    ["spark-on-k8s[connect]==0.11.0.dev0", "spark-on-k8s[connect]==0.13.0+local", "spark-on-k8s[connect]"],
)
def test_unreleased_or_unpinned_sdk_requirement_is_rejected(project, requirement):
    set_sdk_requirement(project, requirement)
    errors = check_project(version_of(project), project)
    assert any("must name a released SDK version" in error for error in errors), errors


def test_mismatched_or_invalid_version_is_rejected(project):
    set_sdk_requirement(project, "spark-on-k8s[connect]==0.13.0")
    errors = check_project("0.2.0", project)
    assert len([error for error in errors if "not 0.2.0" in error]) == 3
    assert any(error.startswith("version:") for error in check_project("v0.1.0", project))
