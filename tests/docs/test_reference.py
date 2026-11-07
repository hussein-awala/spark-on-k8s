"""Check reference entry points and retained links without optional docs imports."""

from __future__ import annotations

import runpy
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def generator():
    return runpy.run_path(str(ROOT / "docs/gen_ref_pages.py"), run_name="reference_generator")


@pytest.fixture(scope="module")
def pages(generator):
    return generator["reference_pages"](ROOT / "spark_on_k8s")


def test_documented_entry_points_lead_navigation_and_keep_existing_urls(generator, pages):
    entries = generator["navigation_entries"](pages)
    assert entries[0] == (("Overview",), "index.md")
    destinations = dict(entries)
    assert destinations["Python client", "Client and resource options"] == "spark_on_k8s/client.md"
    assert destinations["Python client", "Run handles"] == "spark_on_k8s/utils/run.md"
    assert destinations["Job definitions", "JobSpec"] == "spark_on_k8s/jobs.md"
    assert destinations["Integrations", "Airflow operators"] == "spark_on_k8s/airflow/operators.md"
    implementation_index = next(
        i for i, (titles, _) in enumerate(entries) if titles[0] == "Implementation details"
    )
    assert all(titles[0] != "Implementation details" for titles, _ in entries[:implementation_index])
    assert all(titles[0] == "Implementation details" for titles, _ in entries[implementation_index:])
    assert destinations["Implementation details", "api", "apps"] == "spark_on_k8s/api/apps.md"
    assert destinations["Implementation details", "airflow", "_compat"] == "spark_on_k8s/airflow/_compat.md"
    assert (
        destinations["Implementation details", "utils", "configuration"]
        == "spark_on_k8s/utils/configuration.md"
    )
    assert len(set(destinations.values())) == len(entries)
    generated = {page.destination.as_posix() for page in pages}
    assert all(
        path in generated or (ROOT / "docs/reference" / path).is_file() for path in destinations.values()
    )


def test_package_definitions_get_correct_identifiers_and_internal_boundary(generator, pages):
    by_identifier = {page.identifier: page for page in pages}
    for identifier in (
        "spark_on_k8s.sql",
        "spark_on_k8s.api",
        "spark_on_k8s.api.webserver",
        "spark_on_k8s.cli",
    ):
        page = by_identifier[identifier]
        assert page.source.name == "__init__.py"
        assert page.destination == Path(*identifier.split("."), "index.md")
        content = generator["page_content"](page)
        assert f"::: {identifier}\n" in content
        assert ".__init__" not in content
        assert "**Implementation reference.**" in content
        assert "show_submodules: false" in content
    assert "spark_on_k8s" not in by_identifier
    assert "spark_on_k8s.airflow" not in by_identifier


def test_empty_and_version_only_packages_do_not_add_dead_end_pages(generator, tmp_path):
    package = tmp_path / "example"
    package.mkdir()
    (package / "__init__.py").write_text('__version__ = "1.0"\n')
    (package / "empty").mkdir()
    (package / "empty/__init__.py").write_text('"""Namespace marker."""\n')
    (package / "useful").mkdir()
    (package / "useful/__init__.py").write_text("async def observe():\n    pass\n")
    pages = generator["reference_pages"](package)
    assert [(page.identifier, page.destination.as_posix()) for page in pages] == [
        ("example.useful", "example/useful/index.md")
    ]


def test_published_sql_runner_url_and_anchors_lead_to_current_reference(generator, pages):
    path = generator["LEGACY_SQL_PATH"]
    assert any(destination == path for _, destination in generator["navigation_entries"](pages))
    content = (ROOT / "docs/reference" / path).read_text()
    for suffix in ("", ".execute_sql_commands", ".read_sql_file"):
        assert f'id="spark_on_k8s.airflow.scripts.spark_sql{suffix}"' in content
    assert "../../sql/runner.md#spark_on_k8s.sql.runner.execute_sql_commands" in content
    assert "../../../../user-guide/sql.md" in content
    assert "does not provide an import alias" in " ".join(content.split())
