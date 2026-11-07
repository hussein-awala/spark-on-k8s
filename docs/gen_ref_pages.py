"""Generate reference navigation without changing existing module URLs."""

from __future__ import annotations

import ast
import posixpath
from dataclasses import dataclass
from pathlib import Path

# These modules contain documented entry points or returned value types. Listing
# a module here does not make every implementation helper in it a public API.
ENTRY_POINTS = {
    "spark_on_k8s.client": ("Python client", "Client and resource options"),
    "spark_on_k8s.utils.run": ("Python client", "Run handles"),
    "spark_on_k8s.diagnostics": ("Python client", "Diagnostic results"),
    "spark_on_k8s.utils.setup_namespace": ("Python client", "Namespace setup helper"),
    "spark_on_k8s.jobs": ("Job definitions", "JobSpec"),
    "spark_on_k8s.planning": ("Job definitions", "Application plans"),
    "spark_on_k8s.airflow.operators": ("Integrations", "Airflow operators"),
    "spark_on_k8s.connect": ("Integrations", "Spark Connect profiles"),
}
LEGACY_SQL_PATH = "spark_on_k8s/airflow/scripts/spark_sql.md"


@dataclass(frozen=True)
class ReferencePage:
    identifier: str
    source: Path
    destination: Path


def reference_pages(source_root: Path) -> list[ReferencePage]:
    pages = []
    for source in sorted(source_root.rglob("*.py")):
        relative = source.relative_to(source_root)
        parts = relative.with_suffix("").parts
        if source.name == "__init__.py":
            # Empty namespace markers and version-only boilerplate do not need a
            # reference page. Package-defined classes/functions do need one.
            tree = ast.parse(source.read_text(encoding="utf-8"))
            if not any(
                isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) for node in tree.body
            ):
                continue
            parts = parts[:-1]
            destination = Path(source_root.name, *parts, "index.md")
        else:
            # Preserve the URLs produced by the original generator.
            destination = Path(source_root.name, relative.with_suffix(".md"))
        identifier = ".".join((source_root.name, *parts))
        pages.append(ReferencePage(identifier, source, destination))
    return pages


def navigation_entries(pages: list[ReferencePage]) -> list[tuple[tuple[str, ...], str]]:
    entries = [(("Overview",), "index.md")]
    by_identifier = {page.identifier: page for page in pages}
    for identifier, titles in ENTRY_POINTS.items():
        entries.append((titles, by_identifier[identifier].destination.as_posix()))
    for page in pages:
        if page.identifier not in ENTRY_POINTS:
            titles = ("Implementation details", *page.identifier.split(".")[1:])
            entries.append((titles, page.destination.as_posix()))
    entries.append((("Implementation details", "SQL runner: older reference URL"), LEGACY_SQL_PATH))
    return entries


def page_content(page: ReferencePage) -> str:
    parent = (Path("reference") / page.destination).parent.as_posix()
    overview = posixpath.relpath("reference/index.md", parent)
    support = posixpath.relpath("user-guide/support.md", parent)
    if page.identifier in ENTRY_POINTS:
        boundary = (
            f"See the [reference overview]({overview}) for supported entry points and task guides. "
            "A module may also contain helpers outside the documented public API."
        )
    else:
        boundary = (
            "**Implementation reference.** These symbols describe how the project works; "
            "their presence here does not make them supported application imports. "
            f"Use the [documented entry points]({overview}) and [support policy]({support})."
        )
    options = "      show_submodules: false\n"
    if page.source.name == "__init__.py" or page.identifier == "spark_on_k8s.diagnostics":
        # Package entry points and the diagnostic result dataclasses may have no
        # docstrings; rendering only documented members would hide their structure.
        options += "      show_if_no_docstring: true\n"
    return f"# `{page.identifier}`\n\n{boundary}\n\n::: {page.identifier}\n    options:\n{options}"


def generate_reference() -> None:
    import mkdocs_gen_files

    pages = reference_pages(Path("spark_on_k8s"))
    nav = mkdocs_gen_files.Nav()
    for titles, destination in navigation_entries(pages):
        nav[titles] = destination
    for page in pages:
        full_path = Path("reference") / page.destination
        with mkdocs_gen_files.open(full_path, "w") as stream:
            stream.write(page_content(page))
        mkdocs_gen_files.set_edit_path(full_path, page.source)
    with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as stream:
        stream.writelines(nav.build_literate_nav())


# mkdocs-gen-files executes scripts with runpy.run_path's default module name.
if __name__ in {"__main__", "<run_path>"}:
    generate_reference()
