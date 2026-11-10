from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from urllib.parse import urlsplit

import pytest

ROOT = Path(__file__).resolve().parents[2]
DOCS_SITE = "https://hussein.awala.fr/spark-on-k8s/latest/"
REPOSITORY = "https://github.com/hussein-awala/spark-on-k8s/"

_FENCED_BLOCK = re.compile(r"^[ \t]*(`{3,}|~{3,}).*?^[ \t]*\1[ \t]*$", re.MULTILINE | re.DOTALL)
_CODE_SPAN = re.compile(r"(`+).+?\1")
_TARGETS = (
    re.compile(r"\]\(\s*<?([^)\s>]*)"),  # inline links and images
    re.compile(r"^ {0,3}\[[^\]]+\]:\s*<?([^\s>]+)", re.MULTILINE),  # reference definitions
    re.compile(r"\b(?:href|src)\s*=\s*[\"']([^\"']*)", re.IGNORECASE),  # embedded HTML
)
_ABSOLUTE = re.compile(r"(?:https?://|mailto:)", re.IGNORECASE)


def link_targets(markdown: str) -> list[str]:
    text = _CODE_SPAN.sub("", _FENCED_BLOCK.sub("", markdown))
    return [target for pattern in _TARGETS for target in pattern.findall(text)]


def heading_anchors(markdown: str) -> set[str]:
    # Python-Markdown's default toc slug, which MkDocs uses for heading ids.
    anchors = set()
    for heading in re.findall(r"^#{1,6}\s+(.+?)\s*#*\s*$", _FENCED_BLOCK.sub("", markdown), re.MULTILINE):
        text = unicodedata.normalize("NFKD", heading).encode("ascii", "ignore").decode("ascii")
        anchors.add(re.sub(r"[-\s]+", "-", re.sub(r"[^\w\s-]", "", text).strip().lower()))
    return anchors


def readme_targets() -> list[str]:
    targets = link_targets((ROOT / "README.md").read_text(encoding="utf-8"))
    assert targets, "no links were extracted from README.md"
    return targets


def test_readme_links_are_absolute_for_the_pypi_description():
    # README.md is the PyPI long description, where repository-relative links return 404.
    relative = [target for target in readme_targets() if not _ABSOLUTE.match(target)]
    assert not relative, f"Use absolute docs-site or GitHub URLs in README.md: {relative}"


def test_readme_documentation_and_repository_links_resolve_in_this_checkout():
    missing = []
    for target in readme_targets():
        url = urlsplit(target)
        if target.startswith(DOCS_SITE):
            page = url.path.removeprefix(urlsplit(DOCS_SITE).path).strip("/")
            candidates = [ROOT / "docs" / f"{page}.md", ROOT / "docs" / page / "index.md"]
            source = next((path for path in candidates if path.is_file()), None)
            if source is None or (
                url.fragment and url.fragment not in heading_anchors(source.read_text(encoding="utf-8"))
            ):
                missing.append(target)
        elif target.startswith((REPOSITORY + "blob/main/", REPOSITORY + "tree/main/")):
            kind, path = url.path.removeprefix(urlsplit(REPOSITORY).path).split("/main/", 1)
            resolved = ROOT / path
            if not path or not (resolved.is_file() if kind == "blob" else resolved.is_dir()):
                missing.append(target)
    assert not missing, f"README.md links to pages or files that do not exist: {missing}"


@pytest.mark.parametrize(
    "markdown",
    [
        "[guide](docs/user-guide/sql.md)",
        "![diagram](docs/images/flow.png)",
        "[![badge](https://img.shields.io/pypi/v/spark-on-k8s)](LICENSE)",
        "[top](#installation)",
        "[guide][sql]\n\n[sql]: docs/user-guide/sql.md",
        "[guide](<docs/user guide.md>)",
        '<img src="docs/logo.png">',
        "[site](//hussein.awala.fr/spark-on-k8s/)",
    ],
)
def test_relative_targets_are_detected(markdown):
    assert any(not _ABSOLUTE.match(target) for target in link_targets(markdown))


def test_code_examples_are_not_links():
    markdown = "```bash\necho '[x](local.md)'\n```\n\nUse `[x](y)` here.\n"
    assert link_targets(markdown) == []
