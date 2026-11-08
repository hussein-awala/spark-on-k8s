from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = "https://github.com/hussein-awala/spark-on-k8s"


@pytest.fixture
def notes(release_modules):
    return release_modules["notes"]


def write_fixture(directory, version="0.13.0", *, body="## Features\n\nA change.\n", **metadata):
    fields = {
        "version": f'"{version}"',
        "title": f'"Spark On K8S {version}"',
        "date": "null",
        "status": '"planned"',
        "github_url": f'"{REPOSITORY}/releases/tag/{version}"',
    }
    fields.update(metadata)
    path = directory / f"{version}.md"
    path.write_text(
        "---\n" + "\n".join(f"{key}: {value}" for key, value in fields.items()) + "\n---\n\n" + body,
        encoding="utf-8",
    )
    return path


def test_metadata_and_body_are_loaded_without_modification(notes, tmp_path):
    body = "\n## Features\n\n```python\n# Code heading\n```\n\nTrailing spaces  "
    release = notes.load_release(write_fixture(tmp_path, body=body))
    assert release.body == body
    assert release.version == "0.13.0"
    assert release.date is None


@pytest.mark.parametrize(
    "metadata",
    [
        {"status": '"draft"'},
        {"status": '"published"'},
        {"date": '"2026-02-30"'},
        {"date": '"2026-1-01"'},
        {"date": "2026-11-08"},
        {"github_url": '"https://github.com/other/repo/releases/tag/0.13.0"'},
        {"github_url": f'"{REPOSITORY}/releases/tag/0.12.1"'},
        {"title": '""'},
        {"unknown": "true"},
    ],
)
def test_invalid_metadata_is_rejected(notes, tmp_path, metadata):
    with pytest.raises(ValueError):
        notes.load_release(write_fixture(tmp_path, **metadata))


def test_invalid_files_are_rejected(notes, tmp_path):
    with pytest.raises(ValueError, match="empty"):
        notes.load_release(write_fixture(tmp_path, body="\n"))
    with pytest.raises(ValueError, match="LF"):
        notes.load_release(write_fixture(tmp_path, body="## Fixes\r\n"))
    renamed = write_fixture(tmp_path).rename(tmp_path / "0.13.1.md")
    with pytest.raises(ValueError, match="filename"):
        notes.load_release(renamed)
    missing = tmp_path / "0.14.0.md"
    missing.write_text("# Missing metadata\n", encoding="utf-8")
    with pytest.raises(ValueError, match="front matter"):
        notes.load_release(missing)


@pytest.mark.parametrize("version", ["../0.13.0", "v0.13.0", "00.13.0", "0.13", "0.13.0+local", "0.9.1-rc.1"])
def test_versions_use_bare_release_tags(notes, version):
    with pytest.raises(ValueError):
        notes.version_key(version)


def test_numeric_version_order_including_prereleases(notes, tmp_path):
    for version in ("0.13.0rc1", "0.9.0", "0.13.0", "0.10.0", "0.13.0b2", "0.13.0a1", "0.13.0rc10"):
        write_fixture(tmp_path, version)
    assert [release.version for release in notes.load_releases(tmp_path)] == [
        "0.13.0",
        "0.13.0rc10",
        "0.13.0rc1",
        "0.13.0b2",
        "0.13.0a1",
        "0.10.0",
        "0.9.0",
    ]


def test_stale_planned_release_and_empty_directory_are_rejected(notes, tmp_path):
    with pytest.raises(ValueError, match="no release notes"):
        notes.load_releases(tmp_path)
    write_fixture(tmp_path, "0.12.0")
    write_fixture(tmp_path, "0.12.1", date='"2025-04-07"', status='"published"')
    with pytest.raises(ValueError, match="planned 0.12.0 is older than published 0.12.1"):
        notes.load_releases(tmp_path)


def test_demotes_headings_but_preserves_both_kinds_of_fences(notes):
    body = "# One\n## Two\n````python\n# Inside\n```\n## Still inside\n````\n~~~\n# Inside\n~~~\n### Three\n"
    assert notes.demote_headings(body) == (
        "## One\n### Two\n````python\n# Inside\n```\n## Still inside\n````\n~~~\n# Inside\n~~~\n#### Three\n"
    )


def test_render_distinguishes_planned_and_published_releases(notes, tmp_path):
    planned = notes.load_release(write_fixture(tmp_path))
    published = notes.Release(
        "0.12.1", "Spark On K8S 0.12.1", "2025-04-07", "published", "https://example.invalid", "## Fixes\n"
    )
    rendered = notes.render_index([planned, published])
    assert "**In preparation**" in rendered
    assert "Published 2025-04-07 · [GitHub release](https://example.invalid)" in rendered
    assert "### Fixes" in rendered
    assert planned.body == "## Features\n\nA change.\n"


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("https://hussein.awala.fr/spark-on-k8s/0.13.0/", "https://hussein.awala.fr/spark-on-k8s/develop/"),
        (
            "https://hussein.awala.fr/spark-on-k8s/0.13.0/user-guide/installation/?view=full#current-source",
            "https://hussein.awala.fr/spark-on-k8s/develop/user-guide/installation/?view=full#current-source",
        ),
        ("/spark-on-k8s/0.13.0", "/spark-on-k8s/develop"),
        ("//hussein.awala.fr/spark-on-k8s/0.13.0/", "//hussein.awala.fr/spark-on-k8s/develop/"),
        (
            f"{REPOSITORY}/compare/0.12.1...0.13.0?expand=1#files_bucket",
            f"{REPOSITORY}/compare/0.12.1...main?expand=1#files_bucket",
        ),
    ],
)
def test_planned_links_open_development_previews(notes, source, expected):
    assert notes.release_preview_url(source, {"0.13.0"}) == expected
    assert notes.release_preview_url(source, set()) == source


@pytest.mark.parametrize(
    "url",
    [
        "https://hussein.awala.fr/spark-on-k8s/0.12.1/user-guide/installation/",
        "https://hussein.awala.fr/spark-on-k8s/0.13.01/",
        "https://hussein.awala.fr/spark-on-k8s/release-notes/",
        "https://hussein.awala.fr/async-batcher/0.13.0/",
        "https://other.example/spark-on-k8s/0.13.0/",
        f"{REPOSITORY}/releases/tag/0.13.0",
        f"{REPOSITORY}/compare/0.12.0...0.12.1",
        "https://github.com/another/repository/compare/0.12.1...0.13.0",
        "#spark-on-k8s-0130",
        "mailto:maintainer@example.invalid",
    ],
)
def test_published_external_and_other_links_are_unchanged(notes, url):
    assert notes.release_preview_url(url, {"0.13.0"}) == url


def test_repository_notes_are_valid_and_have_at_most_one_draft(notes):
    releases = notes.load_releases(ROOT / "release-notes")
    assert [release.status for release in releases].count("planned") <= 1
    assert {release.version for release in releases} >= {"0.2.0", "0.12.1"}


def test_cli_extract_preserves_each_repository_body_exactly(tmp_path):
    for source in sorted((ROOT / "release-notes").glob("*.md")):
        output = tmp_path / f"{source.stem}.md"
        script = ROOT / "scripts" / "release_notes.py"
        subprocess.run([sys.executable, script, "extract", source.stem, "--output", output], check=True)
        assert output.read_bytes() == source.read_bytes().split(b"\n---\n\n", 1)[1], source.name
