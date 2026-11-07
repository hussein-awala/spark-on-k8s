from __future__ import annotations

import runpy
from pathlib import Path

CHECK_SITE = runpy.run_path(str(Path(__file__).parents[2] / "scripts" / "check_docs.py"))["check_site"]


def test_built_links_handle_subdirectories_encoded_anchors_and_assets(tmp_path):
    (tmp_path / "guide").mkdir()
    (tmp_path / "asset.js").write_text("// bundled resource")
    (tmp_path / "index.html").write_text(
        '<a href="guide/?mode=example#hello%20world">Guide</a>'
        '<a href="https://docs.example/latest/">Released docs</a>'
        '<a href="mailto:maintainer@example.com">Contact</a>'
        '<a href="#top">Top</a><h1 id="top">Home</h1>'
    )
    (tmp_path / "guide" / "index.html").write_text(
        '<a name="hello world"></a><a href="../#top">Home</a><script src="../asset.js"></script>'
    )
    assert CHECK_SITE(tmp_path) == []


def test_broken_rendered_links_fail_with_source_and_target(tmp_path):
    (tmp_path / "index.html").write_text(
        '<a href="missing/">Missing</a><a href="#absent">Missing anchor</a>'
        '<img src="image.png"><a href="../outside.html">Outside</a>'
    )
    errors = CHECK_SITE(tmp_path)
    assert len(errors) == 4
    assert all(error.startswith("index.html:") for error in errors)
    assert any("missing/" in error and "does not exist" in error for error in errors)
    assert any("#absent" in error and "anchor does not exist" in error for error in errors)
    assert any("image.png" in error and "does not exist" in error for error in errors)
    assert any("../outside.html" in error and "leaves" in error for error in errors)


def test_empty_build_is_not_a_passing_link_check(tmp_path):
    assert CHECK_SITE(tmp_path) == ["index.html: missing documentation homepage"]


def test_file_with_trailing_slash_is_not_a_valid_page_or_asset_url(tmp_path):
    (tmp_path / "page.html").write_text('<h1 id="valid">Page</h1>')
    (tmp_path / "asset.js").write_text("// bundled resource")
    (tmp_path / "index.html").write_text(
        '<a href="page.html#valid">Valid page</a><script src="asset.js"></script>'
        '<a href="page.html/">Not a directory</a>'
        '<a href="page.html/#valid">Not a directory anchor</a>'
        '<script src="asset.js/"></script>'
    )
    errors = CHECK_SITE(tmp_path)
    assert len(errors) == 3
    assert all("target does not exist" in error for error in errors)
    assert any("page.html/#valid" in error for error in errors)
    assert any("asset.js/" in error for error in errors)
