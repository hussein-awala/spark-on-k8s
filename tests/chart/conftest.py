"""Render a release-shaped chart without claiming a published image exists."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def release_chart(request, tmp_path, monkeypatch):
    if not hasattr(request.module, "CHART"):
        return
    original = Path(request.module.CHART)
    chart = tmp_path / "chart"
    shutil.copytree(original, chart)
    metadata = chart / "Chart.yaml"
    metadata.write_text(metadata.read_text().replace("appVersion: unreleased", "appVersion: 0.0.0-test"))
    monkeypatch.setattr(request.module, "CHART", chart)
