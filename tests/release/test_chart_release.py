from __future__ import annotations

import runpy
from pathlib import Path

import pytest

CHART_RELEASE = runpy.run_path(str(Path(__file__).parents[2] / "scripts" / "chart_release.py"))
check_inputs = CHART_RELEASE["check_inputs"]
published_versions = CHART_RELEASE["published_versions"]

# Shape of the published repository index, including its historical "v" prefixes.
INDEX = """apiVersion: v1
entries:
  spark-on-k8s:
  - apiVersion: v2
    appVersion: 0.6.0
    version: 0.3.0
  - apiVersion: v2
    appVersion: 0.5.0
    version: "v0.2.0"
generated: "2024-05-05T22:53:16Z"
"""


def test_published_versions_ignore_v_prefix_and_other_fields():
    assert published_versions(INDEX) == {"0.3.0", "0.2.0"}


def test_new_chart_for_ready_api_is_publishable():
    assert check_inputs("0.13.0", "0.4.0", INDEX) == []
    assert check_inputs("0.13.1rc1", "0.4.1-rc.1", INDEX) == []


@pytest.mark.parametrize(
    ("app_version", "chart_version", "message"),
    [
        ("0.12.1", "0.4.0", "requires API 0.13.0"),
        ("v0.13.0", "0.4.0", "bare X.Y.Z"),
        ("latest", "0.4.0", "bare X.Y.Z"),
        ("0.13.0", "0.3.0", "already published"),
        ("0.13.0", "0.2.0", "already published"),
        ("0.13.0", "v0.4.0", "chart_version: use X.Y.Z"),
        ("0.13.0", "0.4", "chart_version: use X.Y.Z"),
    ],
)
def test_unsafe_inputs_are_rejected(app_version, chart_version, message):
    errors = check_inputs(app_version, chart_version, INDEX)
    assert any(message in error for error in errors), errors
