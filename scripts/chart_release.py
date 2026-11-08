"""Check Helm chart publication inputs before the chart repository is updated."""

from __future__ import annotations

import argparse
import re
import runpy
import urllib.request
from pathlib import Path

INDEX_URL = "https://hussein.awala.fr/spark-on-k8s-chart/index.yaml"
# The chart's readiness probe uses /ready, which the API serves from this release on.
MIN_APP_VERSION = (0, 13, 0)
CHART_VERSION = re.compile(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z.-]+)?")
INDEX_VERSION = re.compile(r"""^\s+version:\s*["']?v?([^"'\s]+)["']?\s*$""")
# Chart app versions are the same bare tags that the package release accepts.
validate_app_version = runpy.run_path(str(Path(__file__).with_name("release_eligibility.py")))[
    "validate_version"
]


def published_versions(index: str) -> set[str]:
    """Return chart versions in a Helm repository index, ignoring a historical ``v`` prefix."""
    return {match[1] for line in index.splitlines() if (match := INDEX_VERSION.match(line))}


def check_inputs(app_version: str, chart_version: str, index: str) -> list[str]:
    errors = []
    try:
        validate_app_version(app_version)
    except ValueError as error:
        errors.append(f"app_version: {error}")
    else:
        release = tuple(int(part) for part in re.match(r"(\d+)\.(\d+)\.(\d+)", app_version).groups())
        if release < MIN_APP_VERSION:
            minimum = ".".join(map(str, MIN_APP_VERSION))
            errors.append(f"app_version: the chart's /ready probe requires API {minimum} or newer")
    if not CHART_VERSION.fullmatch(chart_version):
        errors.append("chart_version: use X.Y.Z, optionally followed by -prerelease")
    elif chart_version in published_versions(index):
        errors.append(f"chart_version: {chart_version} is already published; choose a new chart version")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-version", required=True)
    parser.add_argument("--chart-version", required=True)
    parser.add_argument("--index-url", default=INDEX_URL)
    args = parser.parse_args()
    try:
        with urllib.request.urlopen(args.index_url, timeout=30) as response:
            index = response.read().decode("utf-8")
    except OSError as error:
        # Without the published index, a republished version cannot be ruled out.
        print(f"Cannot read the published chart index: {error}")
        return 1
    errors = check_inputs(args.app_version, args.chart_version, index)
    for error in errors:
        print(error)
    if not errors:
        print(f"Chart {args.chart_version} for API {args.app_version} is publishable")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
