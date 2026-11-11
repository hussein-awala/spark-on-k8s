"""Check that the experimental dbt adapter is consistently versioned and publishable."""

from __future__ import annotations

import argparse
import re
import runpy
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROJECT = ROOT / "integrations" / "dbt"
VERSION_FILES = (
    (Path("src/dbt/adapters/spark_on_k8s/__version__.py"), re.compile(r'^version = "([^"]+)"$', re.M)),
    (Path("src/spark_on_k8s_dbt/__init__.py"), re.compile(r'^__version__ = "([^"]+)"$', re.M)),
)
# Adapter versions are bare tags like the package release, for example 0.1.0 or 0.2.0rc1.
validate_version = runpy.run_path(str(Path(__file__).with_name("release_eligibility.py")))["validate_version"]
SDK_REQUIREMENT = re.compile(r"spark-on-k8s(?:\[[^\]]*\])?\s*(.*)")


def check_project(version: str, project: Path = PROJECT) -> list[str]:
    errors = []
    try:
        validate_version(version)
    except ValueError as error:
        errors.append(f"version: {error}")
    metadata = tomllib.loads((project / "pyproject.toml").read_text(encoding="utf-8"))
    declared = {"pyproject.toml": metadata["project"]["version"]}
    for path, pattern in VERSION_FILES:
        match = pattern.search((project / path).read_text(encoding="utf-8"))
        declared[path.as_posix()] = match[1] if match else None
    errors.extend(
        f"{name} declares {value}, not {version}" for name, value in declared.items() if value != version
    )
    sdk = [
        match[1] for item in metadata["project"]["dependencies"] if (match := SDK_REQUIREMENT.fullmatch(item))
    ]
    if len(sdk) != 1:
        errors.append("dependencies must contain exactly one spark-on-k8s requirement")
    elif re.search(r"\.dev|\+", sdk[0]) or not re.search(r"\d", sdk[0]):
        # The checkout's development version and local builds are never on PyPI.
        errors.append(f"spark-on-k8s requirement {sdk[0]!r} must name a released SDK version")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    args = parser.parse_args()
    errors = check_project(args.version)
    for error in errors:
        print(error)
    if not errors:
        print(f"dbt-spark-on-k8s {args.version} metadata is publishable")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
