"""Validate release metadata and record frozen container inputs and installed contents."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import re
import shutil
import sys
import tarfile
import zipfile
from email.parser import BytesParser
from importlib import metadata
from pathlib import Path


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def distribution_metadata(path: Path):
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as wheel:
            members = [name for name in wheel.namelist() if name.endswith(".dist-info/METADATA")]
            if len(members) != 1:
                raise ValueError("Wheel must have exactly one distribution METADATA file")
            return BytesParser().parsebytes(wheel.read(members[0]))
    with tarfile.open(path, "r:gz") as source:
        members = [
            member
            for member in source.getmembers()
            if member.name.count("/") == 1 and member.name.endswith("/PKG-INFO")
        ]
        if len(members) != 1:
            raise ValueError("Source distribution must have one root PKG-INFO file")
        with source.extractfile(members[0]) as contents:
            return BytesParser().parsebytes(contents.read())


def frontend_inventory(wheel: Path) -> list[dict]:
    result = []
    with zipfile.ZipFile(wheel) as archive:
        for name in (
            "spark_on_k8s/api/webserver/static/ui/licenses.md",
            "spark_on_k8s/airflow/ui/static/licenses.md",
        ):
            content = archive.read(name)
            components = [
                {"name": package, "version": version, "license": license_name}
                for package, version, license_name in re.findall(
                    r"^## (.+?) - (\S+) \(([^)]+)\)$", content.decode(), re.MULTILINE
                )
            ]
            if not components and b"does not bundle any dependencies with licenses" not in content:
                raise ValueError(f"No bundled frontend components identified in {name}")
            result.append(
                {"notice": name, "sha256": hashlib.sha256(content).hexdigest(), "components": components}
            )
    return result


def prepare(args):
    wheels = sorted(args.dist.glob("*.whl"))
    sources = sorted(args.dist.glob("*.tar.gz"))
    if len(wheels) != 1 or len(sources) != 1:
        raise ValueError("Expected exactly one fresh wheel and source distribution")
    for path in [*wheels, *sources]:
        info = distribution_metadata(path)
        if info["Name"] != "spark-on-k8s" or info["Version"] != args.version:
            raise ValueError(f"Distribution metadata does not match spark-on-k8s {args.version}: {path.name}")
    match = re.search(
        r"^FROM (python:[^\s]+@sha256:[0-9a-f]{64})$", args.dockerfile.read_text(), re.MULTILINE
    )
    if not match:
        raise ValueError("API Dockerfile must pin its Python base image by digest")
    requirements = args.requirements.read_text()
    if "--hash=sha256:" not in requirements:
        raise ValueError("The API requirements export must contain hashes")
    if re.search(
        r"^(?:-e|spark[-_]on[-_]k8s|apache[-_]airflow|pytest)(?:\b|\[)",
        requirements,
        re.MULTILINE | re.IGNORECASE,
    ):
        raise ValueError("API export must exclude the project, Airflow and test dependencies")
    args.output.mkdir(parents=True, exist_ok=True)
    wheel = args.output / wheels[0].name
    shutil.copyfile(wheels[0], wheel)
    target_requirements = args.output / "requirements.txt"
    if args.requirements.resolve() != target_requirements.resolve():
        shutil.copyfile(args.requirements, target_requirements)
    payload = {
        "schema_version": 1,
        "package": "spark-on-k8s",
        "version": args.version,
        "commit": args.commit,
        "base_image": match.group(1),
        "uv_lock_sha256": sha256(args.lock),
        "requirements_sha256": sha256(target_requirements),
        "bundled_frontend": frontend_inventory(wheel),
        "distributions": [{"file": path.name, "sha256": sha256(path)} for path in [*wheels, *sources]],
    }
    (args.output / "manifest.json").write_text(json.dumps(payload, indent=2) + "\n")
    (args.output / "inputs.sha256").write_text(
        f"{sha256(wheel)}  {wheel.name}\n{sha256(target_requirements)}  requirements.txt\n"
    )
    shutil.copyfile(Path(__file__), args.output / "verify.py")


def inventory(directory: Path):
    manifest = json.loads((directory / "manifest.json").read_text())
    if metadata.version("spark-on-k8s") != manifest["version"]:
        raise ValueError("Installed package version does not match the release manifest")
    if sha256(directory / "requirements.txt") != manifest["requirements_sha256"]:
        raise ValueError("Frozen requirements checksum mismatch")
    for distribution in manifest["distributions"]:
        if distribution["file"].endswith(".whl"):
            if sha256(directory / distribution["file"]) != distribution["sha256"]:
                raise ValueError("Release wheel checksum mismatch")
    packages = sorted(
        ({"name": item.metadata["Name"], "version": item.version} for item in metadata.distributions()),
        key=lambda item: item["name"].lower(),
    )
    evidence = {
        **manifest,
        "python": platform.python_version(),
        "machine": platform.machine(),
        "installed": packages,
    }
    (directory / "installed.json").write_text(json.dumps(evidence, indent=2) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    prepare_parser = subparsers.add_parser("prepare")
    for name in ("dist", "lock", "requirements", "dockerfile", "output"):
        prepare_parser.add_argument(f"--{name}", type=Path, required=True)
    prepare_parser.add_argument("--version", required=True)
    prepare_parser.add_argument("--commit", required=True)
    inventory_parser = subparsers.add_parser("inventory")
    inventory_parser.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.action == "prepare":
            prepare(args)
        else:
            inventory(args.directory)
        return 0
    except (ValueError, KeyError, OSError) as error:
        print(f"Release artifact validation failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
