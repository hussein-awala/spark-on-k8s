"""Describe checked per-architecture API image layouts and assemble the published image index.

Each architecture check runs the image from the OCI layout that BuildKit exported, pulled by its
digest. The multi-architecture index is assembled here from those same layouts without rebuilding,
so its platform manifests are byte-for-byte the ones that passed the checks.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import tarfile
from pathlib import Path

INDEX = "application/vnd.oci.image.index.v1+json"
MANIFEST = "application/vnd.oci.image.manifest.v1+json"
LAYOUT = {"imageLayoutVersion": "1.0.0"}
# BuildKit links each SBOM/provenance attestation manifest to the image manifest it describes.
REFERENCE_TYPE = "vnd.docker.reference.type"
REFERENCE_DIGEST = "vnd.docker.reference.digest"
REFERENCE_NAME = "org.opencontainers.image.ref.name"
DIGEST = re.compile(r"sha256:([0-9a-f]{64})")
CHUNK = 1 << 20


def digest_of(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def blob_name(digest: str) -> str:
    match = DIGEST.fullmatch(digest)
    if not match:
        raise ValueError(f"Unsupported blob digest: {digest!r}")
    return f"blobs/sha256/{match.group(1)}"


class Archive:
    """An OCI image layout tarball, read by blob name without extracting member paths."""

    def __init__(self, path: Path):
        self.path = path
        self._tar = tarfile.open(path)
        self._members = {
            member.name.removeprefix("./"): member for member in self._tar.getmembers() if member.isfile()
        }

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self._tar.close()

    def file(self, name: str) -> bytes:
        member = self._members.get(name)
        if member is None:
            raise ValueError(f"{self.path.name} has no {name}")
        with self._tar.extractfile(member) as source:
            return source.read()

    def json_blob(self, descriptor: dict) -> dict:
        data = self.file(blob_name(descriptor["digest"]))
        if len(data) != descriptor["size"] or digest_of(data) != descriptor["digest"]:
            raise ValueError(f"{self.path.name}: {descriptor['digest']} does not match its content")
        return json.loads(data)

    def copy_blob(self, descriptor: dict, layout: Path) -> None:
        name = blob_name(descriptor["digest"])
        target = layout / name
        if target.exists():
            return
        member = self._members.get(name)
        if member is None:
            raise ValueError(f"{self.path.name} has no {name}")
        target.parent.mkdir(parents=True, exist_ok=True)
        partial = target.with_name(target.name + ".partial")
        checksum = hashlib.sha256()
        with self._tar.extractfile(member) as source, partial.open("wb") as output:
            while chunk := source.read(CHUNK):
                checksum.update(chunk)
                output.write(chunk)
        if (
            partial.stat().st_size != descriptor["size"]
            or "sha256:" + checksum.hexdigest() != descriptor["digest"]
        ):
            partial.unlink()
            raise ValueError(f"{self.path.name}: {descriptor['digest']} does not match its content")
        partial.rename(target)


def read_layout(archive: Archive, architecture: str) -> tuple[dict, dict]:
    """Return the check record and the image index of one exported architecture."""
    if json.loads(archive.file("oci-layout")) != LAYOUT:
        raise ValueError(f"{archive.path.name} is not an OCI image layout 1.0.0")
    roots = json.loads(archive.file("index.json"))["manifests"]
    if len(roots) != 1 or roots[0]["mediaType"] != INDEX:
        raise ValueError(f"{archive.path.name} must contain exactly one image index")
    index = archive.json_blob(roots[0])
    entries = index["manifests"]
    attestations = [
        entry
        for entry in entries
        if entry.get("annotations", {}).get(REFERENCE_TYPE) == "attestation-manifest"
    ]
    images = [entry for entry in entries if entry not in attestations]
    if (
        len(images) != 1
        or images[0]["mediaType"] != MANIFEST
        or images[0].get("platform", {}).get("os") != "linux"
        or images[0]["platform"].get("architecture") != architecture
    ):
        raise ValueError(f"{archive.path.name} must contain exactly one linux/{architecture} image manifest")
    image = images[0]
    archive.json_blob(image)
    # The workflow documents SBOM and provenance attestations; publishing without them would not match.
    if not attestations or any(
        entry["annotations"].get(REFERENCE_DIGEST) != image["digest"] for entry in attestations
    ):
        raise ValueError(f"{archive.path.name} must carry BuildKit attestations for its image manifest")
    record = {
        "architecture": architecture,
        "index_digest": roots[0]["digest"],
        "manifest_digest": image["digest"],
        "attestation_digests": [entry["digest"] for entry in attestations],
    }
    return record, index


def describe(path: Path, architecture: str) -> dict:
    with Archive(path) as archive:
        return read_layout(archive, architecture)[0]


def assemble(images: list[tuple[str, Path, dict]], layout: Path) -> dict:
    """Write one OCI layout whose index lists the checked manifests of every architecture."""
    if layout.exists() and any(layout.iterdir()):
        raise ValueError(f"{layout} must be empty")
    architectures = [architecture for architecture, _, _ in images]
    if not architectures or len(set(architectures)) != len(architectures):
        raise ValueError("Each architecture must be assembled exactly once")
    manifests = []
    platforms = []
    for architecture, path, checked in images:
        with Archive(path) as archive:
            record, index = read_layout(archive, architecture)
            if record != checked:
                raise ValueError(f"{path.name} is not the {architecture} image that passed its checks")
            for entry in index["manifests"]:
                if entry["mediaType"] != MANIFEST:
                    raise ValueError(f"{path.name}: unexpected nested {entry['mediaType']}")
                manifest = archive.json_blob(entry)
                for blob in [manifest["config"], *manifest["layers"]]:
                    archive.copy_blob(blob, layout)
                archive.copy_blob(entry, layout)
                # Descriptors are copied verbatim, so registries see the checked manifest digests.
                manifests.append(entry)
        platforms.append(record)
    data = json.dumps({"schemaVersion": 2, "mediaType": INDEX, "manifests": manifests}).encode()
    digest = digest_of(data)
    (layout / blob_name(digest)).write_bytes(data)
    (layout / "oci-layout").write_text(json.dumps(LAYOUT))
    descriptor = {
        "mediaType": INDEX,
        "digest": digest,
        "size": len(data),
        "annotations": {REFERENCE_NAME: "release"},
    }
    (layout / "index.json").write_text(json.dumps({"schemaVersion": 2, "manifests": [descriptor]}))
    return {"digest": digest, "platforms": platforms}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="action", required=True)
    describe_parser = subparsers.add_parser("describe")
    describe_parser.add_argument("--archive", type=Path, required=True)
    describe_parser.add_argument("--architecture", required=True)
    describe_parser.add_argument("--output", type=Path, required=True)
    assemble_parser = subparsers.add_parser("assemble")
    assemble_parser.add_argument(
        "--image",
        nargs=3,
        action="append",
        required=True,
        metavar=("ARCHITECTURE", "ARCHIVE", "CHECKED"),
        help="Exported layout tarball and the describe record its check saved",
    )
    assemble_parser.add_argument("--output", type=Path, required=True)
    assemble_parser.add_argument("--summary", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.action == "describe":
            result = describe(args.archive, args.architecture)
            args.output.write_text(json.dumps(result, indent=2) + "\n")
            print(result["index_digest"])
        else:
            images = [
                (architecture, Path(archive), json.loads(Path(checked).read_text()))
                for architecture, archive, checked in args.image
            ]
            args.output.mkdir(parents=True, exist_ok=True)
            result = assemble(images, args.output)
            args.summary.write_text(json.dumps(result, indent=2) + "\n")
            print(result["digest"])
        return 0
    except (ValueError, KeyError, TypeError, OSError, tarfile.TarError) as error:
        print(f"Release image validation failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
