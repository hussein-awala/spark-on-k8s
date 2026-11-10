from __future__ import annotations

import hashlib
import io
import json
import re
import tarfile
from pathlib import Path

import pytest
import yaml

INDEX = "application/vnd.oci.image.index.v1+json"
MANIFEST = "application/vnd.oci.image.manifest.v1+json"
WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "release.yml"


def descriptor(data: bytes, media_type: str, **fields) -> dict:
    return {
        "mediaType": media_type,
        "digest": "sha256:" + hashlib.sha256(data).hexdigest(),
        "size": len(data),
        **fields,
    }


def export(path: Path, architecture: str, *, attest=True, platform=None, alter=None, build="") -> Path:
    """Write an OCI layout tarball shaped like BuildKit's export of one attested platform."""
    blobs = {}

    def add(data: bytes, media_type: str, **fields) -> dict:
        result = descriptor(data, media_type, **fields)
        blobs[result["digest"]] = data
        return result

    def manifest(config: dict, layers: list[dict], **fields) -> dict:
        config_descriptor = add(json.dumps(config).encode(), "application/vnd.oci.image.config.v1+json")
        body = {"schemaVersion": 2, "mediaType": MANIFEST, "config": config_descriptor, "layers": layers}
        return add(json.dumps(body).encode(), MANIFEST, **fields)

    # The base layer is shared, as in the real image, so assembly must store it once.
    layers = [
        add(b"base layer", "application/vnd.oci.image.layer.v1.tar+gzip"),
        add(f"api layer {architecture}{build}".encode(), "application/vnd.oci.image.layer.v1.tar+gzip"),
    ]
    image = manifest(
        {"architecture": architecture, "os": "linux"},
        layers,
        platform=platform or {"architecture": architecture, "os": "linux"},
    )
    entries = [image]
    if attest:
        statement = add(f"provenance {architecture}".encode(), "application/vnd.in-toto+json")
        entries.append(
            manifest(
                {"architecture": "unknown", "os": "unknown"},
                [statement],
                platform={"architecture": "unknown", "os": "unknown"},
                annotations={
                    "vnd.docker.reference.digest": image["digest"],
                    "vnd.docker.reference.type": "attestation-manifest",
                },
            )
        )
    root = add(json.dumps({"schemaVersion": 2, "mediaType": INDEX, "manifests": entries}).encode(), INDEX)
    if alter:
        blobs[alter(image, layers)] = b"altered"
    files = {
        "oci-layout": json.dumps({"imageLayoutVersion": "1.0.0"}).encode(),
        "index.json": json.dumps({"schemaVersion": 2, "manifests": [root]}).encode(),
        **{f"blobs/sha256/{digest.removeprefix('sha256:')}": data for digest, data in blobs.items()},
    }
    with tarfile.open(path, "w") as archive:
        for name, data in files.items():
            member = tarfile.TarInfo(name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
    return path


def checked(module, tmp_path, architecture, **options):
    archive = export(tmp_path / f"{architecture}.tar", architecture, **options)
    return architecture, archive, module.describe(archive, architecture)


def test_describe_records_the_checked_manifest_and_its_attestations(release_modules, tmp_path):
    module = release_modules["image"]
    archive = export(tmp_path / "arm64.tar", "arm64", platform={"architecture": "arm64", "os": "linux"})
    record = module.describe(archive, "arm64")
    with tarfile.open(archive) as layout:
        root = json.loads(layout.extractfile("index.json").read())["manifests"][0]
        index = json.loads(layout.extractfile(f"blobs/sha256/{root['digest'][7:]}").read())
    assert record == {
        "architecture": "arm64",
        "index_digest": root["digest"],
        "manifest_digest": index["manifests"][0]["digest"],
        "attestation_digests": [index["manifests"][1]["digest"]],
    }


def test_describe_rejects_another_architecture(release_modules, tmp_path):
    archive = export(tmp_path / "amd64.tar", "amd64")
    with pytest.raises(ValueError, match="linux/arm64 image manifest"):
        release_modules["image"].describe(archive, "arm64")


def test_image_without_sbom_or_provenance_is_not_published(release_modules, tmp_path):
    archive = export(tmp_path / "amd64.tar", "amd64", attest=False)
    with pytest.raises(ValueError, match="attestations"):
        release_modules["image"].describe(archive, "amd64")


def test_describe_rejects_an_altered_manifest(release_modules, tmp_path):
    archive = export(tmp_path / "amd64.tar", "amd64", alter=lambda image, layers: image["digest"])
    with pytest.raises(ValueError, match="does not match its content"):
        release_modules["image"].describe(archive, "amd64")


def test_assembled_index_publishes_the_checked_manifests_unchanged(release_modules, tmp_path):
    module = release_modules["image"]
    images = [checked(module, tmp_path, "amd64"), checked(module, tmp_path, "arm64")]
    layout = tmp_path / "layout"
    summary = module.assemble(images, layout)

    root = json.loads((layout / "index.json").read_text())["manifests"]
    assert root == [
        {
            "mediaType": INDEX,
            "digest": summary["digest"],
            "size": (layout / "blobs" / "sha256" / summary["digest"][7:]).stat().st_size,
            "annotations": {"org.opencontainers.image.ref.name": "release"},
        }
    ]
    index = json.loads((layout / "blobs" / "sha256" / summary["digest"][7:]).read_bytes())
    platforms = [entry for entry in index["manifests"] if entry["platform"]["os"] == "linux"]
    assert [(entry["platform"]["architecture"], entry["digest"]) for entry in platforms] == [
        (record["architecture"], record["manifest_digest"]) for _, _, record in images
    ]
    assert summary["platforms"] == [record for _, _, record in images]
    # A complete, content-addressed layout: registries receive exactly these bytes.
    names = {path.name for path in (layout / "blobs" / "sha256").iterdir()}
    for path in (layout / "blobs" / "sha256").iterdir():
        assert hashlib.sha256(path.read_bytes()).hexdigest() == path.name
    referenced = {summary["digest"][7:]}
    for entry in index["manifests"]:
        manifest = json.loads((layout / "blobs" / "sha256" / entry["digest"][7:]).read_bytes())
        referenced |= {item["digest"][7:] for item in [entry, manifest["config"], *manifest["layers"]]}
    assert names == referenced
    assert (
        len([name for name in names if (layout / "blobs" / "sha256" / name).read_bytes() == b"base layer"])
        == 1
    )


def test_assembly_refuses_an_image_other_than_the_checked_one(release_modules, tmp_path):
    module = release_modules["image"]
    architecture, _, record = checked(module, tmp_path, "amd64")
    # A rebuild of the same inputs need not be identical, and this one was never checked.
    rebuilt = export(tmp_path / "rebuilt.tar", "amd64", build=" rebuilt")
    with pytest.raises(ValueError, match="passed its checks"):
        module.assemble([(architecture, rebuilt, record)], tmp_path / "layout")


def test_assembly_rejects_an_altered_layer(release_modules, tmp_path):
    module = release_modules["image"]
    archive = export(tmp_path / "amd64.tar", "amd64", alter=lambda image, layers: layers[1]["digest"])
    images = [("amd64", archive, module.describe(archive, "amd64"))]
    with pytest.raises(ValueError, match="does not match its content"):
        module.assemble(images, tmp_path / "layout")
    assert not list((tmp_path / "layout").rglob("*.partial"))


def test_each_architecture_is_assembled_once(release_modules, tmp_path):
    module = release_modules["image"]
    image = checked(module, tmp_path, "amd64")
    with pytest.raises(ValueError, match="exactly once"):
        module.assemble([image, image], tmp_path / "layout")


def test_release_workflow_signs_only_the_checked_digest():
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    # PyPI trusted publishing and cosign are the only steps that may request an OIDC token.
    assert {name for name, job in jobs.items() if job.get("permissions", {}).get("id-token") == "write"} == {
        "pypi-publish",
        "image-sign",
    }
    # One build before any publication; registries receive the assembled layout, not a rebuild.
    builders = {
        name
        for name, job in jobs.items()
        for step in job["steps"]
        if step.get("uses", "").startswith("docker/build-push-action@")
    }
    assert builders == {"container-check"}
    assert "container-image" in jobs["pypi-publish"]["needs"]
    push = next(step for step in jobs["docker-release"]["steps"] if step.get("id") == "push")
    assert push["env"]["IMAGE_DIGEST"] == "${{ needs.container-image.outputs.digest }}"
    assert "--preserve-digests" in push["run"]
    assert jobs["docker-release"]["outputs"]["digest"] == "${{ steps.push.outputs.digest }}"
    sign = jobs["image-sign"]
    assert sign["env"]["IMAGE_DIGEST"] == "${{ needs.docker-release.outputs.digest }}"
    installer = sign["steps"][0]["uses"]
    assert re.fullmatch(r"sigstore/cosign-installer@[0-9a-f]{40}", installer)
    commands = [
        line.strip()
        for step in sign["steps"]
        for line in step.get("run", "").splitlines()
        if line.strip().startswith("cosign sign")
    ]
    assert commands == ['cosign sign --yes "$GHCR_IMAGE@$IMAGE_DIGEST" "$DOCKERHUB_IMAGE@$IMAGE_DIGEST"']
