from __future__ import annotations

import io
import json
import tarfile
import zipfile
from types import SimpleNamespace

import pytest


def inputs(tmp_path, version="0.13.0"):
    dist = tmp_path / "dist"
    dist.mkdir()
    wheel = dist / "spark_on_k8s-0.13.0-py3-none-any.whl"
    metadata = f"Metadata-Version: 2.3\nName: spark-on-k8s\nVersion: {version}\n".encode()
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("spark_on_k8s-0.13.0.dist-info/METADATA", metadata)
        archive.writestr("spark_on_k8s/api/webserver/static/ui/licenses.md", "## react - 19.3.1 (MIT)\n")
        archive.writestr(
            "spark_on_k8s/airflow/ui/static/licenses.md",
            "The app does not bundle any dependencies with licenses.\n",
        )
    with tarfile.open(dist / "spark_on_k8s-0.13.0.tar.gz", "w:gz") as archive:
        member = tarfile.TarInfo("spark_on_k8s-0.13.0/PKG-INFO")
        member.size = len(metadata)
        archive.addfile(member, io.BytesIO(metadata))
    lock = tmp_path / "uv.lock"
    lock.write_text("version = 1\n")
    requirements = tmp_path / "requirements.txt"
    requirements.write_text("kubernetes==36.0.3 --hash=sha256:" + "a" * 64 + "\n")
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text("FROM python:3.11-slim@sha256:" + "b" * 64 + "\n")
    return SimpleNamespace(
        dist=dist,
        version="0.13.0",
        commit="c" * 40,
        lock=lock,
        requirements=requirements,
        dockerfile=dockerfile,
        output=tmp_path / "release",
    )


def test_manifest_identifies_actual_artifacts_and_inputs(release_modules, tmp_path):
    module = release_modules["artifacts"]
    args = inputs(tmp_path)
    module.prepare(args)
    result = json.loads((args.output / "manifest.json").read_text())
    assert result["version"] == "0.13.0"
    assert result["commit"] == "c" * 40
    assert result["uv_lock_sha256"] == module.sha256(args.lock)
    assert result["requirements_sha256"] == module.sha256(args.requirements)
    assert len(result["distributions"]) == 2
    assert result["bundled_frontend"][1]["components"] == []
    assert result["bundled_frontend"][0]["components"] == [
        {"name": "react", "version": "19.3.1", "license": "MIT"}
    ]
    assert result["distributions"][0]["sha256"] == module.sha256(next(args.dist.glob("*.whl")))
    assert (args.output / "verify.py").is_file()


def test_metadata_mismatch_blocks_release(release_modules, tmp_path):
    with pytest.raises(ValueError, match="metadata does not match"):
        release_modules["artifacts"].prepare(inputs(tmp_path, version="0.11.0.dev0"))


def test_ambiguous_stale_build_output_blocks_release(release_modules, tmp_path):
    args = inputs(tmp_path)
    (args.dist / "stale.whl").write_bytes(b"stale")
    with pytest.raises(ValueError, match="exactly one"):
        release_modules["artifacts"].prepare(args)


def test_mutable_container_base_is_rejected(release_modules, tmp_path):
    args = inputs(tmp_path)
    args.dockerfile.write_text("FROM python:3.11-slim\n")
    with pytest.raises(ValueError, match="pin"):
        release_modules["artifacts"].prepare(args)


@pytest.mark.parametrize(
    "requirements",
    [
        "kubernetes==36.0.3\n",
        "apache-airflow==3.4.0 --hash=sha256:abc\n",
        "-e .\nkubernetes==36.0.3 --hash=sha256:abc\n",
    ],
)
def test_unhashed_or_wrong_profile_export_is_rejected(release_modules, tmp_path, requirements):
    args = inputs(tmp_path)
    args.requirements.write_text(requirements)
    with pytest.raises(ValueError):
        release_modules["artifacts"].prepare(args)


def test_inventory_rejects_altered_wheel(release_modules, tmp_path, monkeypatch):
    module = release_modules["artifacts"]
    args = inputs(tmp_path)
    module.prepare(args)
    monkeypatch.setattr(module.metadata, "version", lambda name: "0.13.0")
    next(args.output.glob("*.whl")).write_bytes(b"altered")
    with pytest.raises(ValueError, match="wheel checksum"):
        module.inventory(args.output)


def test_inventory_rejects_wrong_installed_version(release_modules, tmp_path, monkeypatch):
    module = release_modules["artifacts"]
    args = inputs(tmp_path)
    module.prepare(args)
    monkeypatch.setattr(module.metadata, "version", lambda name: "0.12.1")
    with pytest.raises(ValueError, match="Installed package version"):
        module.inventory(args.output)


def test_inventory_captures_runtime_packages(release_modules, tmp_path, monkeypatch):
    module = release_modules["artifacts"]
    args = inputs(tmp_path)
    module.prepare(args)
    monkeypatch.setattr(module.metadata, "version", lambda name: "0.13.0")
    monkeypatch.setattr(
        module.metadata,
        "distributions",
        lambda: [SimpleNamespace(metadata={"Name": "spark-on-k8s"}, version="0.13.0")],
    )
    module.inventory(args.output)
    evidence = json.loads((args.output / "installed.json").read_text())
    assert evidence["installed"] == [{"name": "spark-on-k8s", "version": "0.13.0"}]
    assert evidence["python"]
    assert evidence["machine"]
