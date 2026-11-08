"""Publish notebook downloads from the canonical files in this docs checkout."""

from __future__ import annotations

from io import BytesIO
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


def generate_downloads() -> None:
    import mkdocs_gen_files

    source = Path("examples/jupyter")
    destination = Path("downloads/jupyter")
    names = ("spark_connect.ipynb", "notebook_support.py")
    # Read every required source before publishing anything; missing examples must
    # fail the docs build rather than produce a partial or stale download bundle.
    files = {name: (source / name).read_bytes() for name in names}
    archive = BytesIO()
    with ZipFile(archive, "w", compression=ZIP_DEFLATED) as bundle:
        for name, content in files.items():
            info = ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            bundle.writestr(info, content)
    for name, content in {**files, "jupyter-connect.zip": archive.getvalue()}.items():
        with mkdocs_gen_files.open(destination / name, "wb") as stream:
            stream.write(content)


if __name__ in {"__main__", "<run_path>"}:
    generate_downloads()
