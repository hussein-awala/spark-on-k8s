from __future__ import annotations

import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path

import click

from spark_on_k8s.utils.run import RunHandle

receipt_option = click.option(
    "--receipt-file",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Atomically save the run identity before waiting. The destination must not exist.",
)


@contextmanager
def receipt_writer(destination: Path | None):
    """Check local storage before submission and publish without replacing an existing file."""
    if destination is None:
        yield lambda run: None
        return
    destination = destination.expanduser().absolute()
    if os.path.lexists(destination):
        raise FileExistsError("Receipt destination already exists; choose a new file")
    descriptor, temporary = tempfile.mkstemp(prefix=".spark-receipt-", dir=destination.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:

            def write(run):
                json.dump({"schema_version": 1, "run": run.to_dict()}, stream)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
                # Hard-link publication is atomic and never replaces an existing path.
                os.link(temporary, destination)

            yield write
    finally:
        os.unlink(temporary)


def read_receipt(path: Path) -> RunHandle:
    with path.open("rb") as stream:
        data = stream.read(65537)
    if len(data) > 65536:
        raise ValueError("Run receipt exceeds 64 KiB")
    document = json.loads(data)
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ValueError("Expected a version 1 run receipt")
    run = document.get("run")
    fields = {"namespace", "app_id", "driver_pod", "driver_uid"}
    if not isinstance(run, dict) or any(not isinstance(run.get(key), str) or not run[key] for key in fields):
        raise ValueError("Receipt must contain namespace, app_id, driver_pod and driver_uid")
    return RunHandle(**{key: run[key] for key in fields})
