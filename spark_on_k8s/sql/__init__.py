from __future__ import annotations

import json
import posixpath
import re
from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING, Any

from spark_on_k8s.utils.types import NOTSET

if TYPE_CHECKING:
    from spark_on_k8s.utils.types import ConfigMap

SQL_MOUNT_PATH = "/configmap"
SQL_APP_PATH = f"{SQL_MOUNT_PATH}/spark_sql.py"
SQL_ARGUMENTS = [f"{SQL_MOUNT_PATH}/queries.json"]


def prepare_sql_configmap(sql: str, *, allow_empty: bool = False, source_name: str = "<inline>") -> ConfigMap:
    """Prepare SQL statements and the standalone runner for a managed ConfigMap.

    Parsing happens on the submitting machine. The Spark image needs only PySpark
    and the standard library, with no Airflow or spark-on-k8s installation.
    """
    if not isinstance(sql, str):
        raise TypeError("SQL must be a string")
    if not isinstance(source_name, str):
        raise TypeError("SQL source name must be a string")
    # Coordinates need a filename, never the submitter's local directory hierarchy.
    source_name = (
        re.sub(r"[\x00-\x1f\x7f]", "_", source_name.replace("\\", "/").rsplit("/", 1)[-1])[:255] or "<inline>"
    )
    try:
        import sqlparse
    except ImportError as error:
        raise ImportError(
            "SQL submission requires the 'sql' extra: pip install 'spark-on-k8s[sql]'"
        ) from error
    statements = [
        statement
        for statement in sqlparse.split(sql, strip_semicolon=True)
        if sqlparse.format(statement, strip_comments=True).strip()
    ]
    if not statements and not allow_empty:
        raise ValueError("SQL must contain at least one statement")
    cursor = 0
    locations = []
    for number, statement in enumerate(statements, 1):
        offset = sql.find(statement, cursor)
        locations.append(
            {"statement": number, "line": sql.count("\n", 0, offset) + 1 if offset >= 0 else None}
        )
        if offset >= 0:
            cursor = offset + len(statement)
    return {
        "mount_path": SQL_MOUNT_PATH,
        "sources": [
            {"name": "spark_sql.py", "text_path": str(Path(__file__).with_name("runner.py"))},
            {"name": "queries.json", "text": json.dumps(statements)},
            {
                "name": "locations.json",
                "text": json.dumps({"schema_version": 1, "source": source_name, "statements": locations}),
            },
        ],
    }


def prepare_sql_options(
    *, sql: str | None = None, sql_file: str | Path | None = None, **submit_options: Any
) -> dict[str, Any]:
    """Return independent submit_app options for SQL, without contacting Kubernetes."""
    if (sql is None) == (sql_file is None):
        raise ValueError("Provide exactly one of sql or sql_file")
    reserved = {"app_path", "app_arguments", "class_name"}.intersection(submit_options)
    if reserved:
        raise ValueError(f"SQL submission manages these options: {', '.join(sorted(reserved))}")
    if sql_file is not None:
        # utf-8-sig drops an editor byte-order mark, which Spark's SQL parser rejects.
        sql = Path(sql_file).expanduser().read_text(encoding="utf-8-sig")
    sql_configmap = prepare_sql_configmap(
        sql, source_name=Path(sql_file).name if sql_file is not None else "<inline>"
    )
    options = {key: NOTSET if value is NOTSET else deepcopy(value) for key, value in submit_options.items()}
    configmaps = options.pop("driver_ephemeral_configmaps_volumes", None)
    configmaps = [] if configmaps is None or configmaps is NOTSET else list(configmaps)
    mounts = options.get("driver_volume_mounts")
    mounts = [] if mounts is None or mounts is NOTSET else mounts

    def conflicts(path: str, *, template: bool = False) -> bool:
        path = posixpath.normpath("/" + path.lstrip("/"))
        return (path == SQL_MOUNT_PATH and not template) or path.startswith(SQL_MOUNT_PATH + "/")

    if any(conflicts(configmap["mount_path"]) for configmap in configmaps) or any(
        conflicts(mount.mount_path) for mount in mounts
    ):
        raise ValueError(f"SQL submission reserves the {SQL_MOUNT_PATH} mount path and its descendants")
    template = options.get("driver_pod_template")
    if template is not None:
        from spark_on_k8s.utils.driver_pod_template import load_driver_pod_template

        # Resolve files once and inspect only the selected driver. Exact template
        # mounts are replaced by the generated mount; nested mounts would shadow it.
        template = load_driver_pod_template(template)
        options["driver_pod_template"] = template
        containers = template["spec"]["containers"]
        selected = options.get("driver_container_name")
        if selected is None:
            selected = (
                "driver"
                if any(item["name"] == "driver" for item in containers)
                else (containers[0]["name"] if len(containers) == 1 else None)
            )
        driver = next((item for item in containers if item["name"] == selected), None)
        if driver and any(
            conflicts(mount["mountPath"], template=True) for mount in driver.get("volumeMounts", [])
        ):
            raise ValueError(f"SQL driver templates cannot mount descendants of {SQL_MOUNT_PATH}")
    return {
        **options,
        "app_path": SQL_APP_PATH,
        "app_arguments": list(SQL_ARGUMENTS),
        "class_name": None,
        "driver_ephemeral_configmaps_volumes": [*configmaps, sql_configmap],
    }
