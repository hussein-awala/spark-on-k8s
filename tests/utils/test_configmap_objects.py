from __future__ import annotations

import os
import re
import subprocess
import sys

import pytest
from spark_on_k8s.utils.app_manager import SparkAppManager

DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?")


def test_generated_configmap_names_are_distinct_volume_names_for_maximum_length_app_ids():
    # Application IDs may use all 63 label characters; the driver reuses ConfigMap names as
    # Pod volume names, which Kubernetes limits to 63-character DNS labels.
    names = []
    for app_id in ("a" * 62 + "b", "a" * 62 + "c"):
        configmaps = SparkAppManager.create_configmap_objects(
            app_name="a" * 48,
            app_id=app_id,
            configmaps=[{"mount_path": "/first", "sources": []}, {"mount_path": "/second", "sources": []}],
        )
        for configmap in configmaps:
            assert len(configmap.metadata.name) <= 63
            assert DNS_LABEL.fullmatch(configmap.metadata.name)
            assert configmap.metadata.labels["spark-app-id"] == app_id
            names.append(configmap.metadata.name)

    assert len(set(names)) == 4


def test_configmaps_preserve_inline_and_distinct_file_contents(tmp_path):
    first = tmp_path / "settings.conf"
    first.write_text("spark.sql.shuffle.partitions=8\n", encoding="utf-8")
    second = tmp_path / "query.sql"
    second.write_text("SELECT 'café';\nSELECT 2;\n", encoding="utf-8")

    configmaps = SparkAppManager.create_configmap_objects(
        app_name="report",
        app_id="report-123",
        namespace="analytics",
        configmaps=[
            {
                "name": "settings",
                "mount_path": "/etc/settings",
                "sources": [
                    {"name": "empty.txt", "text": ""},
                    {"name": "settings.conf", "text_path": str(first)},
                ],
            },
            {
                "mount_path": "/queries",
                "sources": [
                    {"name": "query.sql", "text_path": str(second)},
                    {"name": "inline.sql", "text": "SELECT 3;\n"},
                ],
            },
        ],
    )

    assert [item.metadata.name for item in configmaps] == ["settings", "report-123-1"]
    assert [item.data for item in configmaps] == [
        {"empty.txt": "", "settings.conf": "spark.sql.shuffle.partitions=8\n"},
        {"query.sql": "SELECT 'café';\nSELECT 2;\n", "inline.sql": "SELECT 3;\n"},
    ]
    for configmap in configmaps:
        assert configmap.metadata.namespace == "analytics"
        assert configmap.metadata.labels == {
            "spark-app-name": "report",
            "spark-app-id": "report-123",
            "spark-role": "driver",
        }


@pytest.mark.parametrize(
    "first,second,message",
    [
        (
            {"name": "settings", "mount_path": "/first"},
            {"name": "settings", "mount_path": "/second"},
            "Configmap name settings is duplicated",
        ),
        (
            {"name": "first", "mount_path": "/settings"},
            {"name": "second", "mount_path": "/settings"},
            "Configmap mount path /settings is duplicated",
        ),
        (
            {"mount_path": "/first"},
            {"name": "report-123-0", "mount_path": "/second"},
            "Configmap name report-123-0 is duplicated",
        ),
    ],
    ids=["duplicate-name", "duplicate-mount-path", "generated-name-collision"],
)
def test_configmaps_reject_conflicting_names_or_mounts(first, second, message):
    with pytest.raises(ValueError, match=message):
        SparkAppManager.create_configmap_objects(
            app_name="report",
            app_id="report-123",
            configmaps=[dict(first, sources=[]), dict(second, sources=[])],
        )


def test_configmap_files_are_utf8_regardless_of_locale(tmp_path):
    # Before Python 3.15 (PEP 686), open() without an encoding follows the locale.
    source = tmp_path / "query.sql"
    source.write_text("SELECT 'café';\n", encoding="utf-8")
    script = (
        "import sys\n"
        "from spark_on_k8s.utils.app_manager import SparkAppManager\n"
        "[configmap] = SparkAppManager.create_configmap_objects(\n"
        "    app_name='report', app_id='report-123',\n"
        "    configmaps=[{'mount_path': '/q', 'sources': [{'name': 'q.sql', 'text_path': sys.argv[1]}]}],\n"
        ")\n"
        "assert configmap.data == {'q.sql': \"SELECT 'caf\\u00e9';\\n\"}, configmap.data\n"
    )
    environment = {**os.environ, "LC_ALL": "C", "PYTHONUTF8": "0", "PYTHONCOERCECLOCALE": "0"}
    result = subprocess.run(
        [sys.executable, "-B", "-P", "-c", script, str(source)],
        env=environment,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_missing_configmap_file_preserves_filename(tmp_path):
    missing = tmp_path / "missing.sql"

    with pytest.raises(FileNotFoundError) as raised:
        SparkAppManager.create_configmap_objects(
            app_name="report",
            app_id="report-123",
            configmaps=[
                {"mount_path": "/queries", "sources": [{"name": "query.sql", "text_path": str(missing)}]}
            ],
        )

    assert raised.value.filename == str(missing)
