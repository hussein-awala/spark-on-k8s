from __future__ import annotations

import pytest
from spark_on_k8s.utils.app_manager import SparkAppManager


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
