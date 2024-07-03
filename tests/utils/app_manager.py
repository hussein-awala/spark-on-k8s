from __future__ import annotations

from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import MagicMock

import pytest
from spark_on_k8s.utils.app_manager import SparkAppManager

if TYPE_CHECKING:
    from spark_on_k8s.utils.types import ConfigMap

APP_NAME = "test-app"
APP_ID = "test-app-id"


@pytest.mark.parametrize(
    "configmaps_list, expected_configmaps",
    [
        (
            [
                {
                    "name": "configmap-volume",
                    "mount_path": "/etc/config",
                    "sources": [
                        {"name": "file1.txt", "text": "config1"},
                        {"name": "file2.txt", "text": "config2"},
                        {"name": "file3.txt", "text_path": "/path/to/file"},
                    ],
                },
                {
                    "name": "configmap-volume2",
                    "mount_path": "/etc/config2",
                    "sources": [
                        {"name": "file4.txt", "text": "config4"},
                        {"name": "file5.txt", "text_path": "/path/to/file2"},
                    ],
                },
            ],
            {
                "configmap-volume": {
                    "file1.txt": "config1",
                    "file2.txt": "config2",
                    "file3.txt": "some fake data",
                },
                "configmap-volume2": {
                    "file4.txt": "config4",
                    "file5.txt": "some fake data",
                },
            },
        ),
        (
            [
                {
                    "name": "configmap-volume",
                    "mount_path": "/etc/config",
                    "sources": [
                        {"name": "file1.txt", "text": "config1"},
                    ],
                },
                {
                    "name": "configmap-volume",
                    "mount_path": "/etc/config2",
                    "sources": [
                        {"name": "file1.txt", "text": "config1"},
                    ],
                },
            ],
            ValueError("Configmap name configmap-volume is duplicated"),
        ),
        (
            [
                {
                    "name": "configmap-volume",
                    "mount_path": "/etc/config",
                    "sources": [
                        {"name": "file1.txt", "text": "config1"},
                    ],
                },
                {
                    "name": "configmap-volume2",
                    "mount_path": "/etc/config",
                    "sources": [
                        {"name": "file1.txt", "text": "config1"},
                    ],
                },
            ],
            ValueError("Configmap mount path /etc/config is duplicated"),
        ),
    ],
)
@mock.patch("builtins.open", mock.mock_open(read_data="some fake data"))
def test_create_configmap_objects(
    configmaps_list: list[ConfigMap],
    expected_configmaps: dict[str, dict[str, str]] | Exception,
):
    spark_app_manager = SparkAppManager(k8s_client_manager=MagicMock())
    if isinstance(expected_configmaps, Exception):
        with pytest.raises(expected_configmaps.__class__, match=str(expected_configmaps)):
            spark_app_manager.create_configmap_objects(
                app_name=APP_NAME,
                app_id=APP_ID,
                configmaps=configmaps_list,
            )
    else:
        configmaps_to_create = spark_app_manager.create_configmap_objects(
            app_name=APP_NAME,
            app_id=APP_ID,
            configmaps=configmaps_list,
        )
        assert len(configmaps_to_create) == len(expected_configmaps)
        for configmap in configmaps_to_create:
            assert configmap.metadata.name in expected_configmaps
            assert configmap.data == expected_configmaps[configmap.metadata.name]
