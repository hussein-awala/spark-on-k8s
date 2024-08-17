from __future__ import annotations

import datetime
import importlib
import json
import os
from typing import Callable
from unittest import mock

import pytest
from freezegun import freeze_time
from kubernetes import client as k8s
from mock.mock import MagicMock
from spark_on_k8s import client as client_module
from spark_on_k8s.client import ExecutorInstances, PodResources, SparkOnK8S, default_app_id_suffix
from spark_on_k8s.utils import configuration as configuration_module

FAKE_TIME = datetime.datetime(2024, 1, 14, 12, 12, 31)


def empty_suffix() -> str:
    return ""


class TestSparkOnK8s:
    @pytest.mark.parametrize(
        "app_name, app_id_suffix, expected_app_name, expected_app_id",
        [
            pytest.param(
                "spark-app-name",
                empty_suffix,
                "spark-app-name",
                "spark-app-name",
                id="app_name_without_suffix",
            ),
            pytest.param(
                "spark-app-name",
                default_app_id_suffix,
                "spark-app-name",
                "spark-app-name-20240114121231",
                id="app_name_with_suffix",
            ),
            pytest.param(
                "some-very-long-name-which-is-not-allowed-by-k8s-which-is-why-we-need-to-truncate-it",
                empty_suffix,
                "some-very-long-name-which-is-not-allowed-by-k8s-which-is-why-we",
                "some-very-long-name-which-is-not-allowed-by-k8s-which-is-why-we",
                id="app_name_without_suffix_long",
            ),
            pytest.param(
                "some-very-long-name-which-is-not-allowed-by-k8s-which-is-why-we-need-to-truncate-it",
                default_app_id_suffix,
                "some-very-long-name-which-is-not-allowed-by-k8s-w",
                "some-very-long-name-which-is-not-allowed-by-k8s-w-20240114121231",
                id="app_name_with_suffix_long",
            ),
            pytest.param(
                "some-name-ends-with-invalid-character-",
                empty_suffix,
                "some-name-ends-with-invalid-character",
                "some-name-ends-with-invalid-character",
                id="app_name_without_suffix_invalid_character",
            ),
            pytest.param(
                "some-name-ends-with-invalid-character-",
                default_app_id_suffix,
                "some-name-ends-with-invalid-character",
                "some-name-ends-with-invalid-character-20240114121231",
                id="app_name_with_suffix_invalid_character",
            ),
            pytest.param(
                "some.invalid_characters-in/the-name",
                empty_suffix,
                "some-invalid-characters-in-the-name",
                "some-invalid-characters-in-the-name",
                id="app_name_without_suffix_invalid_characters",
            ),
            pytest.param(
                "some.invalid_characters-in/the-name",
                default_app_id_suffix,
                "some-invalid-characters-in-the-name",
                "some-invalid-characters-in-the-name-20240114121231",
                id="app_name_with_suffix_invalid_characters",
            ),
            pytest.param(
                "name.with_8.numerical-characters/12345678",
                empty_suffix,
                "name-with-8-numerical-characters-12345678",
                "name-with-8-numerical-characters-12345678",
                id="app_name_without_suffix_numerical_characters",
            ),
            pytest.param(
                "name.with_8.numerical-characters/12345678",
                default_app_id_suffix,
                "name-with-8-numerical-characters-12345678",
                "name-with-8-numerical-characters-12345678-20240114121231",
                id="app_name_with_suffix_numerical_characters",
            ),
            pytest.param(
                "./-_---name-with-trailing-and-leading-dashes-_-_-,;",
                empty_suffix,
                "name-with-trailing-and-leading-dashes",
                "name-with-trailing-and-leading-dashes",
                id="app_name_without_suffix_trailing_and_leading_dashes",
            ),
            pytest.param(
                "./-_---name-with-trailing-and-leading-dashes-_-_-,;",
                default_app_id_suffix,
                "name-with-trailing-and-leading-dashes",
                "name-with-trailing-and-leading-dashes-20240114121231",
                id="app_name_with_suffix_trailing_and_leading_dashes",
            ),
            pytest.param(
                "12345-name-starts-with-numbers",
                empty_suffix,
                "name-starts-with-numbers",
                "name-starts-with-numbers",
                id="app_name_without_suffix_starts_with_numbers",
            ),
            pytest.param(
                "12345-name-starts-with-numbers",
                default_app_id_suffix,
                "name-starts-with-numbers",
                "name-starts-with-numbers-20240114121231",
                id="app_name_with_suffix_starts_with_numbers",
            ),
            pytest.param(
                None,
                empty_suffix,
                "spark-app",
                "spark-app",
                id="none_app_name_without_suffix",
            ),
            pytest.param(
                None,
                default_app_id_suffix,
                "spark-app-20240114121231",
                "spark-app-20240114121231",
                id="none_app_name_with_suffix",
            ),
            pytest.param(
                "custom-app-id-suffix",
                lambda: "-custom-app-id-suffix",
                "custom-app-id-suffix",
                "custom-app-id-suffix-custom-app-id-suffix",
                id="custom_app_id_suffix",
            ),
            pytest.param(
                "custom-dynamic-app-id-suffix",
                lambda: f"-{int(datetime.datetime.now().timestamp())}",
                "custom-dynamic-app-id-suffix",
                "custom-dynamic-app-id-suffix-1705234351",
                id="custom_dynamic_app_id_suffix",
            ),
        ],
    )
    @freeze_time(FAKE_TIME)
    def test_parse_app_name_and_id(
        self, app_name: str, app_id_suffix: Callable[[], str], expected_app_name: str, expected_app_id: str
    ):
        """
        Test the method _parse_app_name_and_id
        """
        spark_client = SparkOnK8S()
        actual_app_name, actual_app_id = spark_client._parse_app_name_and_id(
            app_name=app_name, app_id_suffix=app_id_suffix
        )
        assert actual_app_name == expected_app_name, "The app name is not as expected"
        assert actual_app_id == expected_app_id, "The app id is not as expected"

    @mock.patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_pod")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_service")
    @freeze_time(FAKE_TIME)
    def test_submit_app(self, mock_create_namespaced_service, mock_create_namespaced_pod, mock_create_client):
        """Test the method submit_app"""

        spark_client = SparkOnK8S()
        spark_client.submit_app(
            image="pyspark-job",
            app_path="local:///opt/spark/work-dir/job.py",
            namespace="spark",
            service_account="spark",
            app_name="pyspark-job-example",
            app_arguments=["100000"],
            app_waiter="no_wait",
            image_pull_policy="Never",
            ui_reverse_proxy=True,
            driver_resources=PodResources(cpu=1, memory=2048, memory_overhead=1024),
            executor_instances=ExecutorInstances(min=2, max=5, initial=5),
        )

        expected_app_name = "pyspark-job-example"
        expected_app_id = f"{expected_app_name}-20240114121231"

        created_pod = mock_create_namespaced_pod.call_args[1]["body"]
        assert created_pod.metadata.name == f"{expected_app_id}-driver"
        assert created_pod.metadata.labels["spark-app-name"] == expected_app_name
        assert created_pod.metadata.labels["spark-app-id"] == expected_app_id
        assert created_pod.metadata.labels["spark-role"] == "driver"
        assert created_pod.spec.containers[0].image == "pyspark-job"
        assert created_pod.spec.service_account_name == "spark"
        assert created_pod.spec.containers[0].args == [
            "driver",
            "--master",
            "k8s://https://kubernetes.default.svc.cluster.local:443",
            "--conf",
            f"spark.app.name={expected_app_name}",
            "--conf",
            f"spark.app.id={expected_app_id}",
            "--conf",
            "spark.kubernetes.namespace=spark",
            "--conf",
            "spark.kubernetes.authenticate.driver.serviceAccountName=spark",
            "--conf",
            "spark.kubernetes.container.image=pyspark-job",
            "--conf",
            f"spark.driver.host={expected_app_id}",
            "--conf",
            "spark.driver.port=7077",
            "--conf",
            f"spark.kubernetes.driver.pod.name={expected_app_id}-driver",
            "--conf",
            f"spark.kubernetes.executor.podNamePrefix={expected_app_id}",
            "--conf",
            "spark.kubernetes.container.image.pullPolicy=Never",
            "--conf",
            "spark.driver.memory=2048m",
            "--conf",
            "spark.executor.cores=1",
            "--conf",
            "spark.executor.memory=1024m",
            "--conf",
            "spark.executor.memoryOverhead=512m",
            "--conf",
            f"spark.ui.proxyBase=/webserver/ui/spark/{expected_app_id}",
            "--conf",
            "spark.ui.proxyRedirectUri=/",
            "--conf",
            "spark.dynamicAllocation.enabled=true",
            "--conf",
            "spark.dynamicAllocation.shuffleTracking.enabled=true",
            "--conf",
            "spark.dynamicAllocation.minExecutors=2",
            "--conf",
            "spark.dynamicAllocation.maxExecutors=5",
            "--conf",
            "spark.dynamicAllocation.initialExecutors=5",
            "local:///opt/spark/work-dir/job.py",
            "100000",
        ]

    @mock.patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client")
    @mock.patch("spark_on_k8s.utils.app_manager.SparkAppManager.stream_logs")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_pod")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_service")
    @freeze_time(FAKE_TIME)
    def test_submit_app_with_env_configurations(
        self,
        mock_create_namespaced_service,
        mock_create_namespaced_pod,
        mock_stream_logs,
        mock_create_client,
    ):
        """Test the method submit_app with env configurations"""
        os.environ["SPARK_ON_K8S_DOCKER_IMAGE"] = "test-spark-on-k8s-docker-image"
        os.environ["SPARK_ON_K8S_APP_PATH"] = "/path/to/app.py"
        os.environ["SPARK_ON_K8S_NAMESPACE"] = "test-namespace"
        os.environ["SPARK_ON_K8S_SERVICE_ACCOUNT"] = "test-service-account"
        os.environ["SPARK_ON_K8S_APP_NAME"] = "test-spark-app"
        os.environ["SPARK_ON_K8S_APP_ARGUMENTS"] = '["arg1","arg2"]'
        os.environ["SPARK_ON_K8S_APP_WAITER"] = "log"
        os.environ["SPARK_ON_K8S_IMAGE_PULL_POLICY"] = "Always"
        os.environ["SPARK_ON_K8S_UI_REVERSE_PROXY"] = "true"
        os.environ["SPARK_ON_K8S_DRIVER_CPU"] = "1"
        os.environ["SPARK_ON_K8S_DRIVER_MEMORY"] = "1024"
        os.environ["SPARK_ON_K8S_DRIVER_MEMORY_OVERHEAD"] = "512"
        os.environ["SPARK_ON_K8S_EXECUTOR_CPU"] = "1"
        os.environ["SPARK_ON_K8S_EXECUTOR_MEMORY"] = "718"
        os.environ["SPARK_ON_K8S_EXECUTOR_MEMORY_OVERHEAD"] = "512"
        os.environ["SPARK_ON_K8S_EXECUTOR_MIN_INSTANCES"] = "2"
        os.environ["SPARK_ON_K8S_EXECUTOR_MAX_INSTANCES"] = "5"
        os.environ["SPARK_ON_K8S_EXECUTOR_INITIAL_INSTANCES"] = "5"
        os.environ["SPARK_ON_K8S_SPARK_CONF"] = json.dumps(
            {"spark.conf1.key": "spark.conf1.value", "spark.conf2.key": "spark.conf2.value"}
        )
        os.environ["SPARK_ON_K8S_SECRET_ENV_VAR"] = json.dumps({"KEY1": "VALUE1", "KEY2": "VALUE2"})
        os.environ["SPARK_ON_K8S_DRIVER_ENV_VARS_FROM_SECRET"] = "SECRET1,SECRET2"

        importlib.reload(configuration_module)
        importlib.reload(client_module)

        spark_client = SparkOnK8S()
        spark_client.submit_app()

        expected_app_name = "test-spark-app"
        expected_app_id = f"{expected_app_name}-20240114121231"

        created_pod = mock_create_namespaced_pod.call_args[1]["body"]
        assert created_pod.spec.containers[0].image == "test-spark-on-k8s-docker-image"
        assert created_pod.spec.service_account_name == "test-service-account"
        assert created_pod.spec.containers[0].args == [
            "driver",
            "--master",
            "k8s://https://kubernetes.default.svc.cluster.local:443",
            "--conf",
            f"spark.app.name={expected_app_name}",
            "--conf",
            f"spark.app.id={expected_app_id}",
            "--conf",
            "spark.kubernetes.namespace=test-namespace",
            "--conf",
            "spark.kubernetes.authenticate.driver.serviceAccountName=test-service-account",
            "--conf",
            "spark.kubernetes.container.image=test-spark-on-k8s-docker-image",
            "--conf",
            f"spark.driver.host={expected_app_id}",
            "--conf",
            "spark.driver.port=7077",
            "--conf",
            f"spark.kubernetes.driver.pod.name={expected_app_id}-driver",
            "--conf",
            f"spark.kubernetes.executor.podNamePrefix={expected_app_id}",
            "--conf",
            "spark.kubernetes.container.image.pullPolicy=Always",
            "--conf",
            "spark.driver.memory=1024m",
            "--conf",
            "spark.executor.cores=1",
            "--conf",
            "spark.executor.memory=718m",
            "--conf",
            "spark.executor.memoryOverhead=512m",
            "--conf",
            f"spark.kubernetes.executor.secretKeyRef.KEY1={expected_app_id}:KEY1",
            "--conf",
            f"spark.kubernetes.executor.secretKeyRef.KEY2={expected_app_id}:KEY2",
            "--conf",
            f"spark.ui.proxyBase=/webserver/ui/test-namespace/{expected_app_id}",
            "--conf",
            "spark.ui.proxyRedirectUri=/",
            "--conf",
            "spark.dynamicAllocation.enabled=true",
            "--conf",
            "spark.dynamicAllocation.shuffleTracking.enabled=true",
            "--conf",
            "spark.dynamicAllocation.minExecutors=2",
            "--conf",
            "spark.dynamicAllocation.maxExecutors=5",
            "--conf",
            "spark.dynamicAllocation.initialExecutors=5",
            "--conf",
            "spark.conf1.key=spark.conf1.value",
            "--conf",
            "spark.conf2.key=spark.conf2.value",
            "/path/to/app.py",
            "arg1",
            "arg2",
        ]
        assert created_pod.spec.containers[0].env_from[0].secret_ref.name == expected_app_id
        assert created_pod.spec.containers[0].env_from[1].secret_ref.name == "SECRET1"
        assert created_pod.spec.containers[0].env_from[2].secret_ref.name == "SECRET2"

    @mock.patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_secret")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_pod")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_service")
    @freeze_time(FAKE_TIME)
    def test_submit_app_with_secrets(
        self,
        mock_create_namespaced_service,
        mock_create_namespaced_pod,
        mock_create_namespaced_secret,
        mock_create_client,
    ):
        spark_client = SparkOnK8S()
        spark_client.submit_app(
            image="pyspark-job",
            app_path="local:///opt/spark/work-dir/job.py",
            namespace="spark",
            service_account="spark",
            app_name="pyspark-job-example",
            app_arguments=["100000"],
            app_waiter="no_wait",
            image_pull_policy="Never",
            ui_reverse_proxy=True,
            driver_resources=PodResources(cpu=1, memory=2048, memory_overhead=1024),
            executor_instances=ExecutorInstances(min=2, max=5, initial=5),
            secret_values={
                "KEY1": "VALUE1",
                "KEY2": "VALUE2",
            },
            driver_env_vars_from_secrets=["secret1", "secret2"],
        )

        expected_app_name = "pyspark-job-example"
        expected_app_id = f"{expected_app_name}-20240114121231"

        created_pod = mock_create_namespaced_pod.call_args[1]["body"]
        created_secret = mock_create_namespaced_secret.call_args[1]["body"]

        assert created_pod.metadata.labels["spark-app-name"] == expected_app_name
        assert created_pod.metadata.labels["spark-app-id"] == expected_app_id

        created_pod = mock_create_namespaced_pod.call_args[1]["body"]
        assert created_pod.spec.containers[0].env_from[0].secret_ref.name == expected_app_id
        assert created_pod.spec.containers[0].env_from[1].secret_ref.name == "secret1"
        assert created_pod.spec.containers[0].env_from[2].secret_ref.name == "secret2"
        # ensure that the provided secrets are added to the pod without trying to create them
        mock_create_namespaced_secret.assert_called_once()
        executors_secrets = [
            {
                "env_var": conf.split("=")[0].split(".")[-1],
                "secret_name": conf.split("=")[1].split(":")[0],
                "key": conf.split("=")[1].split(":")[-1],
            }
            for conf in created_pod.spec.containers[0].args
            if conf.startswith("spark.kubernetes.executor.secretKeyRef.")
        ]
        assert executors_secrets == [
            {
                "env_var": "KEY1",
                "secret_name": expected_app_id,
                "key": "KEY1",
            },
            {
                "env_var": "KEY2",
                "secret_name": expected_app_id,
                "key": "KEY2",
            },
        ]

        assert created_secret.metadata.name == expected_app_id
        assert created_secret.metadata.labels["spark-app-name"] == expected_app_name
        assert created_secret.metadata.labels["spark-app-id"] == expected_app_id
        assert created_secret.string_data["KEY1"] == "VALUE1"
        assert created_secret.string_data["KEY2"] == "VALUE2"

    def test__executor_volumes_config(self):
        spark_client = SparkOnK8S()
        executor_volumes_config = spark_client._executor_volumes_config(
            volumes=[
                k8s.V1Volume(
                    name="volume1",
                    host_path=k8s.V1HostPathVolumeSource(path="/mnt/volume1"),
                ),
                k8s.V1Volume(
                    name="volume2",
                    empty_dir=k8s.V1EmptyDirVolumeSource(medium="Memory", size_limit="1Gi"),
                ),
                k8s.V1Volume(
                    name="volume3",
                    secret=k8s.V1SecretVolumeSource(
                        secret_name="secret1", items=[k8s.V1KeyToPath(key="key1", path="path1")]
                    ),
                ),
                k8s.V1Volume(
                    name="volume4",
                    config_map=k8s.V1ConfigMapVolumeSource(
                        name="configmap1", items=[k8s.V1KeyToPath(key="key1", path="path1")]
                    ),
                ),
                k8s.V1Volume(
                    name="volume5",
                    projected=k8s.V1ProjectedVolumeSource(
                        sources=[
                            k8s.V1VolumeProjection(
                                secret=k8s.V1SecretProjection(
                                    items=[k8s.V1KeyToPath(key="key1", path="path1")]
                                )
                            ),
                            k8s.V1VolumeProjection(
                                config_map=k8s.V1ConfigMapProjection(
                                    items=[k8s.V1KeyToPath(key="key1", path="path1")]
                                )
                            ),
                        ]
                    ),
                ),
                k8s.V1Volume(
                    name="volume6",
                    nfs=k8s.V1NFSVolumeSource(server="nfs-server", path="/mnt/volume6", read_only=True),
                ),
                k8s.V1Volume(
                    name="volume7",
                    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                        claim_name="pvc1", read_only=True
                    ),
                ),
                k8s.V1Volume(
                    name="not-used-volume",
                    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                        claim_name="pvc2", read_only=True
                    ),
                ),
            ],
            volume_mounts=[
                k8s.V1VolumeMount(name="volume1", mount_path="/mnt/volume1"),
                k8s.V1VolumeMount(name="volume2", mount_path="/mnt/volume2", sub_path="sub-path"),
                k8s.V1VolumeMount(name="volume6", mount_path="/mnt/volume3"),
                k8s.V1VolumeMount(name="volume7", mount_path="/mnt/volume4", read_only=True),
            ],
        )
        assert executor_volumes_config == {
            # volumes config
            "spark.kubernetes.executor.volumes.hostPath.volume1.path": "/mnt/volume1",
            "spark.kubernetes.executor.volumes.emptyDir.volume2.medium": "Memory",
            "spark.kubernetes.executor.volumes.emptyDir.volume2.sizeLimit": "1Gi",
            "spark.kubernetes.executor.volumes.nfs.volume6.path": "/mnt/volume6",
            "spark.kubernetes.executor.volumes.nfs.volume6.readOnly": True,
            "spark.kubernetes.executor.volumes.nfs.volume6.server": "nfs-server",
            "spark.kubernetes.executor.volumes.persistentVolumeClaim.volume7.claimName": "pvc1",
            "spark.kubernetes.executor.volumes.persistentVolumeClaim.volume7.readOnly": True,
            # volume mounts config
            "spark.kubernetes.executor.volumes.hostPath.volume1.mount.path": "/mnt/volume1",
            "spark.kubernetes.executor.volumes.emptyDir.volume2.mount.path": "/mnt/volume2",
            "spark.kubernetes.executor.volumes.emptyDir.volume2.mount.subPath": "sub-path",
            "spark.kubernetes.executor.volumes.nfs.volume6.mount.path": "/mnt/volume3",
            "spark.kubernetes.executor.volumes.persistentVolumeClaim.volume7.mount.path": "/mnt/volume4",
            "spark.kubernetes.executor.volumes.persistentVolumeClaim.volume7.mount.readOnly": True,
        }

    @mock.patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_pod")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_service")
    @freeze_time(FAKE_TIME)
    def test_submit_app_with_volumes(
        self,
        mock_create_namespaced_service,
        mock_create_namespaced_pod,
        mock_create_client,
    ):
        volumes = [
            k8s.V1Volume(
                name="volume1",
                host_path=k8s.V1HostPathVolumeSource(path="/mnt/volume1"),
            ),
            k8s.V1Volume(
                name="volume2",
                empty_dir=k8s.V1EmptyDirVolumeSource(medium="Memory", size_limit="1Gi"),
            ),
            k8s.V1Volume(
                name="volume3",
                secret=k8s.V1SecretVolumeSource(
                    secret_name="secret1", items=[k8s.V1KeyToPath(key="key1", path="path1")]
                ),
            ),
            k8s.V1Volume(
                name="volume4",
                config_map=k8s.V1ConfigMapVolumeSource(
                    name="configmap1", items=[k8s.V1KeyToPath(key="key1", path="path1")]
                ),
            ),
            k8s.V1Volume(
                name="volume5",
                projected=k8s.V1ProjectedVolumeSource(
                    sources=[
                        k8s.V1VolumeProjection(
                            secret=k8s.V1SecretProjection(items=[k8s.V1KeyToPath(key="key1", path="path1")])
                        ),
                        k8s.V1VolumeProjection(
                            config_map=k8s.V1ConfigMapProjection(
                                items=[k8s.V1KeyToPath(key="key1", path="path1")]
                            )
                        ),
                    ]
                ),
            ),
            k8s.V1Volume(
                name="volume6",
                nfs=k8s.V1NFSVolumeSource(server="nfs-server", path="/mnt/volume6", read_only=True),
            ),
            k8s.V1Volume(
                name="volume7",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name="pvc1", read_only=True
                ),
            ),
            k8s.V1Volume(
                name="not-used-volume",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name="pvc2", read_only=True
                ),
            ),
        ]
        driver_volume_mounts = [
            k8s.V1VolumeMount(name="volume1", mount_path="/mnt/volume1"),
            k8s.V1VolumeMount(name="volume2", mount_path="/mnt/volume2", sub_path="sub-path"),
            k8s.V1VolumeMount(name="volume3", mount_path="/mnt/volume3"),
            k8s.V1VolumeMount(name="volume4", mount_path="/mnt/volume4"),
            k8s.V1VolumeMount(name="volume5", mount_path="/mnt/volume5"),
            k8s.V1VolumeMount(name="volume6", mount_path="/mnt/volume6"),
            k8s.V1VolumeMount(name="volume7", mount_path="/mnt/volume7", read_only=True),
        ]
        executor_volume_mounts = [
            k8s.V1VolumeMount(name="volume1", mount_path="/mnt/volume1"),
            k8s.V1VolumeMount(name="volume2", mount_path="/mnt/volume2", sub_path="sub-path"),
            k8s.V1VolumeMount(name="volume6", mount_path="/mnt/volume3"),
            k8s.V1VolumeMount(name="volume7", mount_path="/mnt/volume4", read_only=True),
        ]
        spark_client = SparkOnK8S()
        spark_client.submit_app(
            image="pyspark-job",
            app_path="local:///opt/spark/work-dir/job.py",
            namespace="spark",
            service_account="spark",
            app_name="pyspark-job-example",
            app_arguments=["100000"],
            app_waiter="no_wait",
            image_pull_policy="Never",
            ui_reverse_proxy=True,
            driver_resources=PodResources(cpu=1, memory=2048, memory_overhead=1024),
            executor_instances=ExecutorInstances(min=2, max=5, initial=5),
            volumes=volumes,
            driver_volume_mounts=driver_volume_mounts,
            executor_volume_mounts=executor_volume_mounts,
        )

        expected_app_name = "pyspark-job-example"
        expected_app_id = f"{expected_app_name}-20240114121231"

        created_pod = mock_create_namespaced_pod.call_args[1]["body"]

        assert created_pod.metadata.labels["spark-app-name"] == expected_app_name
        assert created_pod.metadata.labels["spark-app-id"] == expected_app_id

        created_pod = mock_create_namespaced_pod.call_args[1]["body"]
        assert created_pod.spec.volumes == volumes
        assert created_pod.spec.containers[0].volume_mounts == driver_volume_mounts

        arguments = created_pod.spec.containers[0].args
        volumes_args = {
            conf.split("=")[0]: conf.split("=")[1]
            for conf in arguments
            if conf.startswith("spark.kubernetes.executor.volumes.")
        }

        assert volumes_args == {
            "spark.kubernetes.executor.volumes.hostPath.volume1.path": "/mnt/volume1",
            "spark.kubernetes.executor.volumes.emptyDir.volume2.medium": "Memory",
            "spark.kubernetes.executor.volumes.emptyDir.volume2.sizeLimit": "1Gi",
            "spark.kubernetes.executor.volumes.nfs.volume6.path": "/mnt/volume6",
            "spark.kubernetes.executor.volumes.nfs.volume6.readOnly": "true",
            "spark.kubernetes.executor.volumes.nfs.volume6.server": "nfs-server",
            "spark.kubernetes.executor.volumes.persistentVolumeClaim.volume7.claimName": "pvc1",
            "spark.kubernetes.executor.volumes.persistentVolumeClaim.volume7.readOnly": "true",
            "spark.kubernetes.executor.volumes.hostPath.volume1.mount.path": "/mnt/volume1",
            "spark.kubernetes.executor.volumes.emptyDir.volume2.mount.path": "/mnt/volume2",
            "spark.kubernetes.executor.volumes.emptyDir.volume2.mount.subPath": "sub-path",
            "spark.kubernetes.executor.volumes.nfs.volume6.mount.path": "/mnt/volume3",
            "spark.kubernetes.executor.volumes.persistentVolumeClaim.volume7.mount.path": "/mnt/volume4",
            "spark.kubernetes.executor.volumes.persistentVolumeClaim.volume7.mount.readOnly": "true",
        }

    @mock.patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_pod")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_service")
    @freeze_time(FAKE_TIME)
    def test_submit_app_with_node_selectors(
        self, mock_create_namespaced_service, mock_create_namespaced_pod, mock_create_client
    ):
        """Test the method submit_app"""

        spark_client = SparkOnK8S()
        spark_client.submit_app(
            image="pyspark-job",
            app_path="local:///opt/spark/work-dir/job.py",
            namespace="spark",
            service_account="spark",
            app_name="pyspark-job-example",
            app_arguments=["100000"],
            app_waiter="no_wait",
            image_pull_policy="Never",
            ui_reverse_proxy=True,
            driver_resources=PodResources(cpu=1, memory=2048, memory_overhead=1024),
            executor_instances=ExecutorInstances(min=2, max=5, initial=5),
            driver_node_selector={"component": "spark", "role": "driver"},
            executor_node_selector={"component": "spark", "role": "executor"},
        )

        created_pod = mock_create_namespaced_pod.call_args[1]["body"]
        assert created_pod.spec.node_selector == {"component": "spark", "role": "driver"}
        arguments = created_pod.spec.containers[0].args
        node_selector_config = {
            conf.split("=")[0]: conf.split("=")[1]
            for conf in arguments
            if conf.startswith("spark.kubernetes.executor.node.selector.")
        }
        assert node_selector_config == {
            "spark.kubernetes.executor.node.selector.component": "spark",
            "spark.kubernetes.executor.node.selector.role": "executor",
        }

    @mock.patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_pod")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_service")
    @freeze_time(FAKE_TIME)
    def test_submit_app_with_labels_and_annotations(
        self, mock_create_namespaced_service, mock_create_namespaced_pod, mock_create_client
    ):
        """Test the method submit_app"""

        spark_client = SparkOnK8S()
        spark_client.submit_app(
            image="pyspark-job",
            app_path="local:///opt/spark/work-dir/job.py",
            namespace="spark",
            service_account="spark",
            app_name="pyspark-job-example",
            app_arguments=["100000"],
            app_waiter="no_wait",
            image_pull_policy="Never",
            ui_reverse_proxy=True,
            driver_resources=PodResources(cpu=1, memory=2048, memory_overhead=1024),
            executor_instances=ExecutorInstances(min=2, max=5, initial=5),
            driver_labels={"label1": "value1", "label2": "value2"},
            executor_labels={"label3": "value3", "label4": "value4"},
            driver_annotations={"annotation1": "value1", "annotation2": "value2"},
            executor_annotations={"annotation3": "value3", "annotation4": "value4"},
        )

        created_pod = mock_create_namespaced_pod.call_args[1]["body"]
        assert created_pod.metadata.labels == {
            # default labels
            "spark-app-name": "pyspark-job-example",
            "spark-app-id": "pyspark-job-example-20240114121231",
            "spark-role": "driver",
            # ui reverse proxy label
            "spark-ui-proxy": "true",
            # custom labels
            "label1": "value1",
            "label2": "value2",
        }
        assert created_pod.metadata.annotations == {
            "annotation1": "value1",
            "annotation2": "value2",
        }
        arguments = created_pod.spec.containers[0].args
        labels_config = {
            conf.split("=")[0]: conf.split("=")[1]
            for conf in arguments
            if conf.startswith("spark.kubernetes.executor.label.")
        }
        annotations_config = {
            conf.split("=")[0]: conf.split("=")[1]
            for conf in arguments
            if conf.startswith("spark.kubernetes.executor.annotation.")
        }
        assert labels_config == {
            "spark.kubernetes.executor.label.label3": "value3",
            "spark.kubernetes.executor.label.label4": "value4",
        }
        assert annotations_config == {
            "spark.kubernetes.executor.annotation.annotation3": "value3",
            "spark.kubernetes.executor.annotation.annotation4": "value4",
        }

    @mock.patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_pod")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_service")
    @freeze_time(FAKE_TIME)
    def test_submit_app_with_tolerations(
        self, mock_create_namespaced_service, mock_create_namespaced_pod, mock_create_client
    ):
        """Test the method submit_app"""

        spark_client = SparkOnK8S()
        spark_client.submit_app(
            image="pyspark-job",
            app_path="local:///opt/spark/work-dir/job.py",
            namespace="spark",
            service_account="spark",
            app_name="pyspark-job-example",
            app_arguments=["100000"],
            app_waiter="no_wait",
            image_pull_policy="Never",
            ui_reverse_proxy=True,
            driver_resources=PodResources(cpu=1, memory=2048, memory_overhead=1024),
            executor_instances=ExecutorInstances(min=2, max=5, initial=5),
            driver_tolerations=[
                k8s.V1Toleration(
                    key="key1",
                    operator="Equal",
                    value="value1",
                    effect="NoSchedule",
                ),
                k8s.V1Toleration(
                    key="key2",
                    operator="Exists",
                    effect="NoExecute",
                ),
            ],
        )

        created_pod = mock_create_namespaced_pod.call_args[1]["body"]
        assert created_pod.spec.tolerations == [
            k8s.V1Toleration(
                key="key1",
                operator="Equal",
                value="value1",
                effect="NoSchedule",
            ),
            k8s.V1Toleration(
                key="key2",
                operator="Exists",
                effect="NoExecute",
            ),
        ]

    @mock.patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_pod")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_service")
    @freeze_time(FAKE_TIME)
    def test_submit_app_with_executor_pod_template_path(
        self, mock_create_namespaced_service, mock_create_namespaced_pod, mock_create_client
    ):
        """Test the method submit_app"""

        spark_client = SparkOnK8S()
        spark_client.submit_app(
            image="pyspark-job",
            app_path="local:///opt/spark/work-dir/job.py",
            namespace="spark",
            service_account="spark",
            app_name="pyspark-job-example",
            app_arguments=["100000"],
            app_waiter="no_wait",
            image_pull_policy="Never",
            ui_reverse_proxy=True,
            driver_resources=PodResources(cpu=1, memory=2048, memory_overhead=1024),
            executor_instances=ExecutorInstances(min=2, max=5, initial=5),
            executor_pod_template_path="s3a://bucket/executor.yml",
        )

        created_pod = mock_create_namespaced_pod.call_args[1]["body"]
        arguments = created_pod.spec.containers[0].args
        executor_config = {
            conf.split("=")[0]: conf.split("=")[1]
            for conf in arguments
            if conf.startswith("spark.kubernetes.executor")
        }
        assert executor_config.get("spark.kubernetes.executor.podTemplateFile") == "s3a://bucket/executor.yml"

    @pytest.mark.parametrize(
        "app_waiter",
        [
            pytest.param("log", id="log"),
            pytest.param("wait", id="wait"),
        ],
    )
    @mock.patch("spark_on_k8s.k8s.sync_client.KubernetesClientManager.create_client")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.read_namespaced_pod")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_pod")
    @mock.patch("kubernetes.client.api.core_v1_api.CoreV1Api.create_namespaced_service")
    def test_submit_app_with_startup_timeout(
        self,
        mock_create_namespaced_service,
        mock_create_namespaced_pod,
        mock_read_namespaced_pod,
        mock_create_client,
        app_waiter,
    ):
        """Test the method submit_app"""
        mock_read_namespaced_pod.return_value = MagicMock(**{"status.phase": "Pending"})
        spark_client = SparkOnK8S()
        with pytest.raises(TimeoutError, match="App startup timeout"):
            spark_client.submit_app(
                image="pyspark-job",
                app_path="local:///opt/spark/work-dir/job.py",
                namespace="spark",
                service_account="spark",
                app_name="pyspark-job-example",
                app_arguments=["100000"],
                app_waiter=app_waiter,
                image_pull_policy="Never",
                ui_reverse_proxy=True,
                driver_resources=PodResources(cpu=1, memory=2048, memory_overhead=1024),
                executor_instances=ExecutorInstances(min=2, max=5, initial=5),
                executor_pod_template_path="s3a://bucket/executor.yml",
                startup_timeout=5,
            )
