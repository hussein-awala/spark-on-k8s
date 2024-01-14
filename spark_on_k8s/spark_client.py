from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Callable

from kubernetes import client as k8s

from spark_on_k8s.kubernetes_client import KubernetesClientManager


def default_app_id_suffix() -> str:
    return f"-{datetime.now().strftime('%Y%m%d%H%M%S')}"


class SparkOnK8S:
    logger = logging.getLogger(__name__)

    def __init__(
        self,
        *,
        k8s_client_manager: KubernetesClientManager | None = None,
    ):
        self.k8s_client_manager = k8s_client_manager or KubernetesClientManager()

    def submit_job(
        self,
        image: str,
        app_path: str,
        namespace: str = "default",
        service_account: str = "spark",
        app_name: str | None = None,
        spark_conf: dict[str, str] | None = None,
        class_name: str | None = None,
        app_arguments: list[str] | None = None,
        app_id_suffix: Callable[[], str] = default_app_id_suffix,
    ):
        app_name, app_id = self._parse_app_name_and_id(app_name=app_name, app_id_suffix=app_id_suffix)

        spark_conf = spark_conf or {}
        main_class_parameters = app_arguments or []

        basic_conf = {
            "spark.app.name": app_name,
            "spark.app.id": app_id,
            "spark.kubernetes.namespace": namespace,
            "spark.kubernetes.authenticate.driver.serviceAccountName": service_account,
            "spark.kubernetes.container.image": image,
            "spark.driver.host": app_id,
            "spark.driver.port": "7077",
            "spark.kubernetes.driver.pod.name": f"{app_id}-driver",
            "spark.kubernetes.executor.podNamePrefix": app_id,
        }
        driver_command_args = ["driver", "--master", "k8s://https://kubernetes.default.svc.cluster.local:443"]
        if class_name:
            driver_command_args.extend(["--class", class_name])
        driver_command_args.extend(
            self._spark_config_to_arguments({**basic_conf, **spark_conf}) + [app_path, *main_class_parameters]
        )
        pod = self._create_spark_pod_spec(
            app_name=app_name,
            app_id=app_id,
            image=image,
            namespace=namespace,
            args=driver_command_args,
        )
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            pod = api.create_namespaced_pod(
                namespace=namespace,
                body=pod,
            )
            api.create_namespaced_service(
                namespace=namespace,
                body=self._create_headless_service_object(
                    app_name=app_name,
                    app_id=app_id,
                    namespace=namespace,
                    pod_owner_uid=pod.metadata.uid,
                ),
            )

    def _parse_app_name_and_id(
        self, *, app_name: str | None = None, app_id_suffix: Callable[[], str] = default_app_id_suffix
    ) -> tuple[str, str]:
        if not app_name:
            app_name = f"spark-job{app_id_suffix()}"
            app_id = app_name
        else:
            original_app_name = app_name
            # All to lowercase
            app_name = app_name.lower()
            app_id_suffix_str = app_id_suffix()
            if len(app_name) > (63 - len(app_id_suffix_str) + 1):
                app_name = app_name[: (63 - len(app_id_suffix_str)) + 1]
            # Replace all non-alphanumeric characters with dashes
            app_name = re.sub(r"[^0-9a-zA-Z]+", "-", app_name)
            # Remove leading non-alphabetic characters
            app_name = re.sub(r"^[^a-zA-Z]*", "", app_name)
            # Remove leading and trailing dashes
            app_name = re.sub(r"^-*", "", app_name)
            app_name = re.sub(r"-*$", "", app_name)
            app_id = app_name + app_id_suffix_str
            if app_name != original_app_name:
                self.logger.warning(
                    f"Application name {original_app_name} is too long and will be truncated to {app_name}"
                )
        return app_name, app_id

    @staticmethod
    def _spark_config_to_arguments(spark_conf: dict[str, str] | None) -> list[str]:
        # Convert Spark configuration to a list of arguments
        if not spark_conf:
            return []
        args = []
        for key, value in spark_conf.items():
            args.extend(["--conf", f"{key}={value}"])
        return args

    def _create_spark_pod_spec(
        self,
        *,
        app_name: str,
        app_id: str,
        image: str,
        namespace: str = "default",
        service_account: str = "spark",
        container_name: str = "driver",
        env_variables: dict[str, str] | None = None,
        pod_resources: dict[str, str] | None = None,
        args: list[str] | None = None,
    ):
        # Create a pod template spec for a Spark application
        pod_metadata = k8s.V1ObjectMeta(
            name=f"{app_id}-driver",
            namespace=namespace,
            labels=self.job_labels(
                app_name=app_name,
                app_id=app_id,
            ),
        )
        pod_spec = k8s.V1PodSpec(
            service_account_name=service_account,
            restart_policy="Never",
            containers=[
                self._create_driver_container(
                    image=image,
                    container_name=container_name,
                    env_variables=env_variables,
                    pod_resources=pod_resources,
                    args=args,
                )
            ],
        )
        template = k8s.V1PodTemplateSpec(
            metadata=pod_metadata,
            spec=pod_spec,
        )
        return template

    def _create_driver_container(
        self,
        *,
        image: str,
        container_name: str = "driver",
        env_variables: dict[str, str] | None = None,
        pod_resources: dict[str, str] | None = None,
        args: list[str] | None = None,
    ) -> k8s.V1Container:
        # Create a container for a Spark application
        return k8s.V1Container(
            name=container_name,
            image=image,
            env=[k8s.V1EnvVar(name=key, value=value) for key, value in (env_variables or {}).items()]
            + [
                k8s.V1EnvVar(
                    name="SPARK_DRIVER_BIND_ADDRESS",
                    value_from=k8s.V1EnvVarSource(
                        field_ref=k8s.V1ObjectFieldSelector(
                            field_path="status.podIP",
                        )
                    ),
                ),
            ],
            resources=k8s.V1ResourceRequirements(
                **(pod_resources or {}),
            ),
            args=args or [],
            ports=[
                k8s.V1ContainerPort(
                    container_port=7077,
                    name="driver-port",
                ),
                k8s.V1ContainerPort(
                    container_port=4040,
                    name="ui-port",
                ),
            ],
        )

    def job_labels(
        self,
        *,
        app_name: str,
        app_id: str,
    ) -> dict[str, str]:
        return {"spark-app-name": app_name, "spark-app-selector": app_id, "spark-role": "driver"}

    def _create_headless_service_object(
        self,
        *,
        app_name: str,
        app_id: str,
        namespace: str = "default",
        pod_owner_uid: str | None = None,
    ):
        # Create a service spec for a Spark application
        labels = self.job_labels(
            app_name=app_name,
            app_id=app_id,
        )
        owner = (
            [
                k8s.V1OwnerReference(
                    api_version="v1",
                    kind="Pod",
                    name=f"{app_id}-driver",
                    uid=pod_owner_uid,
                )
            ]
            if pod_owner_uid
            else None
        )
        return k8s.V1Service(
            metadata=k8s.V1ObjectMeta(
                name=app_id,
                labels=labels,
                namespace=namespace,
                owner_references=owner,
            ),
            spec=k8s.V1ServiceSpec(
                selector=labels,
                ports=[
                    k8s.V1ServicePort(
                        port=7077,
                        name="driver-port",
                    ),
                    k8s.V1ServicePort(
                        port=4040,
                        name="ui-port",
                    ),
                ],
                type="ClusterIP",
                cluster_ip="None",
            ),
        )
