from __future__ import annotations

from datetime import datetime

from kubernetes import client as k8s

from spark_on_k8s.kubernetes_client import KubernetesClientManager


class SparkOnK8S:
    def __init__(
        self, *, k8s_client_manager: KubernetesClientManager = KubernetesClientManager(),
    ):
        self.k8s_client_manager = k8s_client_manager

    def submit_job(
        self,
        image: str,
        main_class: str,
        main_class_parameters: list[str] | None = None,
        namespace: str = "default",
        service_account: str = "spark",
        app_name: str | None = None,
        spark_conf: dict[str, str] | None = None,
        jar_path: str = "local:///opt/spark/jars/spark-kubernetes_2.12-3.5.0.jar",
    ):
        if not app_name:
            app_name = f"spark-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            app_id = app_name
        else:
            app_id = f"{app_name}-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        driver_args = [
            "driver",
            "--master",
            "k8s://https://kubernetes.default.svc.cluster.local:443",
            "--conf",
            f"spark.app.name={app_name}",
            "--conf",
            f"spark.app.id={app_id}",
            "--conf",
            f"spark.kubernetes.namespace={namespace}",
            "--conf",
            f"spark.kubernetes.authenticate.driver.serviceAccountName={service_account}",
            "--conf",
            f"spark.kubernetes.container.image={image}",
            "--conf",
            f"spark.driver.host={app_id}",
            "--conf",
            "spark.driver.port=7077",
            "--conf",
            f"spark.kubernetes.driver.pod.name={app_id}-driver",
            "--conf",
            f"spark.kubernetes.executor.podNamePrefix={app_id}",
        ] + self._spark_config_to_arguments(spark_conf) + [
            "--class", main_class,
        ] + [
            jar_path,
        ] + (main_class_parameters or [])
        pod = self._create_spark_pod_spec(
            app_name=app_name,
            app_id=app_id,
            image=image,
            namespace=namespace,
            args=driver_args,
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

    @staticmethod
    def _spark_config_to_arguments(spark_conf: dict[str, str] | None) -> list[str]:
        # Convert Spark configuration to a list of arguments
        if not spark_conf:
            return []
        args = []
        for key, value in spark_conf.items():
            args.extend(["--conf", f"{key}={value}"])
        return args

    @staticmethod
    def _create_spark_pod_spec(
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
            labels=SparkOnK8S._job_labels(
                app_name=app_name,
                app_id=app_id,
            ),
        )
        pod_spec = k8s.V1PodSpec(
            service_account_name=service_account,
            restart_policy="Never",
            containers=[
                SparkOnK8S._create_driver_container(
                    image=image,
                    container_name=container_name,
                    env_variables=env_variables,
                    pod_resources=pod_resources,
                    args=args,
                )
            ]
        )
        template = k8s.V1PodTemplateSpec(
            metadata=pod_metadata,
            spec=pod_spec,
        )
        return template

    @staticmethod
    def _create_driver_container(
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
            env=[k8s.V1EnvVar(name=key, value=value) for key, value in (env_variables or {}).items()] + [
                k8s.V1EnvVar(
                    name="SPARK_DRIVER_BIND_ADDRESS",
                    value_from=k8s.V1EnvVarSource(
                        field_ref=k8s.V1ObjectFieldSelector(
                            field_path="status.podIP",
                        )
                    )
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
            ]
        )

    @staticmethod
    def _job_labels(
        *,
        app_name: str,
        app_id: str,
    ) -> dict[str, str]:
        return {
            "spark-app-name": app_name,
            "spark-app-selector": app_id,
            "spark-role": "driver"
        }

    @staticmethod
    def _create_headless_service_object(
        *,
        app_name: str,
        app_id: str,
        namespace: str = "default",
        pod_owner_uid: str | None = None,
    ):
        # Create a service spec for a Spark application
        labels = SparkOnK8S._job_labels(
            app_name=app_name,
            app_id=app_id,
        )
        owner = [
            k8s.V1OwnerReference(
                api_version="v1",
                kind="Pod",
                name=f"{app_id}-driver",
                uid=pod_owner_uid,
            )
        ] if pod_owner_uid else None
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
            )
        )
