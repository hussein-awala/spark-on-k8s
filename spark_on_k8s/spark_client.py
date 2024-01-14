from __future__ import annotations

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
            # TODO: generate a unique app name
            pass
        driver_args = [
            "driver",
            "--master",
            "k8s://https://kubernetes.default.svc.cluster.local:443",
            "--conf",
            f"spark.app.name={app_name}",
            "--conf",
            f"spark.kubernetes.namespace={namespace}",
            "--conf",
            f"spark.kubernetes.authenticate.driver.serviceAccountName={service_account}",
            "--conf",
            f"spark.kubernetes.container.image={image}",
        ] + self._spark_config_to_arguments(spark_conf) + [
            "--class", main_class,
        ] + [
            jar_path,
        ] + (main_class_parameters or [])
        pod = self._create_spark_pod_spec(
            app_name=app_name,
            image=image,
            namespace=namespace,
            args=driver_args,
        )
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            api.create_namespaced_pod(
                namespace=namespace,
                body=pod,
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
        image: str,
        namespace: str = "default",
        service_account: str = "spark",
        container_name: str = "driver",
        env_variables: dict[str, str] | None = None,
        pod_resources: dict[str, str] | None = None,
        args: list[str] | None = None,
    ):
        # Create a pod template spec for a Spark application
        pod_metadata = k8s.V1ObjectMeta(name=app_name, namespace=namespace)
        pod_spec = k8s.V1PodSpec(
            service_account_name=service_account,
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
            env=[k8s.V1EnvVar(name=key, value=value) for key, value in (env_variables or {}).items()],
            resources=k8s.V1ResourceRequirements(
                **(pod_resources or {}),
            ),
            args=args or [],
        )

