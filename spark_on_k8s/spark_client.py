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
        namespace: str = "default",
        app_name: str | None = None,
        spark_conf: dict[str, str] | None = None,
    ):
        if not app_name:
            # TODO: generate a unique app name
            pass
        spark_conf_list = [
            f"--conf spark.app.name={app_name}",
            f"--conf spark.kubernetes.namespace={namespace}",
            f"--conf spark.kubernetes.container.image={image}",
        ] + self._spark_config_to_arguments(spark_conf)
        # TODO: add submit class to command and support class parameters
        pod = self._create_spark_pod_spec(
            app_name=app_name,
            image=image,
            namespace=namespace,
            command=spark_conf_list,
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
        return [f"--conf {key}={value}" for key, value in spark_conf.items()]

    @staticmethod
    def _create_spark_pod_spec(
        *,
        app_name: str,
        image: str,
        namespace: str = "default",
        container_name: str = "driver",
        env_variables: dict[str, str] | None = None,
        pod_resources: dict[str, str] | None = None,
        command: list[str] | None = None,
    ):
        # Create a pod template spec for a Spark application
        template = k8s.V1PodTemplateSpec()
        template.metadata = k8s.V1ObjectMeta(name=app_name, namespace=namespace)
        template.spec = k8s.V1PodSpec(containers=[
            SparkOnK8S._create_driver_container(
                image=image,
                container_name=container_name,
                env_variables=env_variables,
                pod_resources=pod_resources,
                command=command,
            )
        ])
        return template

    @staticmethod
    def _create_driver_container(
        *,
        image: str,
        container_name: str = "driver",
        env_variables: dict[str, str] | None = None,
        pod_resources: dict[str, str] | None = None,
        command: list[str] | None = None,
    ) -> k8s.V1Container:
        # Create a container for a Spark application
        container = k8s.V1Container()
        container.name = container_name
        container.image = image
        container.env = [k8s.V1EnvVar(name=key, value=value) for key, value in env_variables.items()]
        container.resources = k8s.V1ResourceRequirements(
            **pod_resources,
        )
        container.command = command or []
        return container
