from __future__ import annotations

import logging

from kubernetes import client as k8s

from spark_on_k8s.k8s.sync_client import KubernetesClientManager
from spark_on_k8s.utils.logging_mixin import LoggingMixin


class SparkOnK8SNamespaceSetup(LoggingMixin):
    """Utility class to set up a namespace for Spark on Kubernetes.

    Args:
        k8s_client_manager (KubernetesClientManager, optional): Kubernetes client manager.
            Defaults to None.
        logger_name (str, optional): logger name. Defaults to "SparkOnK8SNamespaceSetup".
    """

    def __init__(
        self,
        *,
        k8s_client_manager: KubernetesClientManager | None = None,
        logger_name: str | None = None,
    ):
        super().__init__(logger_name=logger_name or "SparkOnK8SNamespaceSetup")
        self.k8s_client_manager = k8s_client_manager or KubernetesClientManager()

    def setup_namespace(self, namespace: str, should_print: bool = False):
        """Set up a namespace for Spark on Kubernetes.

        This method creates a namespace if it doesn't exist, creates a service account for Spark
        if it doesn't exist, and creates a cluster role binding for the service account and the
        edit cluster role if it doesn't exist.

        Args:
            namespace (str): the namespace to set up
            should_print (bool, optional): whether to print logs instead of logging them.
                Defaults to False.
        """
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            namespaces = [ns.metadata.name for ns in api.list_namespace().items]
            if namespace not in namespaces:
                self.log(msg=f"Creating namespace {namespace}", level=logging.INFO, should_print=should_print)
                api.create_namespace(
                    body=k8s.V1Namespace(
                        metadata=k8s.V1ObjectMeta(
                            name=namespace,
                        ),
                    ),
                )
            service_accounts = [
                sa.metadata.name for sa in api.list_namespaced_service_account(namespace=namespace).items
            ]
            if "spark" not in service_accounts:
                self.log(
                    msg=f"Creating spark service account in namespace {namespace}",
                    level=logging.INFO,
                    should_print=should_print,
                )
                api.create_namespaced_service_account(
                    namespace=namespace,
                    body=k8s.V1ServiceAccount(
                        metadata=k8s.V1ObjectMeta(
                            name="spark",
                        ),
                    ),
                )
            rbac_api = k8s.RbacAuthorizationV1Api(client)
            cluster_role_bindings = [crb.metadata.name for crb in rbac_api.list_cluster_role_binding().items]
            role_binding_name = f"spark-role-binding-{namespace}"
            if role_binding_name not in cluster_role_bindings:
                self.log(msg="Creating spark role binding", level=logging.INFO, should_print=should_print)
                rbac_api.create_cluster_role_binding(
                    body=k8s.V1ClusterRoleBinding(
                        metadata=k8s.V1ObjectMeta(
                            name=role_binding_name,
                        ),
                        role_ref=k8s.V1RoleRef(
                            api_group="rbac.authorization.k8s.io",
                            kind="ClusterRole",
                            name="edit",
                        ),
                        subjects=[
                            k8s.RbacV1Subject(
                                kind="ServiceAccount",
                                name="spark",
                                namespace=namespace,
                            )
                        ],
                    ),
                )
