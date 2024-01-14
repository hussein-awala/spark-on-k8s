from __future__ import annotations

import logging

from kubernetes import client as k8s

from spark_on_k8s.kubernetes_client import KubernetesClientManager


class SparkOnK8SNamespaceSetup:
    def __init__(
        self,
        *,
        k8s_client_manager: KubernetesClientManager | None = None,
    ):
        self.k8s_client_manager = k8s_client_manager or KubernetesClientManager()
        self.logger = logging.getLogger(__name__)

    def setup_namespace(self, namespace: str):
        with self.k8s_client_manager.client() as client:
            api = k8s.CoreV1Api(client)
            namespaces = [ns.metadata.name for ns in api.list_namespace().items]
            if namespace not in namespaces:
                self.logger.info(f"Creating namespace {namespace}")
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
                self.logger.info(f"Creating spark service account in namespace {namespace}")
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
            if "spark-role-binding" not in cluster_role_bindings:
                self.logger.info("Creating spark role binding")
                rbac_api.create_cluster_role_binding(
                    body=k8s.V1ClusterRoleBinding(
                        metadata=k8s.V1ObjectMeta(
                            name="spark-role-binding",
                        ),
                        role_ref=k8s.V1RoleRef(
                            api_group="rbac.authorization.k8s.io",
                            kind="ClusterRole",
                            name="edit",
                        ),
                        subjects=[
                            k8s.V1Subject(
                                kind="ServiceAccount",
                                name="spark",
                                namespace=namespace,
                            )
                        ],
                    ),
                )
