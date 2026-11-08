"""Disposable HDFS and external Hive metastore for managed dbt integration.

The catalog survives Spark driver deletion, not fixture/namespace deletion. This
single-process Derby/HDFS configuration is deliberately test-only.
"""

from __future__ import annotations

import json
import subprocess
import time
from uuid import uuid4
from xml.sax.saxutils import escape

from kubernetes import client as k8s

REQUEST = (5, 20)


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, default=str) + "\n")


def xml(values):
    return (
        "<configuration>"
        + "".join(
            f"<property><name>{escape(key)}</name><value>{escape(value)}</value></property>"
            for key, value in values.items()
        )
        + "</configuration>"
    )


class Fixture:
    def __init__(self, connection, args):
        self.api = k8s.CoreV1Api(connection)
        self.rbac = k8s.RbacAuthorizationV1Api(connection)
        self.args = args
        self.namespace = "dbt-managed-" + uuid4().hex[:10]
        self.uid = None
        self.resources = {}
        self.uncertain = False
        self.warehouse = f"hdfs://warehouse.{self.namespace}.svc:8020/warehouse"
        self.metastore = f"thrift://metastore.{self.namespace}.svc:9083"

    def record(self):
        save(
            self.args.artifacts / "fixture.json",
            {
                "namespace": self.namespace,
                "namespace_uid": self.uid,
                "resources": self.resources,
                "uncertain_creation": self.uncertain,
                "warehouse": self.warehouse,
                "metastore": self.metastore,
            },
        )

    def create_resource(self, kind, name, body):
        owner = self.rbac if kind in {"role", "role_binding"} else self.api
        method = getattr(owner, "create_namespaced_" + kind)
        self.uncertain = True
        self.record()
        result = method(self.namespace, body, _request_timeout=REQUEST)
        self.resources[f"{kind}/{name}"] = {"name": name, "uid": result.metadata.uid}
        self.uncertain = False
        self.record()
        return result

    def command(self, *arguments, timeout=180):
        return subprocess.run(
            [
                "kubectl",
                "--kubeconfig",
                str(self.args.kubeconfig),
                "--context",
                self.args.context,
                "--namespace",
                self.namespace,
                *arguments,
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=timeout,
        )

    def create(self):
        if self.uid is not None:
            raise RuntimeError("Fixture namespace already created")
        self.uncertain = True
        self.record()
        created = self.api.create_namespace(
            {"metadata": {"name": self.namespace}},
            _request_timeout=REQUEST,
        )
        self.uid = created.metadata.uid
        self.uncertain = False
        self.record()
        self.create_resource("service_account", "spark", {"metadata": {"name": "spark"}})
        self.create_resource(
            "role",
            "spark",
            {
                "metadata": {"name": "spark"},
                "rules": [
                    {
                        "apiGroups": [""],
                        "resources": ["pods", "services", "configmaps"],
                        "verbs": ["get", "list", "watch", "create", "delete", "patch", "update"],
                    }
                ],
            },
        )
        self.create_resource(
            "role_binding",
            "spark",
            {
                "metadata": {"name": "spark"},
                "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "spark"},
                "subjects": [{"kind": "ServiceAccount", "name": "spark", "namespace": self.namespace}],
            },
        )
        self.create_warehouse()
        self.create_metastore()
        self.assert_catalog_alive()
        return self

    def create_warehouse(self):
        hostname = f"warehouse.{self.namespace}.svc"
        configuration = {
            "core-site.xml": xml(
                {"fs.defaultFS": self.warehouse.rsplit("/", 1)[0], "hadoop.tmp.dir": "/data/tmp"}
            ),
            "hdfs-site.xml": xml(
                {
                    "dfs.replication": "1",
                    "dfs.permissions.enabled": "false",
                    "dfs.namenode.name.dir": "file:///data/name",
                    "dfs.datanode.data.dir": "file:///data/data",
                    "dfs.namenode.rpc-address": "0.0.0.0:8020",
                    "dfs.namenode.http-address": "0.0.0.0:9870",
                    "dfs.datanode.address": "0.0.0.0:9866",
                    "dfs.datanode.ipc.address": "0.0.0.0:9867",
                    "dfs.datanode.http-address": "0.0.0.0:9864",
                    "dfs.datanode.hostname": hostname,
                    "dfs.client.use.datanode.hostname": "true",
                    "dfs.datanode.use.datanode.hostname": "true",
                    "dfs.namenode.datanode.registration.ip-hostname-check": "false",
                }
            ),
        }
        self.create_resource(
            "config_map",
            "warehouse-config",
            {
                "metadata": {"name": "warehouse-config"},
                "data": configuration,
            },
        )
        self.service("warehouse", [8020, 9866, 9867])
        script = """set -euo pipefail
mkdir -p /data/name /data/data /data/tmp /tmp/hadoop-logs
hdfs namenode -format -force -nonInteractive
hdfs namenode & nn=$!
hdfs datanode & dn=$!
trap 'kill "$nn" "$dn" 2>/dev/null || true; wait || true' TERM INT EXIT
wait -n "$nn" "$dn"
exit 1
"""
        self.pod(
            "warehouse",
            self.args.hadoop_image,
            ["/bin/bash", "-c", script],
            config="warehouse-config",
            config_mount="/etc/hadoop-fixture",
            env=[
                {"name": "HADOOP_CONF_DIR", "value": "/etc/hadoop-fixture"},
                {"name": "HADOOP_HEAPSIZE_MAX", "value": "128"},
                {"name": "HADOOP_LOG_DIR", "value": "/tmp/hadoop-logs"},
            ],
        )
        self.command("wait", "--for=jsonpath={.status.phase}=Running", "pod/warehouse", "--timeout=120s")
        ready = (
            "for ((n=0;n<120;n++)); do if hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF; "
            "then hdfs dfs -mkdir -p /warehouse; hdfs dfs -chmod 777 /warehouse; exit 0; fi; "
            "sleep 1; done; exit 1"
        )
        result = self.command("exec", "warehouse", "--", "/bin/bash", "-c", ready)
        (self.args.artifacts / "warehouse-ready.log").write_text(result.stdout + result.stderr)

    def create_metastore(self):
        configuration = {
            "hive-site.xml": xml(
                {
                    "javax.jdo.option.ConnectionURL": (
                        "jdbc:derby:;databaseName=/data/metastore_db;create=true"
                    ),
                    "hive.metastore.schema.verification": "false",
                    "datanucleus.schema.autoCreateAll": "true",
                    "hive.metastore.warehouse.dir": self.warehouse,
                    "hive.metastore.uris": "",
                    "hive.async.log.enabled": "false",
                }
            ),
            "core-site.xml": xml({"fs.defaultFS": self.warehouse.rsplit("/", 1)[0]}),
            "hdfs-site.xml": xml({"dfs.client.use.datanode.hostname": "true"}),
            "log4j2.properties": (
                "status = warn\nrootLogger.level = info\n"
                "rootLogger.appenderRef.console.ref = console\n"
                "appender.console.type = Console\nappender.console.name = console\n"
                "appender.console.layout.type = PatternLayout\n"
                "appender.console.layout.pattern = %d %p %c %m%n\n"
            ),
        }
        self.create_resource(
            "config_map",
            "metastore-config",
            {
                "metadata": {"name": "metastore-config"},
                "data": configuration,
            },
        )
        self.service("metastore", [9083])
        self.pod(
            "metastore",
            self.args.image,
            [
                "java",
                "-Xmx256m",
                "-Dhive.async.log.enabled=false",
                "-Dhive.log4j.file=/etc/hive-fixture/log4j2.properties",
                "-cp",
                "/etc/hive-fixture:/opt/spark/jars/*",
                "org.apache.hadoop.hive.metastore.HiveMetaStore",
                "-p",
                "9083",
            ],
            config="metastore-config",
            config_mount="/etc/hive-fixture",
        )
        self.command("wait", "--for=jsonpath={.status.phase}=Running", "pod/metastore", "--timeout=120s")
        ready = (
            "for ((n=0;n<120;n++)); do if (exec 3<>/dev/tcp/127.0.0.1/9083) 2>/dev/null; "
            "then exit 0; fi; sleep 1; done; exit 1"
        )
        self.command("exec", "metastore", "--", "/bin/bash", "-c", ready)

    def service(self, name, ports):
        self.create_resource(
            "service",
            name,
            {
                "metadata": {"name": name},
                "spec": {
                    "type": "ClusterIP",
                    "selector": {"dbt-fixture": name},
                    "ports": [{"name": f"port-{port}", "port": port, "targetPort": port} for port in ports],
                },
            },
        )

    def pod(self, name, image, command, *, config, config_mount, env=()):
        self.create_resource(
            "pod",
            name,
            {
                "metadata": {"name": name, "labels": {"dbt-fixture": name}},
                "spec": {
                    "restartPolicy": "Never",
                    "securityContext": {"runAsUser": 185, "runAsGroup": 185, "fsGroup": 185},
                    "containers": [
                        {
                            "name": name,
                            "image": image,
                            "imagePullPolicy": "Never",
                            "command": command,
                            "env": list(env),
                            "resources": {
                                "requests": {"cpu": "100m", "memory": "512Mi"},
                                "limits": {"cpu": "1", "memory": "1Gi"},
                            },
                            "volumeMounts": [
                                {"name": "data", "mountPath": "/data"},
                                {"name": "config", "mountPath": config_mount, "readOnly": True},
                            ],
                        }
                    ],
                    "volumes": [
                        {"name": "data", "emptyDir": {}},
                        {"name": "config", "configMap": {"name": config}},
                    ],
                },
            },
        )

    def assert_catalog_alive(self):
        result = {}
        for name in ("warehouse", "metastore"):
            pod = self.api.read_namespaced_pod(name, self.namespace, _request_timeout=REQUEST)
            expected = self.resources[f"pod/{name}"]["uid"]
            if pod.metadata.uid != expected or pod.status.phase != "Running":
                raise RuntimeError(f"Fixture {name} identity or state changed")
            result[name] = expected
        return result

    def compute_objects(self):
        known = {item["uid"] for item in self.resources.values()}
        remaining = []
        for kind in ("pod", "service", "config_map", "secret"):
            for obj in getattr(self.api, "list_namespaced_" + kind)(
                self.namespace, _request_timeout=REQUEST
            ).items:
                if kind == "config_map" and obj.metadata.name == "kube-root-ca.crt":
                    continue
                if obj.metadata.uid not in known:
                    remaining.append({"kind": kind, "name": obj.metadata.name, "uid": obj.metadata.uid})
        return remaining

    def assert_compute_absent(self):
        remaining = self.compute_objects()
        if remaining:
            raise RuntimeError(f"Compute or unexpected resources remain: {remaining}")
        return {"compute_resources_remaining": [], "fixture_pods": self.assert_catalog_alive()}

    def snapshot(self, destination):
        for kind in ("pod", "service", "config_map", "event"):
            try:
                objects = getattr(self.api, "list_namespaced_" + kind)(
                    self.namespace, _request_timeout=REQUEST
                )
                save(destination / (kind + "s.json"), self.api.api_client.sanitize_for_serialization(objects))
                if kind == "pod":
                    for pod in objects.items:
                        try:
                            response = self.api.read_namespaced_pod_log(
                                pod.metadata.name,
                                self.namespace,
                                tail_lines=1000,
                                _request_timeout=REQUEST,
                                _preload_content=False,
                            )
                            try:
                                (destination / (pod.metadata.name + ".log")).write_text(
                                    response.data.decode("utf-8", errors="replace")
                                )
                            finally:
                                response.close()
                                response.release_conn()
                        except Exception:
                            pass
            except Exception as error:
                print(f"Diagnostic {kind} unavailable: {error}", flush=True)

    def cleanup(self):
        if self.uid is None or self.uncertain:
            raise RuntimeError("Fixture creation uncertain; preserve namespace for inspection")
        namespace = self.api.read_namespace(self.namespace, _request_timeout=REQUEST)
        if namespace.metadata.uid != self.uid:
            raise RuntimeError("Fixture namespace replaced; no deletion attempted")
        remaining = self.compute_objects()
        if remaining:
            raise RuntimeError(f"Retaining fixture with unexpected or uncleaned resources: {remaining}")
        self.api.delete_namespace(
            self.namespace,
            body=k8s.V1DeleteOptions(preconditions=k8s.V1Preconditions(uid=self.uid)),
            _request_timeout=REQUEST,
        )
        until = time.monotonic() + 90
        while time.monotonic() < until:
            try:
                current = self.api.read_namespace(self.namespace, _request_timeout=REQUEST)
            except k8s.ApiException as error:
                if error.status == 404:
                    save(
                        self.args.artifacts / "fixture-cleanup.json",
                        {
                            "namespace": self.namespace,
                            "namespace_uid": self.uid,
                            "observed_absent": True,
                        },
                    )
                    return
                raise
            if current.metadata.uid != self.uid:
                raise RuntimeError("Replacement namespace preserved during cleanup")
            time.sleep(0.5)
        raise TimeoutError("Fixture namespace deletion was not observed")
