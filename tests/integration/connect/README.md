# Spark Connect integration checks

`run.py` checks the thin client against a separate local Spark Connect process.
`run_kubernetes.py` checks server provisioning and execution on a disposable kind
cluster. The Kubernetes check never uses a local Spark master.

The Kubernetes harness requires an installed, noneditable `spark-on-k8s[connect]`
wheel, `kubectl`, an explicit kind kubeconfig/context, and a Spark image already
loaded into that cluster. It creates one unique namespace with a test service
account and namespace-scoped executor permissions, then removes that namespace
with a UID precondition. It does not change the user's default context.

The workflow tests the existing digest-pinned Apache Spark 4.0.5 and 4.3.0 images.
Both include `/opt/spark/jars/spark-connect_2.13-VERSION.jar`; no Maven download is
needed for the server itself. Spark's [upstream launcher](https://github.com/apache/spark/blob/v4.3.0/sbin/start-connect-server.sh)
uses `org.apache.spark.sql.connect.service.SparkConnectServer`. The provisioner
runs that entry point through the existing Kubernetes driver submission path.

For an already prepared disposable cluster:

```bash
.venv/bin/python -I tests/integration/connect/run_kubernetes.py \
  --kubeconfig /path/to/disposable-kubeconfig \
  --context kind-spark-connect-ci \
  --image spark-connect-test:4.0.5 \
  --spark-version 4.0.5 \
  --server-jar local:///opt/spark/jars/spark-connect_2.13-4.0.5.jar \
  --artifacts /tmp/connect-kubernetes-results
```

The harness checks:

- Driver and both Services have recorded UIDs, private exposure, and exact driver selection.
- Readiness executes a remote query and matches `spark.app.id` to the recorded server.
- DataFrame and SQL sums run successfully. A SQL expression reads executor hostnames;
  those names must match separately owned executor pods and exclude the driver.
- Two client sessions have separate temporary views and SQL settings. Closing a
  session and failing a SQL query leave the server available.
- The installed CLI creates a second server, writes its versioned receipt, observes
  status, waits for real readiness, and requests deletion. A reachable endpoint
  belonging to that other server cannot satisfy the first server's readiness.
- Stale driver/Service UIDs and a foreign Service recreated under the same name
  cause deletion to fail before other resources are changed.
- An unschedulable driver times out without cancellation. A missing Spark serializer
  class produces a real failed driver. Both remain inspectable until deletion.
- Explicit deletion removes the recorded resources and owned executors; repeated
  deletion is idempotent. Namespace cleanup is separate and cannot hide a failed
  resource garbage-collection assertion.

Only the harness manages loopback `kubectl port-forward` processes. The SDK does
not create tunnels or expose a public listener. Evidence includes JSON receipts,
status/readiness/deletion results, exact Kubernetes metadata, driver/executor logs,
and CLI stdout/stderr. Each CI matrix row uploads these artifacts even on failure.

Use `--case execution`, `--case pending`, or `--case startup-failure` for a focused
rerun after a change. CI uses the default `all` and runs every scenario.
