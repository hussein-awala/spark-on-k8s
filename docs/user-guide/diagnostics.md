# Diagnose a Spark application

These commands are available from the current source tree. Check the documentation for your installed release before using them with a published package.

Diagnostics collect a bounded snapshot of driver and executor pod states and Kubernetes events. They explain observed image-pull failures, scheduling constraints, denied operations, OOM kills, crash loops and unavailable persistent volume claims. Each finding contains a stable `code`, `severity`, affected resource, evidence and a suggested next step. An unsuccessful exit code alone is not treated as proof of an OOM kill or another specific cause.

## Inspect an application

Use the application ID returned by submission:

```shell
spark-on-k8s app diagnose --namespace spark --app-id YOUR_APPLICATION_ID
spark-on-k8s app diagnose --namespace spark --app-id YOUR_APPLICATION_ID --output json
```

The SDK accepts either an application ID or an exact driver pod name:

```python
from spark_on_k8s.client import SparkOnK8S

spark = SparkOnK8S()
report = spark.diagnose_app(namespace="spark", app_id="YOUR_APPLICATION_ID")
print(report.to_dict())

# Useful when the driver name was shortened with a hash or when inspecting an existing pod.
report = spark.diagnose_app(namespace="spark", pod_name="YOUR_DRIVER_POD")
```

The JSON report has `schema_version: 1`, `namespace`, `app_id`, `driver_pod`, `findings`, `inspected_pods`, `inspected_events` and `incomplete`. A report with no findings means that no specific problem was observed. It does not establish that the application is healthy. Events and previous container termination states may describe earlier attempts that have since recovered.

Collection reads at most 20 executors and 100 events per inspected pod by default. `--pod-limit` (1–50) and `--event-limit` (1–200) adjust those limits; the SDK accepts the same parameters and a positive `timeout` of at most 30 seconds (10 by default). Collection uses one executor page and one event page per inspected pod, without following pagination. A limit, denied read or timeout produces an incomplete report while retaining available evidence. A failed initial driver lookup produces an error.

Executor selection stays within the chosen namespace, uses Spark's application label, and verifies each executor's owner UID against the driver. Events must match the inspected pod UID, so events for a replaced pod with the same name are excluded. No logs, environment values, Secret contents or PVC contents are fetched. Event messages can still contain workload-specific information; share reports with the same care as Kubernetes events.

## Console and Airflow task tab

Open application **Details** in the standalone console to see the same findings. The JSON endpoint is:

```text
GET /webserver/api/apps/{namespace}/pods/{driver_pod}/diagnostics
```

It returns `{ "application": { ... }, "diagnosis": { ... } }`. The two objects come from the same inspected driver pod. Query parameters `pod_limit` and `event_limit` use the bounds above. The endpoint permits up to five seconds for the initial driver lookup, followed by the bounded collection budget.

The optional [Airflow task tab](airflow.md) uses the shared diagnostics panel. Its backend resolves the task's application and checks the driver ownership annotation against the authorized task before returning findings. It does not accept a browser-supplied namespace or application ID. Diagnostics are unavailable after the driver is deleted; the task's history link remains separate. When an application does not succeed, the Airflow operators also write these findings and the driver's last log lines to the task log before cleanup; see the [failure postmortem](airflow.md#failure-postmortem). When the Spark API enables remote bearer authentication, the Airflow service URL must use a trusted gateway that authenticates the Airflow backend and injects that credential, or a separate restricted internal API. See [remote submission deployment](remote-submission.md#configure-the-server).

The API caller needs namespaced pod reads and `list` access to core `events`. Current source chart namespace Roles include this event permission. For an existing installation using an older chart or a custom service account, an administrator must review and add the permission before event collection can succeed. The page can still show pod evidence when event access is denied. API authentication and task authorization continue to apply; diagnostics do not grant additional access.

## Check cluster prerequisites

```shell
spark-on-k8s doctor --namespace spark --service-account spark
spark-on-k8s doctor --namespace spark --service-account spark --output json
```

`doctor` reads the named service account and submits non-persistent Kubernetes `SelfSubjectAccessReview` requests for the authenticated **caller's** application-management permissions. It does not impersonate the driver or apply resources. Service-account existence and the caller's access do not prove that the driver can create executors: `driver_permissions_verified` remains `false`. A denied or unavailable review is reported explicitly.

The caller checks cover pod creation, lookup, listing and deletion; driver log reads; service and ConfigMap creation; and event listing. They are targeted prerequisites, not an exhaustive validation of every optional workload feature. Cluster network reachability from the driver, image contents, admission policies and storage availability still require runtime evidence.

To prepare a namespace-scoped baseline driver role for administrator review, render manifests offline:

```shell
spark-on-k8s doctor --namespace spark --service-account spark --emit-rbac > driver-rbac.yaml
```

This command prints a Role and RoleBinding and never contacts Kubernetes. It does not create the namespace or service account and does not apply the manifests. The binding references the existing service account. The role allows Spark to manage pods, services and ConfigMaps within that namespace; collection deletion is limited to services and ConfigMaps for Spark cleanup. It grants no cluster-wide permissions, Secret access or pod exec access. Optional features such as dynamically created PVCs may need additional permissions that an administrator should review separately.

See the upstream [Spark Kubernetes service-account guidance](https://spark.apache.org/docs/latest/running-on-kubernetes.html#rbac) and [Kubernetes RBAC guidance](https://kubernetes.io/docs/concepts/security/rbac-good-practices/) when adapting the baseline to your workload.
