# Automation and run identities

These features describe the current source. For shell scripts and CI, use
`--output json` instead of parsing human-readable progress messages.

## Submit and observe a run

```bash
spark-on-k8s app submit \
  --namespace spark \
  --image registry.example.com/team/job:version \
  --path local:///opt/spark/work-dir/job.py \
  --name daily-report \
  --wait --timeout 900 --output json > result.json
```

The command writes one JSON document to stdout. Progress and driver logs go to
stderr in JSON mode, so `--logs --output json > result.json 2> driver.log` keeps
both streams separate. `--logs` waits for a terminal application state after the
log stream ends; end-of-stream alone does not prove that a job succeeded.

A successful waiting submission has this shape:

```json
{
  "schema_version": 1,
  "operation": "submit",
  "namespace": "spark",
  "error": null,
  "run": {
    "namespace": "spark",
    "app_id": "daily-report-20261107120000",
    "driver_pod": "daily-report-20261107120000-driver",
    "driver_uid": "5bdceeb0-7fa8-4f72-823b-80f26e6195c4"
  },
  "status": "Succeeded"
}
```

Without `--wait` or `--logs`, exit code 0 means the application resources were
submitted, and `status` is `null`. It does not mean the workload succeeded.
The receipt identifies the actual generated application ID and driver pod.
It is retained in runtime error output if submission succeeded before waiting
failed, timed out, or was interrupted. An error during resource creation can leave partial resources. The client records
a confirmed driver UID immediately after receiving its creation response. Results
include `submission_phase` (`preparing`, `creating_resources`, `driver_created`,
`resources_created`) so a later owner-reference or Service failure can still return
the known driver identity. If creation itself has an ambiguous transport result,
no confirmed receipt is invented. Inspect the cluster before retrying. When the API
server definitively rejects the driver Pod or a ConfigMap (a 4xx response other
than 408), the client deletes only the Secret and ConfigMaps it created earlier in
that submission, using their creation UIDs as preconditions. Server errors, timeouts
and interruptions keep them because the driver may exist.

`--timeout` takes positive, finite seconds and requires a wait or log waiter on
submission. It limits observation after submission, including a running job and
a quiet log stream. It does not bound initial resource creation or an external
Kubernetes credential plugin. The default is unlimited observation. The existing
`SPARK_ON_K8S_STARTUP_TIMEOUT` remains a separate limit for a pending driver.
Neither timeout nor interruption automatically kills or deletes the application.
The deadline is checked before each status or log request and each poll sleep. A
status returned by a request that started within the budget is reported: a final
`Succeeded` or `Failed` read at the deadline exits 0 or 3, not 5. A timeout result
has `status: null`.

To observe a previously submitted application:

```bash
spark-on-k8s app wait --namespace spark --app-id "$APP_ID" \
  --timeout 900 --output json
spark-on-k8s app status --namespace spark --app-id "$APP_ID" --output json
spark-on-k8s apps list --namespace spark --output json
```

A `status` or `wait` document contains `schema_version`, `operation`, `namespace`,
`app_id`, `status`, and `error`. A list document contains the same common fields
plus `items`, an array of `{ "app_id": "...", "namespace": "..." }` objects;
it does not contain `app_id` or `status` at the top level. On a list error,
`items` is `null`. Listing does not make a status request for each application.

Runtime errors use `error: { "code": "...", "message": "..." }` with codes
`api_error`, `operation_error`, `not_found`, `timeout`, or `interrupted`.
JSON error messages exclude Kubernetes response bodies. Receipts do not contain submitted
configuration or secrets. Driver logs remain application-controlled data and may
contain sensitive information. Consumers should check `schema_version`, tolerate
additional fields, and avoid depending on object-key order or human messages.

Some results also contain `warnings`, an array of `{ "code": "...", "message": "..." }`
advisories. `connect check` and `connect server wait` use the `version_skew` code
when the Spark Connect client and server differ in major or minor version (see
[client and server versions](spark_connect.md#client-and-server-versions)). A warning
never changes `error` or the exit code; text output prints it on stderr. Tolerate
unknown warning codes, and test the array explicitly if a script must fail on one.
CLI argument errors are reported by Click on stderr before execution and do not
produce a JSON document. `--help` remains human-readable.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Submission accepted, successful query, or waiting workload succeeded |
| `1` | Operation, configuration, or Kubernetes API failure |
| `2` | Invalid CLI arguments or options |
| `3` | Waiting workload ended in `Failed` |
| `4` | Requested application or Kubernetes resource was not found |
| `5` | Observation, startup, or transport timeout |
| `6` | Waiting workload outcome is unknown; success was not established |
| `130` | User interruption |

Workload outcome codes apply to `app wait` and `app submit --wait` / `--logs`.
`app status` returns 0 when the query succeeds, even if the reported status is
`Failed`. `app logs` remains a standalone streaming command; follow it with
`app wait` when a script needs the workload result.

Shells configured with `set -e` stop on a failed workload. To retain and inspect
failure output, explicitly capture the exit code:

```bash
exit_code=0
spark-on-k8s app wait -n spark --app-id "$APP_ID" --output json \
  > result.json || exit_code=$?
cat result.json
exit "$exit_code"
```

## Python run handles

Existing callers can continue using `submit_app(...)`, which returns the driver
pod name as a string. `submit_run(...)` accepts the same arguments and returns an
immutable `RunHandle` with `namespace`, `app_id`, `driver_pod`, and `driver_uid` fields:

```python
from spark_on_k8s.client import SparkOnK8S

client = SparkOnK8S()
run = client.submit_run(
    namespace="spark",
    image="registry.example.com/team/job:version",
    app_path="local:///opt/spark/work-dir/job.py",
    app_name="daily-report",
    app_waiter="no_wait",
)
print(run.to_dict())
client.app_manager.wait_for_app(
    namespace=run.namespace, pod_name=run.driver_pod, timeout=900,
)
status = client.app_manager.app_status(
    namespace=run.namespace, pod_name=run.driver_pod,
)
```

Python callers explicitly inspect the final status; the SDK does not convert it
into a process exit code. Both submission methods update `last_submitted_run`
and the existing `last_submitted_app_id`. A later attempt clears these properties
before validation, so a failed submission cannot return a previous run's identity.
Keep the returned handle when submitting multiple runs with one client. These
per-client convenience properties are not intended for concurrent submissions.

For SDK submission with `app_waiter="wait"` or `"log"`, the optional
`wait_timeout` sets an observation budget after resource creation. Managers also
accept `timeout` on `wait_for_app` and `stream_logs`; expiration raises
`TimeoutError`. `submit_run` does not support dry runs. A handle contains no
credentials, clients, or automatic reattachment behavior.

## Save identity before waiting

Add `--receipt-file run.json` to `app submit`, `job submit`, or `sql submit`.
The parent directory must exist and the destination must be new. A private file
(mode 0600) is published atomically after submission, before log streaming or
waiting. A timeout, interruption, or observer crash does not remove it. Existing
receipts are never overwritten. Stdout still contains a single final result.

```bash
spark-on-k8s job submit job.yaml --profile production \
  --receipt-file run.json --output json > result.json
```

The file contains `schema_version: 1` and `run` with namespace, application ID,
driver name and Kubernetes UID. Use its namespace and application ID with
`app wait` to resume observation from another process using the same cluster.
Receipts contain no credentials, kubeconfig or cluster endpoint. Select the
correct cluster explicitly; a receipt is an identity, not authorization.

A known driver receipt is also saved when a later resource-creation step fails.
If receipt persistence fails too, the original submission failure remains primary
and `receipt_error` describes the storage failure. A receipt cannot recover an
unknown submission response or make data writes
idempotent. If local publication fails after submission, the command fails and
its final JSON still contains the known run. Investigate that run before retrying.

## Cancel the recorded run

```bash
spark-on-k8s app cancel --receipt run.json --output json
```

Cancellation reads the current pod, verifies its UID, application label and
driver role, then sends a Kubernetes UID precondition with the deletion request.
A stale receipt cannot delete a replacement pod with the same name. Older
receipts without a UID cannot use this command. `accepted: true` means the API
accepted deletion; it does not establish that executors, volumes or finalizers
have finished cleanup. `--force` requests zero grace period and still preserves
the identity checks. No timeout automatically invokes cancellation.
An unreadable or invalid receipt is a runtime failure (exit 1): the JSON result has
`run: null`, an empty `namespace` and an `operation_error`. A receipt path that does
not exist is a CLI usage error (exit 2) without a JSON document.

## Contract coverage and lifecycle ownership

| Command family | Structured success | Structured runtime failures | Completion meaning |
| --- | --- | --- | --- |
| app submit/wait, job submit, sql submit | `--output json` | Versioned result after CLI parsing | Inspect terminal `status`; detached submit means accepted |
| app status, apps list, app cancel | `--output json` | Versioned result | Query/deletion request succeeded, not data correctness |
| job validate | `--output json`, per-profile results | Validation failures in result | Offline validation only |
| job plan | `--output json` | Load/build errors remain Click text | Redacted preview, no reservation |
| app diagnose, doctor | `--output json` report | Click text error, exit 1 (including a missing application) | Observed evidence, not a health certification |
| connect check, connect server | `--output json` | Versioned result | Tiny query succeeded on existing server; server requests accepted; `server list` is a label-based observation |
| remote plan/submit | JSON by default | Versioned result | Request admitted or accepted, not workload completion |
| legacy kill/delete/log commands | Text | Text | Signal/deletion request or log observation |

CLI usage errors still follow Click's text/exit-code contract. Do not assume every
JSON-capable command serializes every failure.

`SparkJob` is loaded by a client; it is not a Kubernetes custom resource or a
reconciled controller. The caller submits and decides whether to observe or retry.
Kubernetes runs the created workload after the client exits. Airflow owns scheduling
and task retry policy when its operator is used. SDK/CLI observation timeouts retain
the application; Airflow startup timeouts deliberately delete it. A kill signal is
a termination request, not evidence of a completed streaming drain or checkpoint.

Disable caching for workflow submission steps. Reusing a cached receipt does not
establish that this workflow executed the data job. Observe the same run before
starting downstream consumers; retrying observation and resubmitting are different
actions. Use one active submission per `SparkOnK8S` instance: the compatibility
`last_submitted_*` attributes are mutable and shared-instance concurrent submission
is not supported. Persist the returned immutable run handle per operation.
