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
    "driver_pod": "daily-report-20261107120000-driver"
  },
  "status": "Succeeded"
}
```

Without `--wait` or `--logs`, exit code 0 means the application resources were
submitted, and `status` is `null`. It does not mean the workload succeeded.
The receipt identifies the actual generated application ID and driver pod.
It is retained in runtime error output if submission succeeded before waiting
failed, timed out, or was interrupted. An error during resource creation can
leave partial resources; the receipt is not populated until submission finishes.

`--timeout` takes positive, finite seconds and requires a wait or log waiter on
submission. It limits observation after submission, including a running job and
a quiet log stream. It does not bound initial resource creation or an external
Kubernetes credential plugin. The default is unlimited observation. The existing
`SPARK_ON_K8S_STARTUP_TIMEOUT` remains a separate limit for a pending driver.
Neither timeout nor interruption automatically kills or deletes the application.

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
immutable `RunHandle` with `namespace`, `app_id`, and `driver_pod` fields:

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
