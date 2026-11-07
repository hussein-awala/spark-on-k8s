# Run history, readiness and metrics

The optional run recorder keeps a small operational history after Kubernetes driver pods disappear.
It is separate from Spark History Server: it stores metadata, not Spark event logs, stages, executors'
logs or replayable job definitions. The console's **Recorded runs** page lists recorded runs and filters them by
namespace, last observed status and presence. It is read-only and does not offer kill, delete or replay
controls for retained records.

History is disabled by default. When disabled, the application creates no database or background
collector, and `/runs/v1` and `/webserver/runs` return 404.

## Enable persistent metadata

Provision a writable directory on a local or block-backed persistent filesystem, then configure the API:

```bash
export SPARK_ON_K8S_RUN_HISTORY_ENABLED=true
export SPARK_ON_K8S_RUN_HISTORY_PATH=/var/lib/spark-on-k8s/runs.sqlite3
export SPARK_ON_K8S_RUN_HISTORY_NAMESPACES=analytics,batch
spark-on-k8s api start
```

The API service account needs permission to list pods in each configured namespace. Wildcards are not
accepted; at most twenty namespaces can be configured. The recorder only requests pods labeled
`spark-role=driver` and uses the server's existing Kubernetes credentials.

Use one API pod with persistent block storage. Multiple API worker processes in that pod can share
one SQLite file: a database lease elects a collector, and a transaction checks its owner, generation
and expiry before committing. A stale worker cannot overwrite a newer worker's snapshot. Worker
shutdown cancels pending requests and releases the lease. After a crash, another worker can take over
once the lease expires.

SQLite WAL requires reliable local filesystem locking. **Do not use NFS, SMB or a database file copied
independently to multiple replicas.** This feature is intended for a small deployment, not distributed
HA storage. Back up the database with SQLite's backup facility or during a clean shutdown; copying
only the main file while WAL writes are active can omit committed records. Keep the directory private
to the API process and back up the database according to your operational needs.

The store initializes schema version 1 transactionally and refuses an unknown future version.
Configuration and credentials are not stored in the database. A missing/unwritable directory or an
invalid configuration prevents recorder startup; the API does not silently fall back to ephemeral
storage.

## What is recorded

Each run is keyed by its Kubernetes pod UID. Reusing a pod name or Spark application ID creates a
separate record instead of overwriting the old run. Retained fields are:

- UID, namespace, application ID/name and driver pod name;
- driver image, Spark version and Kubernetes creation time;
- first and last observation times, last observed pod phase, and the time the pod was first absent.

Raw pod specifications, annotations, arguments, environment variables, SQL, credentials and logs are
never stored. The collector selects the designated driver container when a pod also has sidecars.

Disappearance is independent of status. A running pod that disappears remains recorded as last seen
`Running`, with a disappearance time; disappearance does **not** imply failure or success. Previously
observed `Succeeded` and `Failed` phases remain terminal. A pod with the same UID that reappears clears
its disappearance marker. The recorder cannot recover pods created and deleted entirely between polls,
statuses it never observed, or runs that predate its first successful scan.

A namespace snapshot commits only after every Kubernetes page succeeds. Authentication failures,
expired/repeated cursors, page limits, malformed metadata, cancellation or timeouts do not mark unseen
pods missing and do not publish partial results. Other namespaces can still be collected while time
remains in the overall budget. The starting namespace rotates between cycles so one slow namespace
cannot continually prevent later namespaces from being collected.

## Bounds and retention

All settings below are optional environment variables with the `SPARK_ON_K8S_RUN_HISTORY_` prefix.

| Suffix | Default | Meaning |
| --- | ---: | --- |
| `INTERVAL` | 30 | Seconds between collection cycles, 5–3600. |
| `TIMEOUT` | 15 | Overall Kubernetes collection budget in seconds, 1–300. |
| `REQUEST_TIMEOUT` | 5 | Timeout per Kubernetes request in seconds, 1–30; the overall budget can interrupt it sooner. |
| `PAGE_SIZE` | 200 | Pods requested per page, 1–500. |
| `MAX_PAGES` | 20 | Maximum pages per namespace, 1–100. |
| `RETENTION_DAYS` | 30 | Delete records not observed for this many days, 1–3650. |
| `MAX_ROWS` | 10000 | Maximum retained records across namespaces, 1–1000000. |

SQLite lock waits are bounded to two seconds per transaction. The lease includes the collection budget,
poll interval and a safety margin. No new namespace scan starts after the collection budget expires.
Retention is enforced after both successful collections and recorded failures. The oldest records by
last observation are removed first when the row limit is exceeded; a small limit can discard useful
history, including records for still-present pods. SQLite reuses freed pages, so deleting rows need not
shrink the database file immediately.

Restart API workers to change these settings. Increasing page limits may increase memory use and cluster
API traffic; use namespaces and quotas that match the deployment's scale.

## Read-only HTTP API

`GET /runs/v1/config` returns enabled namespaces and retention settings. `GET /runs/v1` returns
`{schema_version: 1, items: [...], next_cursor: string | null}`. Each item contains:

```json
{
  "uid": "6fb31548-331f-4c94-bb51-b0af6b8cd263",
  "namespace": "analytics",
  "app_id": "daily-report",
  "name": "Daily report",
  "driver_pod": "daily-report-driver",
  "image": "registry.example.com/jobs/report:approved",
  "spark_version": "4.0.5",
  "status": "Succeeded",
  "created_at": "2026-11-07T12:00:00Z",
  "first_seen": "2026-11-07T12:00:15Z",
  "last_seen": "2026-11-07T12:04:15Z",
  "disappeared_at": "2026-11-07T12:04:45Z"
}
```

Creation time, image, Spark version and application ID can be null. Disappearance time is null for a
present pod. Filters are `namespace`, `status` (`Pending`, `Running`, `Succeeded`, `Failed`, `Unknown`),
`presence` (`all`, `present`, `missing`) and `limit` (1–200, default 50). Pass `next_cursor` back as `cursor`
with the same filters. Ordering uses immutable first-observed time and UID, so updating an existing run
does not move it between pages. Retention may remove rows between requests. Only currently configured
namespaces are exposed, even if the database contains older records from another namespace.

In [remote mode](remote-submission.md), history and metrics use the same operator bearer authorization
as other API routes. The recorder does not introduce tenant-specific permissions.

## Health and Prometheus

`/health` remains a lightweight liveness probe. `GET` and `HEAD /ready` check actual pod-list access in
the configured namespaces, with a five-second overall deadline. Without history, readiness checks the
API's default namespace. With history, the recorder must also be initialized and have a recent
successful snapshot for every configured namespace. The freshness window is twice the poll interval
plus the collection timeout and ten seconds. Until the first successful scan, readiness is 503.

Both probes remain accessible without a bearer in remote mode. Readiness returns only
`{"status":"ready"}` or `{"status":"not_ready"}` and does not expose namespace, database or credential
errors. Failed readiness removes the pod from service traffic; liveness does not restart it merely
because Kubernetes is temporarily unavailable.

Set `SPARK_ON_K8S_METRICS_ENABLED=true` to expose `/metrics` in Prometheus text format. It remains protected
by the remote operator bearer when remote mode is enabled. Configure the scraper's authorization
through its secret mechanism; do not place a token in a scrape URL. Metrics include:

- `spark_on_k8s_history_enabled`;
- `spark_on_k8s_runs_recorded{namespace,status}`;
- `spark_on_k8s_collection_successes_total{namespace}` and `spark_on_k8s_collection_failures_total{namespace}`;
- `spark_on_k8s_collection_age_seconds{namespace}` (`-1` before any successful collection).

Labels contain only configured namespaces and five bounded status values. No application IDs, pod
names, images or exception text appear in metric labels. Counters persist with the SQLite database.

## Helm configuration

The chart uses `/health` and `/ready` for liveness and readiness. To enable history, provision an existing
ReadWriteOnce block-backed claim and allow the API's filesystem user to write it:

```yaml
replicaCount: 1
autoscaling:
  enabled: false
namespaces: analytics,batch
runHistory:
  enabled: true
  namespaces: [analytics, batch]
  existingClaim: spark-run-metadata
  intervalSeconds: 30
  retentionDays: 30
  maxRows: 10000
metrics:
  enabled: true
```

The database is mounted at `/var/lib/spark-on-k8s/runs.sqlite3`. The chart requires a claim and explicit
namespaces, rejects multiple replicas/autoscaling, and uses a `Recreate` rollout so two API pods do not
compete for the claim. Existing environment overrides remain supported. Make sure history namespaces
also have the pod-list permissions configured by `namespaces` or an externally managed service account.
The separately configured `sparkHistory` chart section still refers to Spark History Server event logs.
