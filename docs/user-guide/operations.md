# Operational boundaries

## Know which identity is acting

The local client uses its selected kubeconfig/context; the API submits with its
own Kubernetes identity. Driver and executor service accounts govern workload
access to Kubernetes and external storage. An Airflow connection selects the
submitter identity and is separate from the task UI's service credentials.
A gateway authenticates callers; it does not make a shared API service account
act as each caller. Use separate API deployments and namespaces for separate
trust boundaries. Do not treat namespace filtering as tenant authorization.

The convenience namespace setup helper currently creates a cluster-scoped
binding to Kubernetes' `edit` role. Administrators should provision a
namespace-scoped RoleBinding for production deployments instead. Neither the
chart nor this library is a cloud IAM administrator. Prefer workload identity
and external Secret references to literal credentials in job files.

## Diagnose evidence before changing resources

Record the cluster context, namespace, application ID and driver UID with your
incident. Distinguish application fetch time, diagnostic collection time and
Kubernetes event time. Refresh before acting on an old view. Retained history
can say Running after an application disappears; it is not current health.
Incomplete reads, missing events and empty logs do not prove health or justify
resubmission. Compare current and previous container termination reasons,
scheduler events and driver/executor logs before increasing resources.

Use namespace-scoped commands, substituting only identifiers from the receipt:

```bash
kubectl --context CONTEXT -n NAMESPACE get pod DRIVER -o wide
kubectl --context CONTEXT -n NAMESPACE describe pod DRIVER
kubectl --context CONTEXT -n NAMESPACE logs DRIVER -c driver
kubectl --context CONTEXT -n NAMESPACE get events --field-selector involvedObject.uid=DRIVER_UID
```

Do not replay data writes, delete shared PVCs or repair streaming checkpoints
based only on a pod failure. Keep images, job definitions and external data
recovery procedures independently versioned.

## Back up optional history consistently

The SQLite history store is an operational index, not a job queue or durable
source of submission truth. Restrict database and backup access because labels
and operational metadata may be sensitive. A live database may use WAL files;
copying only the `.sqlite` file while it is being written is not a safe backup.
Use SQLite's online backup API (or stop all writers before a filesystem copy):

```python
import sqlite3
from pathlib import Path

source = Path("/var/lib/spark-on-k8s/history.sqlite").resolve()
backup = Path("/secure-backups/history.sqlite").resolve()
with sqlite3.connect(source.as_uri() + "?mode=ro", uri=True) as original:
    with sqlite3.connect(backup) as target:
        original.backup(target)
        assert target.execute("PRAGMA integrity_check").fetchone() == ("ok",)
```

Choose a new protected destination and test restoration with the exact package
version in a disposable deployment. Confirm records and retention settings,
then verify live Kubernetes state separately. There is no automatic migration,
restore endpoint or claim of multi-writer/high-availability database support.

## Qualify an additional integration

Cloud identity, table formats, accelerators and new orchestrators need a named
owner and a reproducible acceptance test before joining the supported matrix.
Pin image and connector versions. Prove work runs in executors, verify actual
output and a controlled failure, and exercise interruption/restart semantics.
For storage, check namespace, mount ownership and persistence across driver
replacement. For streaming/lakehouse jobs, distinguish pod recovery from
checkpoint correctness, table commits, ordering and replay guarantees.

Prefer a small recipe using the existing SDK and receipts over another control
plane; see the [ecosystem recipes](recipes.md). The
[Spark Operator assessor](spark-operator-migration.md) reports unsupported source
fields; it does not silently translate unknown Spark Operator semantics.
OpenLineage reports and Airflow run correlation are optional evidence, not proof
that every data write committed. Deployment examples do not establish arbitrary
multi-cloud or multi-tenant isolation guarantees.

## Authorization review matrix

| Surface | Authenticator / authorizer | Effective cluster identity | Actor attribution |
| --- | --- | --- | --- |
| Local SDK/CLI | Kubernetes credentials and RBAC | Selected local or in-cluster identity | Kubernetes audit configuration |
| Standalone internal API | External network/gateway controls | API service account | No built-in end-user principal |
| Remote-enabled API and metrics | Shared operator bearer and admission policy | API service account | Shared operator, not a tenant identity |
| Airflow task tab | Airflow session, task permissions, ownership checks | Configured Spark API service identity | Airflow task binding; separate API has its own controls |
| Airflow DAG execution | Airflow Kubernetes connection and cluster RBAC | Worker/triggerer connection identity | Task execution and Kubernetes audit configuration |

Disabling Airflow tab writes only disables browser mutations; it does not prevent
DAG tasks from submitting workloads. Gateway TLS alone does not establish all
these boundaries. See [browser and gateway checks](../security-boundaries.md)
for prefixes, static assets, HTTP, WebSockets and direct-backend isolation.

Recovery restores recorded observations through the backup point, then collection
resumes from currently visible pods. It cannot reconstruct deleted pods or
intermediate statuses during an outage. Readiness indicates current service health,
not historical completeness. Diagnostics classify infrastructure evidence, not
workload efficiency; neither a successful pod nor an empty finding list means a
Spark job is tuned.
