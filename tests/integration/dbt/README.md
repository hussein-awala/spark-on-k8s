# Managed dbt on disposable Kubernetes

This suite tests the experimental `spark_on_k8s` dbt adapter through its installed
`spark-on-k8s-dbt` entrypoint. It provisions fresh Spark compute for every wrapper
invocation, shared by the dbt commands of that invocation, and tests both
`method: thrift` and `method: connect` on Spark 4.3.0.

The fixture creates a unique namespace, driver service account/RBAC, HDFS, and an
external Hive metastore. The metastore runs the Hive 2.3.10 jars bundled with the
pinned Spark image and keeps its embedded Derby database in its own Pod. Its
catalog and HDFS data survive deletion of every Spark driver, but are intentionally
discarded with the fixture. This is an unauthenticated, single-process test fixture,
not a production storage or metastore deployment.

Install the built noneditable core and `dbt-spark-on-k8s` wheels with their locked
test dependencies in an isolated Python 3.12 environment. Prepare a disposable kind
cluster using the repository's pinned kind/node versions. Load these images:

- Spark: `apache/spark:4.3.0-scala2.13-java21-python3-ubuntu@sha256:3dedc5c8665a6f472e2f0a3a828207a79a30785090ef325d3af219cb90a73d63`.
- HDFS: build this directory's `Hadoop.Dockerfile`. It copies the pinned official
  Hadoop 3.5.0 Java distribution onto the Spark image. The upstream Hadoop source
  image is AMD64-only; no AMD64 executable is run when building on ARM64. Native
  Hadoop libraries are optional for this fixture.

Give the loaded images local aliases and run:

```sh
.venv/bin/python -I tests/integration/dbt/run_kubernetes.py \
  --kubeconfig /path/to/disposable-kubeconfig --context kind-dbt-ci \
  --image spark-dbt-test:4.3.0 --hadoop-image spark-dbt-hadoop:3.5.0 \
  --method both --artifacts /tmp/managed-dbt-results
```

Use a new artifact directory. The runner requires an explicit kind context and
never modifies the default kubeconfig, Docker context, or user Python environment.
The caller needs namespace/RBAC creation rights; the Spark service account only
receives namespaced Pod, Service, and ConfigMap permissions. Every fixture endpoint
is private; the managed wrapper owns its loopback tunnel.

Before its first invocation, each transport runs `spark-on-k8s-dbt check --output json`
against the generated profile. Every check must pass, and the runner then verifies
that no compute or other unexpected resource exists in the fixture namespace.

Each transport uses two dbt threads and checks separate invocations of:

1. Seed, followed by a 13-node build (seed, four models, eight data tests). The
   seed log must contain the server-ready and compute-deleted progress lines.
2. One invocation of two commands: a persisted-table SQL assertion from a newly
   provisioned driver, then `dbt docs generate` (including catalog collection
   after compilation) on the same server.
3. A separate Python model project targeting an existing table, which must fail
   without changing that table or the incremental model's contents.
4. Intentional invalid SQL with `--keep-going`, followed in the same invocation by
   a persisted-table assertion that must succeed on the same server (exit 1), then
   a successful recovery build.
5. SIGINT after Spark's REST API reports the long SQL plan, its active job, and an
   active task on an owned executor. The invocation's second command must not
   start. Another persisted-table assertion follows.

For every invocation, the runner verifies the exit code, completed cleanup receipt,
one observed driver despite concurrent connections and every command of a
sequence, the number of dbt commands that started, a fresh driver UID, and actual
absence of compute resources. A multi-command invocation must report exactly one
server-ready and one compute-deleted progress line and its sequence summary. The
warehouse/metastore Pod UIDs must remain stable.
It also retries the installed recovery CLI after the seed and verifies that it
preserves the fixture. The runner removes inherited `DBT_*` environment overrides
and requires two actual worker thread IDs in each successful 13-node build.
It inspects the generated dbt catalog and verifies table data is stored in HDFS
and materialized by an observed executor. SQL cancellation observation requires
caller permission to exec a bounded read of the driver's local Spark UI API.
No expected failure is accepted merely because a command exited nonzero.

On ownership uncertainty, unexpected resources, or incomplete compute cleanup, the
runner preserves the fixture and reports failure. Otherwise it deletes only its
recorded namespace with a UID precondition and waits for observed absence.
Artifacts include command logs, dbt targets, lifecycle receipts, observed Pod
identities, independent resource checks, fixture snapshots, and cleanup evidence.
