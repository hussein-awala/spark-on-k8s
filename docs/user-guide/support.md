# Support and compatibility boundaries

Spark-on-k8s submits and observes Spark applications using existing Kubernetes
infrastructure. The Python client, CLI and optional integrations share that core.
Use [Start here](start-here.md) to select the smallest installation for your task.
This page describes source policy; consult the documentation matching your
installed release for its available features and tested versions.

## What the project owns

| Surface | Supported contract | Outside that contract |
| --- | --- | --- |
| Python | Documented client methods, run handles, configuration types and documented helper imports | Every importable private/internal object; thread-safe concurrent use of mutable client convenience properties |
| CLI | Documented commands/options and the versioned JSON/exit contracts identified in [automation](automation.md) | Parsing human progress/error prose or assuming every command returns JSON |
| Job files and plans | Documented schema/version and merge behavior in [job files](jobs.md) | A GitOps reconciler, scheduler or proof of cluster admission |
| Experimental dbt | Separate managed-command adapter for pinned dbt Core/dbt-spark and Spark 4.3 SQL workloads | Stable adapter API, Python models, dbt Fusion/Cloud/v2, automatic cluster/catalog deployment or hard-kill cleanup |
| Airflow | Documented operators, connection behavior, provider discovery and supported task UI integration | A replacement scheduler, identity system or indefinite support for every Airflow generation |
| API and UI | Documented routes/configuration and shared console behavior | User/tenant isolation from a shared operator token; see [remote submission](remote-submission.md) |
| History | Bounded metadata, readiness and recorder metrics as documented in [run history](run-history.md) | Durable Spark logs, audit certification, HA database or submission transaction authority |
| Examples | The explicit assertions and runtime combinations attached to each example | Certification of every cloud, connector, storage backend or application outcome |

[Interface stability](stability.md) classifies each of these surfaces as Stable,
Experimental or Internal and lists the criteria for a 1.0 release.

Kubernetes administrators own cluster access, admission, workload identities,
capacity, storage and network boundaries. Application owners own dependencies,
input/output correctness, replay safety and external side effects. A successful
pod is not a data-quality certification. Spark Connect profiles support existing
endpoints. The optional [server lifecycle helper](spark_connect_server.md) creates
and deletes private single-driver servers; administrators retain responsibility
for network access, authentication, capacity and replacement after failure.

## Compatibility and retirement

The [installation matrix](installation.md) and [Spark runtime matrix](spark_compatibility.md)
name the tested combinations. A dependency range is not evidence that every possible
combination has been exercised. Optional integrations must identify their maintained
workflow, test cost and review owner; accepting one does not promise every feature
on every interface. Airflow 2.11 remains a legacy compatibility path; new deployments
should follow the Airflow 3 guidance. Do not remove the legacy path silently.

For a change to a documented contract, state the previous behavior, replacement,
affected releases and migration steps in the PR and release notes. Normally retain
the old behavior or a usable compatibility path for at least one published release
with a deprecation notice before removal. Security or correctness fixes may need
an earlier break: explain the reason and operational consequence prominently.
Additive JSON fields remain permitted where the existing schema tells consumers
to tolerate them. An internal refactor does not need a deprecation notice unless
it changes a documented import or observable contract.

Before retiring a Python/Airflow/Spark combination, record upstream support status,
actual maintained test coverage, dependency constraints and the proposed replacement.
Announce the decision in release notes; do not infer retirement from a failing CI
job or silently delete the matrix row to make a build pass. There is no guaranteed
support duration, response-time SLA or enterprise certification.

## Deprecation register

No public API removal or compatibility retirement is scheduled by this change.
Airflow 2.11 remains supported by its existing legacy checks; its upstream status
alone is not a removal announcement.

When a retirement is accepted, add a row with the affected contract, replacement,
first notice release, earliest removal release and decision/migration link. Keep
the entry through removal so users can trace the upgrade path. Until then, this
register is intentionally empty rather than assigning speculative deadlines.

## Evaluate and report

For a deployment pilot, record the Python package version, chart version, API image
digest and documentation revision separately. Charts and packages have independent
release channels. A rendered chart is not evidence of a successful deployment;
record what ran in your staging cluster, including one job, diagnostics and the
recovery procedure if you enable persistent history. The [release runbook](../releases.md)
defines how future release records should make these distinctions visible.

Use [Discussions](https://github.com/hussein-awala/spark-on-k8s/discussions) for usage
questions, and [issues](https://github.com/hussein-awala/spark-on-k8s/issues) for
reproducible defects. Include sanitized versions and evidence, not credentials or
private data. Security reports follow the
[private reporting policy](https://github.com/hussein-awala/spark-on-k8s/blob/main/SECURITY.md),
which also states which release lines receive security fixes.
The [deployment security boundary](../security-boundaries.md) describes gateway,
proxy and browser responsibilities.
