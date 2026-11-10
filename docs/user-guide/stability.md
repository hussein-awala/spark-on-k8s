# Interface stability and 1.0

This page classifies the public surfaces of the current source and states what each
level promises. The [support boundaries](support.md) define what the project owns
and how a documented contract is retired; this page applies that policy surface by
surface. Use the documentation for your installed release: some surfaces listed here
do not exist in older releases.

## Stability levels

| Level | What you can rely on |
| --- | --- |
| **Stable** | A documented contract you can build automation on. A patch release does not change it incompatibly. An incompatible change in a minor release follows the [deprecation process](support.md#compatibility-and-retirement): a release-note notice, normally a usable compatibility path for at least one published release, a [deprecation register](support.md#deprecation-register) entry and migration steps. A security or correctness fix can break a contract sooner; its release notes state the reason and the operational consequence prominently. |
| **Experimental** | Available for evaluation while its design settles. It can change or be removed in any release. Release notes record the change, without a deprecation period. |
| **Internal** | An implementation detail, even when it is importable, visible in the browser or reachable over HTTP. It can change in any release without notice. |

A surface that is neither documented in a guide nor listed on this page is Internal.
Human-readable output is never a contract: progress and error messages, `--help`
text, log formatting and the console's layout can change in any release. Automation
uses the versioned JSON results and exit codes instead.

Stable behavior includes the lifecycle rules stated across the guides. An uncertain
status is not reported as absence. A submission receipt identifies a run; it does
not establish that the workload or its data writes succeeded. An observation timeout
does not cancel the application; the Airflow operator's startup timeout is the
documented exception that deletes a Pending driver. A shared operator token is not
tenant isolation.

### Versioned documents

Several Stable contracts carry a version marker: `schema_version` in JSON results and
files, `apiVersion: spark-on-k8s/v1` in job files and `/v1` in HTTP paths. Within one
marker, changes are additive, such as a new field in a result. Consumers check the
marker, ignore unknown fields and do not depend on key order or messages. A change
that existing consumers cannot tolerate, such as removing or renaming a field or
changing its type or meaning, uses a new marker and follows the deprecation process.

A release can add optional job-file fields, and existing job files keep working after
an upgrade. Job files reject unknown settings, so a file that uses a new field needs a
release that knows it.

## Surfaces

### Python package and CLI

| Surface | Level | Contract |
| --- | --- | --- |
| Entry points in the [Python reference overview](../reference/index.md), such as `SparkOnK8S`, `RunHandle`, `JobSpec`, `ApplicationPlan`, `Diagnosis` and the Spark Connect profile and server classes | Stable | Documented methods, arguments and results |
| Module pages under **Implementation details**, undocumented helpers and names starting with `_` | Internal | Reference pages exist for contributors |
| Full content of generated Kubernetes resources, including `ApplicationPlan.resources` | Internal | Naming and identity rules documented in the guides are Stable |
| CLI commands and options | Stable | Command guides; usage errors exit with code 2 |
| CLI `--output json` results and exit codes | Stable | `schema_version: 1`; [contract coverage](automation.md#contract-coverage-and-lifecycle-ownership) and [exit codes](automation.md#exit-codes) |
| Submission receipt files | Stable | `schema_version: 1`; [receipts](automation.md#save-identity-before-waiting) |
| Job files | Stable | `apiVersion: spark-on-k8s/v1`, `kind: SparkJob`; [job files](jobs.md) |
| Offline plan documents | Stable envelope | `schema_version: 1`, run identity and `capacity` summary; the embedded resources are Internal |
| Diagnostic and `doctor` reports | Stable | `schema_version: 1` and finding `code` values; [diagnostics](diagnostics.md) |
| Spark Connect profile files | Stable | [Named connection profiles](spark_connect.md#named-connection-profiles) |
| Documented `SPARK_ON_K8S_*` environment variables | Stable | The guide that documents each variable |

### API, console and run history

| Surface | Level | Contract |
| --- | --- | --- |
| Remote submission routes `/jobs/v1/plan` and `/jobs/v1/submit` | Stable | Request document, `schema_version: 1` results, HTTP status and error codes; [HTTP contract](remote-submission.md#http-contract-and-retry-safety) |
| Submission policy files | Stable | `schema_version: 1`; [admission policy](remote-submission.md#admission-policy) |
| Run history routes `/runs/v1` and `/runs/v1/config` | Stable | `schema_version: 1`, filters and cursor; [read-only HTTP API](run-history.md#read-only-http-api) |
| `/health`, `/ready` and the documented `/metrics` names and labels | Stable | [Health and Prometheus](run-history.md#health-and-prometheus) |
| Application routes: `GET` on `/apps/list_apps` and `/apps/list_apps/{namespace}`, and `PATCH` (stop) and `DELETE` on `/app/{namespace}/{app_id}` | Stable | Paths and methods; [REST API](getting_started.md#rest-api) |
| Console diagnostics endpoint | Stable | [Console and Airflow task tab](diagnostics.md#console-and-airflow-task-tab) |
| Console page and Spark UI proxy URLs, such as `/webserver/apps?namespace=...` and `/webserver/ui/{namespace}/{app_id}` | Stable | [REST API](getting_started.md#rest-api) |
| Other `/webserver/api` routes, React components and packaged static assets | Internal | Transport and rendering for the shared console |
| Run history SQLite database | Internal | Read records through `/runs/v1`. The store refuses an unknown future schema version, so an older release cannot open a database upgraded by a newer one |

### Airflow

| Surface | Level | Contract |
| --- | --- | --- |
| `SparkOnK8SOperator` documented arguments and Kubernetes connection behavior | Stable | [Airflow](airflow.md) |
| Native task tab settings and permission model | Stable | [Native task tab](airflow.md#native-task-tab-in-airflow-3) |
| Triggers, operator links, XCom records used for adoption and the task tab, plugin routes and other provider modules | Internal | Documented behavior, not their structure |

### Deployment artifacts and integrations

| Surface | Level | Contract |
| --- | --- | --- |
| Helm chart values in the [Helm values reference](../helm-chart/index.md), validated by `values.schema.json` | Stable | Versioned with the chart: an incompatible change needs a new chart minor version and migration steps in the [chart guide](chart-deployments.md) |
| Rendered chart templates and resource names | Internal | Upgrade notes describe required migrations |
| API image version tags | Stable | An exact version tag identifies a release; `X.Y` and `latest` move as the [release runbook](../releases.md#implementation-and-dry-run) describes. Pin a digest to fix the contents |
| API image contents, base image and file layout | Internal | Rebuilt for each release |
| `dbt-spark-on-k8s` adapter: profile fields, commands and Python modules | Experimental | Separate pre-release distribution; [managed dbt](dbt.md) |
| Examples, starters and test fixtures | Internal | Copy and adapt them; their assertions describe what was tested |

## Versioning

The Python package and its API image share one version. The Helm chart and the dbt
adapter have independent versions; see
[packages and release order](../releases.md#packages-and-release-order).

Before 1.0, a patch release (`0.Y.Z`) contains fixes and does not change a Stable
surface incompatibly, apart from the security and correctness exception. A minor
release (`0.Y.0`) adds features and can make an incompatible change to a Stable
surface only through the deprecation process.

From 1.0, Stable surfaces follow [Semantic Versioning](https://semver.org/): an
incompatible change needs a new major version and a deprecation notice in an earlier
minor release, minor releases add compatible features and deprecations, and patch
releases contain fixes. The security and correctness exception remains, stated
prominently in release notes. Experimental and Internal surfaces keep their levels.

## Criteria for 1.0

The `spark-on-k8s` package and API image move to 1.0.0 when every criterion below
holds. Each names evidence a reviewer can check. The Helm chart and the dbt adapter
decide their own 1.0 separately. 1.0 is a statement about interface stability; it
does not add a support period, a response time or a certification.

1. **Classified surfaces.** Every public surface appears on this page, each Stable
   row links to its canonical guide or reference page, and no Stable surface depends
   on an Experimental one. Experimental surfaces stay outside the 1.0 promise unless
   they first become Stable. *Evidence:* this page and a strict documentation build.
2. **Contract tests.** Each versioned document and each documented exit code has a
   regression test for its marker, required fields and codes, so an incompatible
   change fails CI. *Evidence:* the test paths, listed in the 1.0 release PR.
3. **Two releases without breaks.** Every versioned Stable contract has shipped
   unchanged, apart from additive changes, in at least two consecutive published
   minor releases. *Evidence:* release notes for those releases.
4. **No pending removals.** The [deprecation register](support.md#deprecation-register)
   has no entry awaiting removal, so 1.0 starts without scheduled breaking changes.
   *Evidence:* the register.
5. **Legacy paths decided.** The register or this page records whether each legacy
   path is retained or retired: the Airflow 2.11 compatibility profile, the text-only
   `app kill`, `app delete` and `app logs` commands, and the unversioned application
   routes under `/apps` and `/app`. *Evidence:* the recorded decisions.
6. **Release process exercised.** At least two releases were published through the
   tag-triggered workflow with exact-commit eligibility, a retained release record of
   package hashes, image digests and chart version, and a GitHub release body
   extracted from its release-notes file. *Evidence:* the
   [release records](../releases.md#5-verify-and-record-the-usable-combination).
7. **Qualified deployment.** The 1.0 candidate's API image and matching chart version
   pass a staging smoke test and are recorded as a cluster-qualified combination.
   *Evidence:* the release record.
8. **Named runtime matrix.** The installation and compatibility guides name the
   supported Python, Spark, Airflow and Kubernetes versions for 1.0, and each has a
   CI job that passes on the release commit. *Evidence:* the release eligibility
   record.
9. **Security process.** Private vulnerability reporting is enabled,
   [the security policy](https://github.com/hussein-awala/spark-on-k8s/blob/main/SECURITY.md)
   states which release lines receive fixes, including the previous major line after
   a later major release, and no accepted advisory for the release line is unresolved.
   *Evidence:* the repository's **Security** tab.
