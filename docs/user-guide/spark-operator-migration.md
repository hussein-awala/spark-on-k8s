# Migrate from the Spark Operator

Teams running the Kubeflow Spark Operator describe applications as
`SparkApplication` and `ScheduledSparkApplication` custom resources. Since 0.13.0,
`spark-on-k8s job assess-operator` assesses those manifests offline and drafts
equivalent [job files](jobs.md) for the settings that translate.

## What changes: who owns the application

Read this before comparing YAML. The operator is a controller: it watches custom
resources, submits them, restarts or resubmits them according to their restart
policy, runs schedules and deletes finished applications after a time to live.
spark-on-k8s is a client. It creates the driver pod, Service and ConfigMaps, then
returns or observes the run when asked. Nothing reconciles the application
afterwards.

| Concern | Spark Operator | spark-on-k8s |
| --- | --- | --- |
| Submission | Controller runs `spark-submit` in cluster mode | Client creates the driver pod directly; Spark runs in client mode inside it |
| Retries | `restartPolicy` resubmits | The caller retries; each retry is a new submission ([automation](automation.md)) |
| Schedules | `ScheduledSparkApplication` | A [Kubernetes CronJob](recipes.md#schedule-with-a-kubernetes-cronjob) or [Airflow](airflow.md) |
| Spark UI | Operator-created Service or Ingress | `ui_reverse_proxy: true` with the API's [Spark UI proxy](getting_started.md#rest-api), or `kubectl port-forward` |
| Cleanup | `timeToLiveSeconds` | [`apps prune`](automation.md#prune-finished-or-orphaned-drivers) on a schedule you own, or `app delete`; prune ages count from pod creation, not completion; the driver pod owns executors and client-created objects |
| Driver pod shape | Operator webhook or pod template | [Driver pod templates](driver_pod_templates.md) and job-file fields |

Keep the operator for workloads that depend on controller behavior, such as
automatic resubmission after node loss. Both can run in the same cluster while you
migrate; the assessor never reads or changes cluster resources.

## Assess a manifest

```bash
spark-on-k8s job assess-operator spark-pi.yaml
spark-on-k8s job assess-operator manifests.yaml --output json > assessment.json
spark-on-k8s job assess-operator spark-pi.yaml --draft-dir drafts/
```

The input can contain several YAML documents. Each
`sparkoperator.k8s.io/v1beta2` `SparkApplication` or `ScheduledSparkApplication`
is assessed; other documents, including older API versions, are listed as skipped.
Files are limited to 1 MiB, and YAML aliases or duplicate keys are rejected.

Every field in an assessed document receives one status. A finding for an object
or list covers the fields nested below it.

| Status | Meaning |
| --- | --- |
| `mapped` | The draft carries the setting with the same meaning, or the SparkJob default already behaves the same way. `target` names the SparkJob setting. |
| `approximated` | The draft carries it, or its intent is covered differently; `note` states the difference. Review each one. |
| `unsupported` | Not carried into the draft. `note` says why and `alternative` recommends what to use instead. Unrecognized fields are always unsupported: nothing is translated by guesswork. |
| `ignored` | Observed state or server-written metadata (`status`, `metadata.uid`), unmounted volumes and settings that have no effect in the source either. |

An application is `migration_ready` only when it has no unsupported findings and
its draft passes offline validation. A restart policy that resubmits, a schedule,
or an operator-only feature such as `monitoring` therefore always produces
`migration_ready: false`. Readiness is an assessment result, not a guarantee that
the job behaves identically: compare a staging run before switching production.

Each application also lists `notes` for differences that are not tied to one
field, such as the lifecycle change above, a missing namespace (job files default
to `default`), a missing driver service account (job files default to `spark`) or
unset memory overhead (job files default to 512 MiB per pod).

### Output and exit codes

`--output json` writes one document to stdout:

```json
{
  "schema_version": 1,
  "operation": "assess_operator",
  "error": null,
  "migration_ready": false,
  "applications": [
    {
      "document": 1,
      "kind": "SparkApplication",
      "name": "spark-pi",
      "namespace": "spark-jobs",
      "migration_ready": false,
      "counts": {"mapped": 20, "approximated": 6, "unsupported": 1, "ignored": 0},
      "findings": [
        {
          "path": "spec.restartPolicy",
          "status": "unsupported",
          "target": null,
          "note": "spark-on-k8s submits once; no controller resubmits ...",
          "alternative": "Retry from the caller ..."
        }
      ],
      "notes": ["Lifecycle ownership changes: ..."],
      "draft": {"apiVersion": "spark-on-k8s/v1", "kind": "SparkJob", "...": "..."},
      "draft_error": null,
      "draft_file": null
    }
  ],
  "skipped": [{"document": 2, "api_version": "v1", "kind": "ConfigMap", "reason": "..."}]
}
```

Paths use dots for fields, `[N]` for list positions and `[key]` for map keys such
as `spec.sparkConf[spark.executor.memory]`. Consumers should check
`schema_version`, tolerate additional fields and avoid parsing the text report.

| Exit code | Meaning |
| --- | --- |
| `0` | Every document was read and assessed, including when fields are unsupported or `migration_ready` is false |
| `1` | Unreadable or invalid input (invalid YAML, no Spark Operator application, a document without `metadata.name` or `spec`), or a draft file could not be written; the JSON result has an `operation_error` |
| `2` | Invalid CLI arguments, including a missing input file or a `--draft-dir` that is an existing file |

Use `migration_ready` or the `counts` to gate a migration in CI; the exit code
does not.

## Drafts

A draft is a `spark-on-k8s/v1` SparkJob document containing only the mapped and
approximated settings. The command builds it, then validates and plans it in
process exactly like `spark-on-k8s job validate`, without contacting Kubernetes.
Only a draft that passes is emitted; otherwise `draft` is `null` and `draft_error`
explains the validation failure (for example, no image in the source).

`--draft-dir DIRECTORY` (created if missing) writes each valid draft to `DIRECTORY/NAME.yaml` with a
header comment marking it as a draft. Existing files are never replaced, and
nothing is written when any destination already exists. The JSON output always
contains the draft object.

```bash
spark-on-k8s job assess-operator spark-pi.yaml --draft-dir drafts/
spark-on-k8s job validate drafts/spark-pi.yaml
spark-on-k8s job plan drafts/spark-pi.yaml --output json
```

The draft name is the source name, converted to a valid SparkJob name when
needed. Generated application IDs add a timestamp suffix, so driver pods are no
longer named `NAME-driver`. A `ScheduledSparkApplication` produces a draft of its
`spec.template`; schedule it separately.

### Credentials stay out of reports and drafts

Findings never contain source values. A setting that looks like a credential is
reported as `unsupported` and left out of the draft, using the same detection as
plan redaction: sensitive key or variable names (`password`, `token`,
`secret.key`, `access.key` and similar), credentials in URLs, signed URL
parameters and `NAME=secret` tokens in arguments or Java options. Move such
values to Kubernetes Secrets and reference them, for example with
`spark.kubernetes.executor.secretKeyRef.NAME` for executors and a
`valueFrom.secretKeyRef` entry in the driver pod template. Detection is
pattern-based: review drafts before committing them, because positional arguments
or unusual names can still contain sensitive data.

## How fields translate

The report is the authoritative accounting for a given manifest. The main
translations are:

| Spark Operator field | SparkJob setting |
| --- | --- |
| `metadata.name`, `metadata.namespace` | `metadata.name`, `spec.namespace` |
| `metadata.labels` | `driver_labels` and `executor_labels`, except `kueue.x-k8s.io/` labels, which the operator does not copy to pods (unsupported) |
| `image`, `driver.image`, `executor.image` | `image`; a different executor image becomes `spark.kubernetes.executor.container.image` |
| `imagePullPolicy`, `imagePullSecrets` | `image_pull_policy`; pull secrets in the driver template and `spark.kubernetes.container.image.pullSecrets` |
| `mainApplicationFile`, `mainClass`, `arguments` | `app_path`, `class_name`, `app_arguments` |
| `sparkConf`, `hadoopConf` | `spark_conf` (`hadoopConf` keys gain the `spark.hadoop.` prefix) |
| Driver and executor `cores`, `memory`, `memoryOverhead` | `driver_resources` and `executor_resources` (whole cores, MiB) |
| `executor.instances`, `dynamicAllocation` | `executor_instances` |
| `deps.packages`, `deps.jars`, `deps.files`, `deps.pyFiles`, `deps.archives` | `packages`; `spark.jars`, `spark.files`, `spark.submit.pyFiles`, `spark.archives` |
| `serviceAccount` | `service_account` (driver); `spark.kubernetes.authenticate.executor.serviceAccountName` (executors) |
| `nodeSelector`, `labels`, `annotations` | `driver_*` and `executor_*` maps |
| `env`, `envFrom`, `volumeMounts`, `configMaps`, `secrets`, `tolerations`, `affinity`, security contexts, `sidecars`, `initContainers` | `driver_pod_template` and `executor_pod_template`; executor literals and Secret keys use `spark.executorEnv.*` and `spark.kubernetes.executor.secretKeyRef.*`, and executor Secret mounts use `spark.kubernetes.executor.secrets.*` |

Notable approximations and unsupported fields:

- `mode`, `type`, `sparkVersion` and `pythonVersion` have no SparkJob field: the
  image and `app_path` determine them.
- The driver's CPU request and limit both equal `driver_resources.cpu` in whole
  cores, so `driver.coreRequest` and `driver.coreLimit` are approximated. Executor
  values become `spark.kubernetes.executor.request.cores` and `limit.cores`.
- A driver sidecar that keeps running keeps the driver pod, and any waiting,
  running. Prefer a native sidecar; see [driver pod templates](driver_pod_templates.md).
- `gpu` and `hostNetwork` are unsupported: add GPU limits or host networking to
  the pod templates yourself, then verify the running driver and executor pods.
- Dependencies given as local paths are unsupported: the driver resolves them in
  its own pod. Bake them into the image and use `local://`, or use a remote URI.
- `sparkConf` keys that shape a cluster-mode driver pod
  (`spark.kubernetes.driver.secrets.*`, `.volumes.*`, `.podTemplateFile` and
  similar) are unsupported; driver labels, annotations, node selectors, resources
  and the driver service account are translated into SparkJob fields instead.
- `restartPolicy` other than `Never`, schedules, `timeToLiveSeconds`,
  `sparkUIOptions`, `driverIngressOptions`, `monitoring`, `batchScheduler`,
  `sparkConfigMap`, `hadoopConfigMap`, `proxyUser` and operator pod `template`
  fields are unsupported. The [recipes](recipes.md) cover scheduling, batch
  schedulers, lineage and object storage.

## Limits

The assessor is not a controller, importer of live resources or parity checker.
It reads files only, does not read the cluster or the operator's configuration
(for example webhook defaults), and does not compare a draft with pods the
operator created. Volumes, sidecars and other pod fields are copied into
templates without a complete Kubernetes schema check; the API server still
validates them at submission. To trial a migration, submit the validated draft in
a staging namespace next to the existing application and compare its output, pod
specifications from `job plan` and resource use before retiring the custom
resource.
