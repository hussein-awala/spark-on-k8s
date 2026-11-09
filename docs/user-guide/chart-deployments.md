# Chart deployment boundaries

These controls describe the current source chart. Published charts, API images
and Python packages have independent release histories; inspect the selected
chart and image before upgrading. The chart is an API/console deployment with an
optional Spark History Server. It does not deploy a Spark scheduler, create a
cloud workload identity, or configure applications submitted through the SDK.

## Select the image explicitly

The API image uses `image.digest` when set. Otherwise it uses `image.tag`; an
explicitly empty tag opts into that chart's `appVersion` fallback. A version tag
is clearer than `latest`, but only a verified digest fixes the image contents.

The source chart requires an explicit compatible image tag or digest while its
`appVersion` is `unreleased`. Release packaging sets a qualified application
version, which becomes the default image tag. Building or rendering the local
chart does not build an API image.

The optional History Server has a separate image lifecycle. Enabling
`sparkHistory.enabled` requires an explicit `sparkHistory.image.tag` or
`sparkHistory.image.digest`; there is no implicit API-version or `latest`
fallback. For both images, a digest takes precedence over a simultaneously set
tag. Use `repository` without an embedded tag or digest, and put the complete
`sha256:` plus 64 hexadecimal characters in `digest`.

No History Server image version is qualified by these chart-rendering checks.
Validate your selected History image, event-log storage and Spark runtime together
before deployment. An explicitly selected `latest` tag remains accepted for
intentional development use; it does not make the deployment reproducible.

## Start with scoped, externally managed access

The source includes `chart/examples/production-values.yaml`. It uses explicit
`analytics,batch` namespaces, the preprovisioned `spark-observer` service account,
external RBAC, a ClusterIP service, and illustrative resource reservations. Its
`REQUIRED_SHA256_DIGEST` placeholder deliberately fails validation. Set a digest
you have verified for the selected API image repository:

```bash
# Set API_IMAGE_DIGEST to the verified sha256:... digest; no sample image is implied.
: "${API_IMAGE_DIGEST:?Set the verified API image digest}"
helm lint ./chart -f chart/examples/production-values.yaml \
  --set-string image.digest="$API_IMAGE_DIGEST"
helm template spark-console ./chart --namespace spark-system \
  -f chart/examples/production-values.yaml \
  --set-string image.digest="$API_IMAGE_DIGEST" > rendered-console.yaml
```

Provision the release namespace and named service account before installation.
The administrator must grant that account the needed access in the selected
application namespaces. Review the rendered resources and adjust the example's
namespaces, resource requests and limits for your deployment. The example does
not establish availability, throughput or cloud-identity compatibility.

The default `readOnly: true` remains unchanged. With chart-managed RBAC and an
explicit namespace string, the chart creates namespace Roles and RoleBindings.
`namespaces: "*"` intentionally selects a cluster-wide binding instead. Neither
namespace selection nor hiding UI buttons provides user or tenant authorization;
use the [remote-submission security boundary](remote-submission.md).

## Decide who owns the account and permissions

`rbac.create` controls API roles and bindings independently of
`serviceAccount.create`. Its default, `null`, follows `serviceAccount.create` to
preserve existing rendering. The History Server has its own service-account
setting; API RBAC settings do not grant History Server storage permissions.

| API configuration | Account created by Helm | Roles/bindings created by Helm |
| --- | --- | --- |
| `serviceAccount.create: true`, `rbac.create: null` | Yes | Yes; legacy default |
| `serviceAccount.create: false`, `rbac.create: null` | No | No; legacy external-account behavior |
| `serviceAccount.create: false`, explicit `serviceAccount.name`, `rbac.create: true` | No | Yes, bound to that account in the release namespace |
| `serviceAccount.create: true`, `rbac.create: false` | Yes | No |
| `serviceAccount.create: false`, `rbac.create: false` | No | No; fully external ownership |

The chart requires an explicit account name when creating RBAC for an external
account. `readOnly` only changes chart-managed permissions; it cannot reduce
permissions already granted externally. The namespace edit Role supports existing
management actions, but it is not a complete grant for every optional submission
resource. Review the actual templates and grant any required Secret/ConfigMap
operations deliberately for a remote-submission deployment.

When changing ownership on an installed release, inspect the Helm diff first.
Setting a create flag to `false` removes previously managed resources from the
release; Helm can delete them during upgrade. Arrange the replacement permissions
and deliberate resource-ownership transfer before that upgrade. Do not assume
that changing the flag alone preserves the existing account or grants.

## Keep the four workload identities separate

| Role | Identity and metadata inputs | Image and image-pull inputs |
| --- | --- | --- |
| API/console | Chart `serviceAccount.*`, `podLabels`, `podAnnotations`; its Kubernetes credentials observe/manage applications | Chart `image.*`, `imagePullSecrets` |
| Spark driver | Submission `namespace`, `service_account`, `driver_labels`, `driver_annotations` and driver template | Submission `image`, `image_pull_policy`; driver template for supported pull-secret customization |
| Spark executors | Application executor template and Spark configuration for the chosen runtime; `executor_labels` and `executor_annotations` | Application image/configuration and executor template; API chart pull secrets are not inherited |
| History Server | Chart `sparkHistory.serviceAccount.*`, `sparkHistory.podLabels`, `sparkHistory.podAnnotations`; independent event-storage identity | `sparkHistory.image.*`; chart `imagePullSecrets` is applied to its pod too |

These roles may share an account when that is intentional; they do not need to.
The resolved submission `service_account` overrides a service account written
inside the driver template. API chart account annotations and labels do not
become application defaults. See [driver templates](driver_pod_templates.md) for
the supported merge rules and inspect the generated driver plan.

Cloud IAM bindings, storage credentials and admission policies remain owned by
the cluster administrator. A successful [doctor check](diagnostics.md) establishes
only its reported caller checks and account existence; it does not prove that a
driver, executor or History Server can access your object store. Cloud-specific
recipes and arbitrary-user container support are not qualified by this chart
validation suite.

## Understand the scaling controls

| Control | What it scales | Owner |
| --- | --- | --- |
| Chart `replicaCount` or `autoscaling.*` | API/console replicas | API deployment owner |
| Application `ExecutorInstances` / Spark dynamic allocation | Executors within one Spark application | Application owner |
| Kubernetes cluster/node-pool autoscaler | Available worker nodes | Cluster/cloud administrator |

Increasing `autoscaling.maxReplicas` does not increase an application's executor
ceiling or create nodes. The optional History Server has its own
`sparkHistory.replicaCount` and autoscaling settings. CPU/memory HPA targets need
appropriate container requests and the cluster's metrics support; setting a
target to `null` or `0` disables that metric, and an enabled HPA requires at least
one positive target.

The [metadata recorder](run-history.md) is separate from the Spark History
Server. Enabling `runHistory` requires exactly one API replica, no API autoscaling,
an existing writable persistent claim, and one to twenty explicit namespace
entries. With chart-managed RBAC and an explicit `namespaces` list, every
`runHistory.namespaces` entry must also appear in `namespaces`; otherwise the API
cannot list its driver pods and never becomes ready, so rendering fails. The chart
uses a `Recreate` rollout. It neither provisions the claim nor
validates its filesystem locking behavior; use supported block/local storage,
rehearse backups and allow for rollout downtime.

## Validate before installation

`values.schema.json` checks chart-owned types, image references, service ports,
namespace forms, HPA bounds and recorder settings. Template guards cover
cross-field comparisons and reserved volume names. Run both `helm lint` and
`helm template`: lint can suppress template-level `fail` messages, while rendering
rejects those cross-field conflicts. `namespaces` remains a
comma-separated string; `runHistory.namespaces` is a YAML list. Kubernetes
passthrough objects, such as affinity, resources and security contexts, remain
open so normal cluster-specific fields are accepted.

Rendering checks do not validate cloud permissions, image availability, storage
access or API server admission. Raw `env` overrides still take precedence over
chart-generated environment settings and remain the operator's responsibility.
An `ingress.tls` entry terminates TLS before the API, so browser log streams are
rejected until `env` sets `SPARK_ON_K8S_API_WEBSOCKET_ALLOWED_ORIGINS` to the
public `https://` origins; see
[browser origins](../security-boundaries.md#log-websocket-browser-origins).
To serve the API below an ingress path such as `/platform/spark`, have the ingress
controller remove that prefix before forwarding and set
`SPARK_ON_K8S_API_ROOT_PATH` to the same path in `env`; see
[mounted paths](../security-boundaries.md#mounted-spark-ui-paths).
Use the [development guide](../development.md) for the Helm regression suite and
[release guidance](../releases.md) for the artifact publication boundary.

## Preparing the next chart release

The source chart marks `appVersion` as `unreleased` and requires an explicit
compatible `image.tag` or `image.digest`. The release publisher sets the package's
qualified application version; an empty image tag then selects that version.
API 0.12.1 does not implement `/ready` and is not compatible with these probes.
Do not publish this chart against that older API image. The `helm-release`
workflow refuses an `app_version` below 0.13.0 or without a published
`ghcr.io/hussein-awala/spark-on-k8s` image, and a `chart_version` that the chart
repository already lists. Run it only after the package release has published the
API image, selecting that release tag in the workflow's ref selector so the chart
source matches the qualified application.

When upgrading a release installed from published chart 0.3.0, pass your values
file again, or use `--reset-then-reuse-values` (Helm 3.14 and later), instead of
`--reuse-values`. `--reuse-values` replaces this chart's defaults with the old
release's values, including its `latest` image tags, and omits new settings such
as `rbac`, `runHistory` and `metrics`. The chart schema rejects such an upgrade
with a `missing properties` error before rendering.

Shared RBAC resource names now include the release namespace identity. Existing
installations can set `rbac.legacyNames: true` to retain their old names during an
upgrade. New installations should use the default `false`, allowing identical
release names in separate namespaces. To migrate existing installations, render
and review both RBAC sets, ensure the new bindings exist before removing old
bindings, and verify access under the service account. Switching the value in a
normal Helm upgrade replaces the old resources; schedule that transition if a
brief permissions interruption is unacceptable. Never reuse legacy names for two
releases managing the same shared scope.

History configuration updates now roll its Deployment automatically. History
`affinity` and `tolerations` are passed to the Pod. The rare long-name suffix
collision is disambiguated; existing installations using those colliding names
need a reviewed recreation because Deployment selectors are immutable.
