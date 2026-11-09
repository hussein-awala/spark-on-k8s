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

The default `readOnly: true` remains unchanged. With chart-managed RBAC, the default
empty `namespaces` creates a Role and RoleBinding in the release namespace, and an
explicit namespace string creates them in each listed namespace.
`namespaces: "*"` intentionally selects a cluster-wide binding instead. The chart
passes the same scope to the API as `SPARK_ON_K8S_API_NAMESPACES`, so the API refuses
other namespaces even where Service DNS would reach them. Neither the namespace scope
nor hiding UI buttons provides user or tenant authorization; use the
[remote-submission security boundary](remote-submission.md).

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

## Run the API as a non-root user

The API image runs as the dedicated user and group `10001`. The chart's default
`podSecurityContext` and `securityContext` match that identity and meet Pod
Security `restricted`: `runAsNonRoot`, `runAsUser`, `runAsGroup` and `fsGroup`
`10001`, `fsGroupChangePolicy: OnRootMismatch`, the `RuntimeDefault` seccomp
profile, no privilege escalation and all capabilities dropped. Override individual
keys in your values file, or set a key to `null` to remove it; an image built with
another user needs matching IDs. The History Server keeps its own security settings.

The API must be able to read files mounted from volumes, such as a
remote-submission token or policy, or a kubeconfig selected with
`SPARK_ON_K8S_CONFIG_FILE`. Its home directory is `/home/spark-on-k8s`; `/root`
is not readable. Keep `service.port` at 1024 or above, because some container
runtimes do not let a non-root process bind lower ports.

A run-history claim written by an earlier root API process holds root-owned
files. On volume types that support `fsGroup`, such as most block-storage CSI
drivers and `local` volumes, the kubelet makes them group-writable at the first
non-root mount. Volume types that ignore `fsGroup`, such as `hostPath`-backed
provisioners, keep them read-only: the API then exits with `attempt to write a
readonly database`. Before upgrading such a claim, change ownership from the
current root pod:

```bash
kubectl exec --namespace <release-namespace> deploy/<release-fullname> -- \
  chown -R 10001:10001 /var/lib/spark-on-k8s
```

If the upgraded API already exited, `helm rollback` to the root release and run
the command there before upgrading again. To keep running as root instead,
restore the previous identity and capabilities; the capabilities let root write
files that a failed non-root start may have created. Such a pod does not meet Pod
Security `restricted`:

```yaml
podSecurityContext:
  runAsNonRoot: false
  runAsUser: 0
  runAsGroup: 0
securityContext:
  capabilities: null
```

`readOnlyRootFilesystem` is not a default because the Kubernetes client writes
temporary files in some configurations, such as kubeconfig credentials embedded
as data. To enable it, also mount a writable `/tmp`:

```yaml
securityContext:
  readOnlyRootFilesystem: true
volumes:
  - name: tmp
    emptyDir: {}
volumeMounts:
  - name: tmp
    mountPath: /tmp
```

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
entries. Unless `namespaces` is `"*"`, every `runHistory.namespaces` entry must also
be in the API scope: the release namespace by default, or the listed namespaces.
Otherwise the API refuses to start, and with chart-managed RBAC rendering fails. The chart
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

## Add operational objects

The settings in this section are disabled by default, apart from the History
Server probes, which apply only when `sparkHistory.enabled` is set. A release
upgraded with `--reuse-values` from a chart without these settings renders as
before. Some invalid combinations fail only during rendering, so run
`helm template` as well as `helm lint`.

### Restrict API traffic

`networkPolicy.enabled` adds a NetworkPolicy for the API pods. It admits traffic
to the API port only from `networkPolicy.ingress.from` and from pods with this
release's API labels: the API pods and the chart's `helm test` pod. With an empty
list nothing else can connect, including an ingress controller, a gateway,
Prometheus and clients of a `LoadBalancer` or `NodePort` Service. List every peer
that needs the API:

```yaml
networkPolicy:
  enabled: true
  ingress:
    from:
      - namespaceSelector:
          matchLabels:
            kubernetes.io/metadata.name: ingress-nginx
      - namespaceSelector:
          matchLabels:
            kubernetes.io/metadata.name: monitoring
        podSelector:
          matchLabels:
            app.kubernetes.io/name: prometheus
```

The policy needs a network plugin that enforces NetworkPolicy; other plugins
accept the object and enforce nothing. NetworkPolicy does not block traffic from
a pod's own node, so kubelet probes keep working. Traffic through a
`LoadBalancer` or `NodePort` Service can arrive with a node address rather than
the client's, depending on `externalTrafficPolicy` and the plugin. A NetworkPolicy
limits which peers can connect; it does not authenticate callers or replace
[remote mode](remote-submission.md) or an authenticating gateway.

`networkPolicy.egress.enabled` also restricts the API's outbound connections to:

- DNS on port 53 to `networkPolicy.egress.dns.to`: kube-dns pods in
  `kube-system` by default. An empty list allows port 53 to any destination, for
  example a node-local DNS cache;
- the Kubernetes API server peers in `networkPolicy.egress.kubernetesApi.to`, on
  `kubernetesApi.ports` (443 and 6443 by default);
- port 4040 on pods labeled `spark-role=driver` in the namespaces selected by
  `namespaces`, for the Spark UI proxy;
- this release's API pods, so its `helm test` pod can connect, and the History
  Server pods on port 18080 when `sparkHistory.enabled` is set;
- `networkPolicy.egress.extraRules`, for example a cluster selected by
  `SPARK_ON_K8S_CONFIG_FILE`.

The chart cannot discover API server addresses. Most plugins evaluate egress after
a Service address is translated to its endpoints, so list the endpoint addresses
of the `default/kubernetes` Service:

```bash
kubectl get endpointslices --namespace default \
  --selector kubernetes.io/service-name=kubernetes \
  -o jsonpath='{range .items[*].endpoints[*]}{.addresses[0]}{"\n"}{end}'
```

```yaml
networkPolicy:
  egress:
    enabled: true
    kubernetesApi:
      to:
        - ipBlock:
            cidr: 172.18.0.2/32
```

With an empty `kubernetesApi.to`, the chart's policy grants no API server access:
the API stays unready unless another policy allows it, and the install notes warn
about it. Plugins differ: some match the Service address before translation, and
some do not match node or control-plane addresses with `ipBlock` at all and need
their own policy resources for the API server. After enabling egress, check
readiness, the Spark UI proxy and log streams. The driver namespace selector
follows `namespaces`; an `env` override of `SPARK_ON_K8S_API_NAMESPACES` does not
change it.

`sparkHistory.networkPolicy.enabled` adds an ingress-only NetworkPolicy for the
History Server: the API pods and `sparkHistory.networkPolicy.ingress.from` can
reach port 18080. History Server egress, such as to event-log storage, stays
unrestricted.

### Keep replicas available during node drains

`podDisruptionBudget.enabled` adds a PodDisruptionBudget for the API pods, and
`sparkHistory.podDisruptionBudget.enabled` one for the History Server pods. Set
`minAvailable` or `maxUnavailable` to a pod count or a percentage such as `"50%"`.
Setting both is rejected; setting neither uses `maxUnavailable: 1`.

Rendering fails when a budget permits no voluntary disruption at the smallest
replica count, which is `replicaCount`, or `autoscaling.minReplicas` with
autoscaling enabled. Such a budget blocks `kubectl drain`, node pool upgrades and
cluster autoscaler scale-down indefinitely. For example, `minAvailable: 1` with one
replica is rejected; Kubernetes rounds percentages up, so `minAvailable: "60%"`
with two replicas is rejected too. Run at least two API replicas to keep the API
available while a node drains. A single-replica API, as [run history](run-history.md)
requires, can only use a budget that allows its pod to be evicted, so a budget
adds no protection there.

### Scrape metrics with the Prometheus Operator

`metrics.serviceMonitor.enabled` adds a `monitoring.coreos.com/v1` ServiceMonitor
for the API's `/metrics` and requires `metrics.enabled`. Rendering fails if the
cluster does not serve the ServiceMonitor API, rather than silently omitting the
object and leaving monitoring unconfigured. `helm template` and client-side dry
runs do not query a cluster, so pass the API explicitly:

```bash
helm template spark-console ./chart --namespace spark-system \
  --set metrics.enabled=true --set metrics.serviceMonitor.enabled=true \
  --set-string image.tag="$API_IMAGE_TAG" \
  --api-versions monitoring.coreos.com/v1/ServiceMonitor
```

Set `metrics.serviceMonitor.labels` to match your Prometheus
`serviceMonitorSelector`, and `interval` or `scrapeTimeout` to override the
Prometheus defaults. The API and History Server Services share the chart's
selector labels, so the ServiceMonitor keeps only the API Service's targets by
name. With `networkPolicy.enabled`, also list Prometheus in
`networkPolicy.ingress.from`. Metric values come from the run-history database, so
every API worker reports the same values.

With `remote.enabled`, `/metrics` requires the operator bearer token, and the
ServiceMonitor authenticates with the `remote.tokenKey` entry of
`remote.existingSecret`. The Prometheus Operator then copies that token into the
configuration of every Prometheus instance that selects the ServiceMonitor. The
token grants full operator access to the API, including submissions, logs, kill
and delete: restrict access to that Prometheus configuration accordingly, or do
not enable the ServiceMonitor in remote mode.

### Set the API worker count

`workers` sets `SPARK_ON_K8S_API_WORKERS`, the number of API processes in each
pod, from 1 to 64. The default `null` keeps the image default, which is 4 in the
published image. An `env` entry with the same name takes precedence. Each worker
is a separate process with its own memory, Kubernetes client and
`SPARK_ON_K8S_API_LIMIT_CONCURRENCY` connection limit, so raise `resources` with
the worker count. Run history keeps its single replica but supports several
workers in that pod: a database lease elects one collector. In remote mode, each
worker reads the token and policy files when it starts.

### Probe the History Server

With `sparkHistory.enabled`, the History Server container has readiness and
liveness probes on its UI port, 18080, at `/api/v1/version`. Readiness keeps a
starting server out of its Service. Liveness restarts a server that has stopped
answering for about two minutes, after a one-minute initial delay. Adding the
probes changes the pod template, so upgrading a release with the History Server
enabled restarts its pods once.

Override individual probe fields under `sparkHistory.readinessProbe` and
`sparkHistory.livenessProbe`, or set a probe to `null` to remove it. Helm merges
your probe with the default, so set `httpGet: null` beside another handler such as
`tcpSocket`. An authentication filter on the History Server UI must allow the
probe path, or the probe must use `tcpSocket`.

### Enable remote submission

`remote.enabled` configures [remote submission](remote-submission.md). The chart
sets `SPARK_ON_K8S_SUBMISSION_ENABLED`, `SPARK_ON_K8S_SUBMISSION_TOKEN_FILE` and
`SPARK_ON_K8S_SUBMISSION_POLICY_FILE`, and mounts only the `remote.tokenKey` and
`remote.policyKey` entries of `remote.existingSecret` read-only under
`remote.mountPath` (`/etc/spark-on-k8s/remote` by default). The chart never
creates this Secret and has no value for an inline token, so the token stays out
of Helm values and release history. Create the Secret in the release namespace
with your secret tooling, for example from a random token file and the policy:

```bash
kubectl create secret generic spark-api-remote --namespace spark-system \
  --from-file=token=./submission-token --from-file=policy.json=./submission-policy.json
```

```yaml
remote:
  enabled: true
  existingSecret: spark-api-remote
namespaces: analytics
readOnly: false
```

Every route except `/health` and `/ready` then requires the bearer token,
including the console; kubelet probes and the chart's `helm test` use those two
public routes. Submissions create driver pods and Services, so with chart-managed
RBAC set `readOnly: false`. Keep the policy's namespaces within `namespaces`: the
API refuses to start with a policy namespace outside its scope. A missing Secret
key also prevents the pod from starting. Each worker reads the files when it
starts, so restart the Deployment with `kubectl rollout restart` after rotating
the token or changing the policy. `env` entries for the same variables take
precedence, and `remote-submission` is a reserved volume name. The token is one
shared operator credential, not tenant isolation.

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

The default `namespaces` value changed from `"*"` to empty, which selects only the
release namespace. A release that relied on the old default loses its cluster-wide
binding on upgrade, and the API then refuses other namespaces. Set `namespaces: "*"`
or list the namespaces before upgrading. With `"*"`, the console opens the API's own
namespace unless `SPARK_ON_K8S_API_DEFAULT_NAMESPACE` is set in `env`. If `env` sets
`SPARK_ON_K8S_API_DEFAULT_NAMESPACE` to a namespace outside the new scope, the API
refuses to start and `helm upgrade --wait` times out: include that namespace in
`namespaces`, or change or remove the variable, in the same upgrade.

History configuration updates now roll its Deployment automatically. History
`affinity` and `tolerations` are passed to the Pod. The rare long-name suffix
collision is disambiguated; existing installations using those colliding names
need a reviewed recreation because Deployment selectors are immutable.
