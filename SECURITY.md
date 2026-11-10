# Security policy

spark-on-k8s has one maintainer and community contributors. Security reports are
handled on a best-effort basis: this page says what to expect, not a contractual
response time or support period.

## Reporting a vulnerability

Do not report a suspected vulnerability in a public issue, discussion or pull
request. Use GitHub private vulnerability reporting instead:
[open a private security advisory](https://github.com/hussein-awala/spark-on-k8s/security/advisories/new),
also available as **Report a vulnerability** on the repository's **Security** tab.
Only you and the repository maintainers can see the report. The project has no
separate security e-mail address.

Include as much of the following as you can:

- the affected component (Python client or CLI, API and console, remote submission,
  Airflow provider or task tab, run history, Spark Connect helpers, Helm chart, API
  image or the experimental dbt adapter) and its package, chart or image version, or
  the commit you tested;
- the relevant deployment settings, such as whether remote submission, run history
  or metrics are enabled, the URL prefix and the gateway in front of the API;
- steps to reproduce or a proof of concept, and the impact: what an attacker
  controls and what they gain beyond the access they already have;
- whether the issue is public or reported elsewhere, any disclosure deadline you
  are working to, and how you want to be credited.

Reproduce on a disposable cluster with test credentials. Do not send real tokens,
kubeconfig files, private data or production logs; describe a secret instead of
including it. Only test systems you own or are authorized to test.

## What to expect

These are targets, not guarantees:

- an acknowledgement within 7 days;
- an initial assessment within 14 days: accepted, more information needed, or
  outside the [scope](#scope) below, with the reason;
- for an accepted report, updates in the advisory thread while a fix is prepared.
  Where the fix would reveal the problem, it is developed in the advisory's
  temporary private fork. Time to release depends on severity, complexity and the
  qualification in the [release runbook](docs/releases.md).

If you receive no acknowledgement after 14 days, comment on your advisory, or ask in
[Discussions](https://github.com/hussein-awala/spark-on-k8s/discussions) for the
maintainer to check private reports without including any vulnerability details.
Please keep the details private until the advisory is published or a disclosure
date is agreed in the advisory thread. Reporters are credited in the advisory
unless they ask not to be.

## Supported versions

Security fixes go to the newest release line of each published artifact. The
artifacts have independent version numbers; see
[packages and release order](docs/releases.md#packages-and-release-order).

| Artifact | Receives security fixes | Not supported |
| --- | --- | --- |
| [`spark-on-k8s`](https://pypi.org/project/spark-on-k8s/) Python package and its API image | The latest minor release line, normally through a new patch release | Earlier minor lines, unless an advisory announces a backport |
| `spark-on-k8s` Helm chart | The latest published chart version | Earlier chart versions |
| `dbt-spark-on-k8s` (experimental) | The latest published pre-release, best effort; a fix can require upgrading to a pre-release with other changes | Earlier pre-releases |
| `main` branch | Fixes land here first, and reports against it are welcome | It is not a release |

Published images are not rebuilt in place. A fix to the API image's base image or
bundled dependencies ships as a new release with a new version tag; pin a digest
and upgrade to that release. Fixes are tested against the Python, Spark and Airflow
combinations that the [installation](docs/user-guide/installation.md) and
[Spark compatibility](docs/user-guide/spark_compatibility.md) guides name for that
release. Airflow 2.11 is a legacy compatibility path: it is end-of-life upstream and
keeps dependency versions with known security advisories that this project cannot
fix. Use Airflow 3 for new deployments.

## How fixes are published

An accepted vulnerability is fixed in a release published through the normal
[release workflows](docs/releases.md): a new Python package and API image and, when
affected, a new chart version or dbt adapter pre-release. A
[GitHub Security Advisory](https://github.com/hussein-awala/spark-on-k8s/security/advisories)
is then published with the affected and fixed versions, severity, any workaround and
credit; a CVE identifier can be requested through it. The fixed version's
[release notes](https://hussein.awala.fr/spark-on-k8s/release-notes/) link the
advisory and describe any action users must take.

A security fix can change a documented contract without the usual deprecation
period when no compatible fix is possible. The release notes then explain the change
and its operational consequences, as described in the
[support policy](docs/user-guide/support.md#compatibility-and-retirement). To follow
fixes, watch the repository's releases and its published advisories.

## Scope

The deployment security model is described in:

- [browser and gateway security boundaries](docs/security-boundaries.md);
- [remote submission](docs/user-guide/remote-submission.md), including the operator
  bearer, admission policy and retry safety;
- the [authorization review matrix](docs/user-guide/operations.md#authorization-review-matrix);
- [chart deployment boundaries](docs/user-guide/chart-deployments.md);
- [support and compatibility boundaries](docs/user-guide/support.md).

Report privately when spark-on-k8s fails to enforce a boundary those pages
describe. For example:

- with remote submission enabled, an API, console, proxy, WebSocket, run history or
  metrics request succeeds without the operator bearer, other than `GET` or `HEAD`
  on `/health` and `/ready`;
- a remote plan or submission is admitted although the admission policy does not
  allow it, or a rejected request writes to Kubernetes;
- the operator token, a Kubernetes credential or a Secret value appears in browser
  bootstrap data, a URL, a receipt, JSON error output or a redacted plan, or is
  forwarded to a Spark UI, History Server or driver pod;
- the log WebSocket accepts a browser `Origin` that the documented origin policy
  rejects;
- the Airflow task tab reads or acts on an application that does not belong to the
  authorized task, or performs a write while writes are disabled;
- a proxy request reaches a target other than the requested Spark UI or History
  Server, or client-controlled request headers can redirect console pages;
- UID-guarded `app cancel` deletes a pod other than the one its receipt identifies;
- the chart grants Kubernetes permissions beyond those its values document;
- a published artifact does not match its source and recorded build inputs, or a
  workflow can be made to publish from untrusted input.

These are documented boundaries, not vulnerabilities by themselves. If the
documentation is misleading about them, open an ordinary issue or pull request:

- Every holder of the shared operator token can use every route the deployment
  exposes, including listing, logs, kill and delete. A shared operator token is not
  tenant isolation, and namespace allowlists are not per-user authorization.
- With remote submission disabled, the API has no built-in authentication; the
  network and gateway in front of it control access.
- Approved images and application code run with their service account's
  permissions and are not sandboxed. A user with Kubernetes credentials can submit
  anything their RBAC allows.
- Proxied Spark UI and History Server pages share the console's browser origin;
  run those services inside the same trust boundary.
- The namespace setup helper's cluster-wide `edit` binding is a documented
  convenience; production deployments provision namespace-scoped access.
- Driver logs, Kubernetes events, diagnostic reports and run history fields contain
  workload-controlled data that can include sensitive values.
- TLS, browser authentication, CSRF protection for write requests and rate limiting
  are gateway responsibilities.
- A vulnerability in Spark, Kubernetes, Airflow, dbt or another dependency without
  an exploitable path through spark-on-k8s belongs upstream. An already public
  dependency advisory that affects the default installation or API image can be
  raised as an issue or a pull request that updates the lock file.
- Example applications, starter projects and test fixtures are illustrations, not
  hardened deployments.

## Release artifacts

Python distributions are published to PyPI from the tag-triggered release workflow
using trusted publishing. The API image is built from the release wheel and
hash-checked locked dependencies, and its build requests OCI SBOM and provenance
attestations. The [release runbook](docs/releases.md) describes these records. They
identify build inputs; they do not certify that dependencies are free of known
vulnerabilities.

The release workflow signs the API images it publishes to
`ghcr.io/hussein-awala/spark-on-k8s` and `docker.io/husseinawala/spark-on-k8s` by
digest, with Sigstore cosign keyless signing. The certificate identity is
`https://github.com/hussein-awala/spark-on-k8s/.github/workflows/release.yml@refs/tags/<version>`
and the issuer is `https://token.actions.githubusercontent.com`. The signed digest
is the multi-architecture image whose platform images passed the workflow's checks
before the PyPI upload; it is copied to both registries without a rebuild. See
[verify a published API image](docs/user-guide/chart-deployments.md#verify-a-published-api-image).

Images released before signing was added, including 0.12.1 and earlier, are not
signed. Python distributions, Helm charts and the dbt adapter are not signed by this
mechanism. A valid signature shows which workflow run published an image; it does
not certify that the image or its dependencies are free of vulnerabilities.
