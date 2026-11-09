# Security Policy

## Supported Versions

This project is still under development and does not have a stable version yet.
In the current state, the security issues will be addressed as soon as possible in a new patch release or a new minor
release if there is any merged pull request that introduces a new feature.

## Reporting a Vulnerability

If you find a security vulnerability, please do not open a public issue. Instead, you can open a private security
advisory via this [link](https://github.com/hussein-awala/spark-on-k8s/security/advisories/new), and after providing
the necessary information, we will review your advisory and take the necessary actions to fix it.


## Verifying release artifacts

The release workflow signs the API images it publishes to `ghcr.io/hussein-awala/spark-on-k8s`
and `docker.io/husseinawala/spark-on-k8s` by digest, with Sigstore cosign keyless signing. The
certificate identity is
`https://github.com/hussein-awala/spark-on-k8s/.github/workflows/release.yml@refs/tags/<version>`
and the issuer is `https://token.actions.githubusercontent.com`. The signed digest is the
multi-architecture image whose platform images passed the workflow's checks before the PyPI
upload; it is copied to both registries without a rebuild and carries BuildKit SBOM and
provenance attestations. See [verify a published API image](docs/user-guide/chart-deployments.md#verify-a-published-api-image).

Images released before signing was added, including 0.12.1 and earlier, are not signed. Python
distributions, Helm charts and the dbt adapter are not signed by this mechanism. A valid
signature shows which workflow run published an image; it does not certify that the image or its
dependencies are free of vulnerabilities.

## Deployment boundaries

See [browser and gateway boundaries](docs/security-boundaries.md) and the
[authorization matrix](docs/user-guide/operations.md#authorization-review-matrix)
for surface-specific safeguards and deployment responsibilities. Submission policy
does not harden every deployment automatically. See the
[support policy](docs/user-guide/support.md) for maintenance and migration scope.
