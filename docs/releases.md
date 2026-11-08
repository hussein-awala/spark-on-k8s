# Release runbook

Use this runbook to prepare a release or to publish one when the user/maintainer
has explicitly authorized publication. Preparing notes, local checks and an artifact
record does not publish; the documented dry-run workflow performs hosted validation
without publishing. Pushing a version tag starts external publication.
Reuse authorization already given for the concrete release. Do not ask again merely
because this page contains a publication checkpoint.

## Observed practice and its evidence

The following historical sources were inspected on 7 November 2026. They establish
naming and communication patterns, not proof that every historical release ran
present-day checks.

| Evidence | What it establishes |
| --- | --- |
| [0.12.1, 7 April 2025](https://github.com/hussein-awala/spark-on-k8s/releases/tag/0.12.1) | Bare numeric tag, title `Spark On K8S 0.12.1`, grouped fixes/miscellaneous changes, PR links and a full comparison link; documentation versioning is listed as a change |
| [0.12.0, 17 February 2025](https://github.com/hussein-awala/spark-on-k8s/releases/tag/0.12.0) | Fixes, features and maintenance changes are grouped and linked to their PRs |
| [0.11.0, 19 January 2025](https://github.com/hussein-awala/spark-on-k8s/releases/tag/0.11.0) | Python 3.8 support removal is called out as a significant change |
| [Discussion 127](https://github.com/hussein-awala/spark-on-k8s/discussions/127) | A concrete UI configuration request received a maintainer reply linking PR 132 and the new settings, subsequently listed in 0.12.0 |
| [Discussion 124](https://github.com/hussein-awala/spark-on-k8s/discussions/124) | A usage request led to published driver examples; the maintainer explicitly noted the then-current main-branch docs and planned versioning |
| [Discussion 72](https://github.com/hussein-awala/spark-on-k8s/discussions/72) | Packaging guidance favored including Python modules in the application image, with alternatives described rather than a new deployment system |

These discussions are user support evidence. They do **not** establish a mandatory
release announcement, voting process or permission to post a new message.
Source links in release notes should connect a change to its real PR/discussion;
do not describe analytical review personas as customer feedback.

The current repository workflows are the source of truth for mechanics:

- `.github/workflows/release.yml`: tag-triggered Python distributions, PyPI and
  API image publication. The release build sets the package version from the tag.
- `.github/workflows/deploy-doc.yml`: main publishes `develop`; a qualified tag
  publishes that documentation version. Only the highest stable numeric version
  can update the `latest` alias.
- `.github/workflows/helm-release.yml`: independent manual dispatch with
  `app_version` and `chart_version`, publishing to the chart repository.

Read these files at the candidate commit before acting. Existing release tags can
be newer than the development version string in `pyproject.toml` (the inspected
source had `0.11.0-dev` while historical releases include `0.12.1`). Never select
the next version by incrementing that development string alone.

## Packages and release order

One project release can involve up to four independently versioned artifacts. They
are published by different workflows and must not be forced to share a number:

| Artifact | Version source | Publication | Depends on |
| --- | --- | --- | --- |
| `spark-on-k8s` wheel/sdist and API image | Bare `X.Y.Z` tag | `release.yml` on the tag push | Exact qualified commit |
| Versioned documentation | Same tag | `deploy-doc.yml` on the tag push | Same eligibility record |
| Helm chart | `chart_version` input; `appVersion` from `app_version` | Manual `helm-release.yml` | Published API image for `app_version` |
| Experimental `dbt-spark-on-k8s` | `integrations/dbt/pyproject.toml` | Not automated yet | Published `spark-on-k8s` on PyPI |

Publish in that order. The chart's default image tag is its `appVersion`, so
releasing the chart before the matching API image exists produces an install that
cannot pull its image. The dbt adapter's metadata must name a released SDK
version, which only exists after the package upload. Record a chart or adapter that
is intentionally not released with the package as such, rather than leaving it
implied.

## Release notes

Canonical release notes live in `release-notes/X.Y.Z.md`, one file per GitHub
release. Each file contains YAML metadata followed by the exact GitHub release body:

```markdown
---
version: "0.13.0"
title: "Spark On K8S 0.13.0"
date: null
status: "planned"
github_url: "https://github.com/hussein-awala/spark-on-k8s/releases/tag/0.13.0"
---

Release body …
```

`scripts/release_notes.py check` validates the metadata, filename/version match and
LF line endings, and rejects a planned release older than the newest published one.
Bodies of releases published before this page existed were copied from GitHub
without rewording. Draft the next release as `status: "planned"` while changes
merge; keep significant/migration changes first, then features, fixes, other
packages, maintenance, contributors and the exact comparison link.

The [release-notes page](https://hussein.awala.fr/spark-on-k8s/release-notes/) is
a separate small site shared by every documentation version, and every version's
navigation links to that permanent URL. Main-branch documentation deployments
rebuild it from `release-notes/*.md` and commit it to `gh-pages` under
`release-notes/`; tag deployments leave it untouched, so publishing an older
maintenance release cannot replace newer history. It is not a mike version and
does not appear in the version selector. Planned releases are labeled
**In preparation**, and their documentation and comparison links open the
`develop` documentation and a comparison with `main` until the release exists.
Preview it locally with:

```bash
.venv-quality/bin/python scripts/release_notes.py build --output-dir release-notes-site
.venv-quality/bin/python scripts/check_docs.py release-notes-site
```

When publishing, export the reviewed body instead of maintaining a second copy.
Extraction only writes a local file:

```bash
.venv-quality/bin/python scripts/release_notes.py extract "$RELEASE_VERSION" \
  --output "/tmp/spark-on-k8s-$RELEASE_VERSION-notes.md"
```

After the package and GitHub release are confirmed, set the file's `status` to
`published` and its `date` to the GitHub publication date in a signed
documentation PR. Its main deployment updates the shared page without rebuilding
archived version documentation. Correct historical bodies only deliberately, and
edit the GitHub release in the same change so the two stay identical.

## Separate experimental dbt distribution

`integrations/dbt` is an opt-in experiment with an independent version and lockfile.
The main release workflow builds and publishes the root package only; it does not
publish `dbt-spark-on-k8s`. The experimental package currently pins the development
SDK from the same checkout. Before changing that pin or publishing the experiment,
select the actual compatible SDK artifact, update its lockfile, and run both
transport jobs in `dbt-kubernetes.yml` against the proposed pair. A normal root
release does not establish compatibility for a separately rebuilt adapter.

## Current safeguards

Signed commits, explicit tested-commit evidence, compatibility notes and an artifact
record below are requirements of this contributor workflow. They are stated here
as current safeguards, not inferred historical practice. Automated gates should
implement the same checks; do not treat a documented manual check as an enforced
GitHub rule. Inspect existing repository/environment controls before adding a
redundant gate.

## Implementation and dry-run

`scripts/release_eligibility.py` validates a bare `X.Y.Z` version (optionally `aN`,
`bN` or `rcN`) and requires successful runs of `ci.yml`, `spark-compatibility.yml`,
`spark-connect.yml` and `airflow-kubernetes.yml` for the exact commit. Only same-repository
push/manual runs qualify; PR runs and another commit's green results do not. The
latest eligible run must pass every job: empty or skipped execution fails closed.
Missing/in-progress runs are polled for up to 30 minutes by the workflow; failures,
API errors or expiry stop publication. The CLI accepts a bounded 1–3600 second
budget for independent checks.

For a local read-only eligibility check, set `RELEASE_VERSION` to the reviewed
candidate version and provide an existing read-only Actions credential through
`GH_TOKEN`, then run:

```bash
CANDIDATE_SHA="$(git rev-parse HEAD)"
python scripts/release_eligibility.py \
  --repository hussein-awala/spark-on-k8s --commit "$CANDIDATE_SHA" \
  --version "$RELEASE_VERSION" --timeout-seconds 300 \
  --output /tmp/spark-release-eligibility.json
```

For the `release` workflow's manual **Run workflow**, choose the candidate branch
or tag in GitHub's native ref selector and supply the `version` input. Every job
checks out the resulting immutable event SHA; there is no arbitrary checkout-ref
input. This path runs eligibility, builds distributions and tests frozen API
images for Linux amd64 and arm64, then stops before PyPI/registry publication. It
does not trigger a documentation push. Qualify missing main-commit workflows via
their own manual runs first; the core CI dispatch runs all profiles even for a
documentation-only candidate.

Packaging-related PRs run the same artifact/container path with preview version
`0.0.0rc1` and read-only permissions. They intentionally skip release eligibility:
PR image validation does not claim that the future merge commit passed main checks.
Both publishing jobs are restricted to tag-push events. Preview artifacts must not
be used as released packages.

`scripts/release_artifacts.py prepare` rejects ambiguous build directories and
mismatched wheel/sdist versions. The workflow exports the API-only dependency
closure from `uv.lock` before substituting the release version. The API Dockerfile
installs the actual wheel plus hash-checked binary dependencies from that export;
it no longer resolves the project version from PyPI. Generated build inputs live
under `docker/release/` and must not be committed. Their manifest records source
SHA, wheel/sdist hashes, lock/requirements hashes, pinned base reference and the
actual bundled frontend components from the wheel's license notices. Airflow's
host-supplied React is not counted as a bundled dependency. Each architecture
records installed Python packages at `/opt/spark-on-k8s/release/installed.json`.

The `release-eligibility`, `release-container-inputs`, `release-installed-*` and
`published-release-manifest` workflow artifacts contain these records. Registry
publication rebuilds from the same frozen inputs; it does not promote the exact
image digest used in the architecture checks and does not promise bit-for-bit
reproducibility. It also requests the build action's OCI SBOM and provenance attestations.
Attach the relevant manifest and qualification links to the GitHub release for
retention beyond transient workflow artifacts. A generated inventory or SBOM is
not a certification that dependencies are vulnerability-free.

New tag-publication runs are serialized. The gate compares all existing bare
stable numeric tags: only a highest stable version may move global `latest`, and
only a highest version within a minor may move its `major.minor` image tag.
Prereleases move neither. Documentation uses the same global rule. Thus a later
`0.12.2` maintenance release does not overwrite `latest` after `0.13.0`. A higher
failed tag conservatively prevents lower-version promotion; resolve that state
explicitly instead of automatically moving channels backward. GHCR's destination
uses the repository owner, independently of the person rerunning the workflow.

### Documentation publication

The docs workflow adds its isolated environment to `PATH` because `mike` launches
its documentation builder as a child command. `mike deploy` builds once using the
strict configuration, updates only the local `gh-pages` branch, and records the
source SHA and channel in `versions.json`. The generated HTML must pass the local
link, anchor and asset check before that branch is pushed. The checked site is
retained as the `documentation-site` workflow artifact.

Every newly built page labels its channel: `develop` is unreleased main-branch
content; a prerelease names its candidate version; a release names its exact tag.
The source link identifies the immutable commit used for that build. Local builds
without publication metadata say **Local documentation preview**. Existing archived
versions are preserved and may predate these labels. The version selector and
`latest` alias describe documentation versions, not the package installed in a
reader's environment. Page revision dates describe source edits, not the last
successful deployment.

After a docs deployment, verify the public `develop/` or versioned homepage, its
source link and `versions.json` against the workflow commit. Check `latest/` only
when the release is eligible to advance that alias. A failed publishing run can
leave older pages publicly available; a successful local build alone does not
prove the public site has updated. Diagnose the failed run before retrying, and
never treat an old page's content or timestamp as evidence for the new commit.

### 1. Establish the candidate

Record the intended version, full commit SHA, previous published tag and intended
release type. Inspect local changes, repository identity, remotes and current
published tags/releases. Use the actual configured remote or explicit repository
URL; do not assume that `origin` exists. Confirm the candidate contains the intended
merged changes, and that the version is unused. Do not guess a release number when
it has not been selected by the maintainer.

Compare from the previous actual release, not from an arbitrary development version.
Prepare notes in `release-notes/X.Y.Z.md` as described in [Release notes](#release-notes):
significant/migration changes first, then features, fixes, other packages,
maintenance, contributors where applicable, and the exact comparison link. Include
support retirements, changed defaults and operational limitations. The planned file
is a reviewable draft; merging it does not publish a release.

### 2. Qualify the exact commit and artifacts

Use [development validation](development.md). For a release candidate, require:

- Locked dependencies; lint, format, strict docs and clean package builds.
- Core/API CI across the supported Python matrix and installed-wheel checks.
- Installed-wheel provider/operator checks for both Airflow 3 and 2.11, including
  lazy discovery and Airflow 3's native task UI with only its own extra installed.
- Shared frontend build/browser checks and clean regenerated packaged assets.
- Real Spark Kubernetes and Airflow Kubernetes scenarios for the selected candidate;
  the real Connect workflow when that integration is included in the release.
  Preserve the workflow URLs, commit SHA and scenario artifacts.
- Verification that distribution metadata equals the selected release version,
  and that container inputs use the release wheel and intended dependency/base inputs.

Required evidence must match the exact commit that will be tagged. A green check
on a PR head does not prove the merge commit passed. Path-filtered workflows can
have no run on a docs-only merge; run the missing qualification on the candidate
when publication requires it. A successful workflow with skipped required scenarios
is insufficient. If infrastructure is unavailable locally, use hosted disposable
jobs and wait for their results; never substitute mocked or local-mode results for
Kubernetes evidence.

Build the release-version candidate in an isolated checkout/output directory when
the development version differs. Keep the working development checkout intact.
Record which built artifacts were actually tested; a source checkout test does not
validate a wheel. The release workflow must fail before publishing on missing or
failed eligibility evidence, version mismatch or artifact validation failure.

### 3. Review identity, notes and authorization

Verify signed commits using the contributor's configured trust setup. Do not
disable signing, impersonate a maintainer or replace signing identity to get past
a failure. Resolve unknown-key verification separately from an invalid signature.
Use a signed annotated version tag for a newly published release and verify its
signature before pushing. These are current safeguards; they are not claims about
all historical tags.

At this checkpoint the candidate SHA/version, passing evidence, release notes and
artifact destinations must be concrete. If the request was preparation only,
return those materials and stop before a tag push, release publication or publishing
workflow dispatch. The explicitly nonpublishing validation path above remains available. If publication of this release was already authorized, proceed within
that authorization. Do not publish a discussion or contact contributors without
explicit authorization for that communication.

### 4. Publish through the existing channels

Push only the intended verified version tag to the inspected repository. Tags
trigger package/image and documentation publication; monitor all relevant runs.
Before a prerelease or maintenance release, inspect the eligibility record's
`promote_latest` and `promote_minor` decisions and establish the intended destinations.
The version itself can publish without advancing either mutable channel.

Publish the GitHub release against that tag with the body extracted from its
release-notes file, without creating a second tag at a different commit. For
example, `gh release create "$RELEASE_VERSION" --verify-tag --title "Spark On K8S
$RELEASE_VERSION" --notes-file "/tmp/spark-on-k8s-$RELEASE_VERSION-notes.md"`.
Publish an updated chart only when requested and needed, after the matching API
image exists, using the independent workflow's explicit app/chart versions.
Do not force all channels to increment together. Do not dispatch a workflow from a
different default-branch commit while claiming it validated the tagged candidate.

### 5. Verify and record the usable combination

Resolve published artifacts and retain a small release record containing:

| Field | Required evidence |
| --- | --- |
| Source/package | Full source commit, tag, package version and wheel/sdist hashes |
| API image | Registry references and immutable digests; base/dependency inventory where produced |
| Chart | Actual chart version and app version, or explicitly `not released/qualified` |
| Documentation | Version URL and source revision |
| Validation | Exact workflow runs, scenario outcomes and limits of validation |

For a recommended API deployment, render the selected chart with the exact image
reference and record the settings used. Claim a cluster-qualified combination only
after that exact image/chart combination passes a staging smoke test; otherwise
label it render/package validation. Historical releases without such a record are
not retroactively certified. Link installation instructions to the recorded version,
not mutable `latest` or main-branch examples.

If a stage fails, preserve successful artifacts, inspect the failed stage, and
retry only a safe, understood operation. Do not delete/reuse a published version,
move a published tag, or repeatedly upload an immutable package. A correction may
need a new patch release. Document partial publication and resolve it before claiming
the release complete.
