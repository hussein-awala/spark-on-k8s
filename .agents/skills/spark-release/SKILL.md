---
name: spark-release
description: Prepare or execute an explicitly requested spark-on-k8s release using historical release/discussion evidence, current qualification checks and the repository publication workflows. Preparation alone does not publish artifacts.
---

# Prepare or publish a release

Read root `AGENTS.md` and [the release runbook](../../../docs/releases.md).
The runbook is canonical for historical evidence, current safeguards and release
records; do not copy it into another checklist. Inspect the release, documentation
and Helm workflow files at the intended candidate commit.

Establish the actual repository, selected version, previous published tag and
candidate SHA. For manual dry-run validation, choose the branch/tag in GitHub's
native workflow ref selector and set only the version input; jobs check out the
immutable event SHA, not a user-supplied arbitrary checkout ref. Query current releases/tags; the development version string has
historically lagged published releases and is not a safe version-selection source.
Use the GitHub connector when available for releases. For historical discussion
context, use a supported read-only discussion API or `gh api graphql` if the
connector lacks it. Never substitute issues for discussions or post a new message
as part of research.

Prepare grouped notes with real PR links, significant migration/support changes
and the correct comparison. Distinguish observed historical practice from current
safeguards. The historical Q&A discussions document support-to-fix/example paths,
not a required approval ritual or an automatic announcement channel.

Qualify the exact candidate and built distributions through the runbook and change
checks: installed-wheel profiles, Airflow 3/2.11, real Kubernetes scenarios, relevant
Connect checks, browser assets and strict docs. Do not equate a green PR head with
its merge commit or a skipped workflow with executed scenarios. Record artifact
identities and the evidence for any recommended chart/image pairing.

Before external publication, make the version, commit, notes, validation and
destinations reviewable. If the user requested only preparation, return those
materials without a tag push, publication or publishing dispatch. The runbook's
nonpublishing preview/manual-validation paths can validate artifacts within the
requested preparation scope. If the user already
explicitly authorized this release, continue without asking the same permission
again. A release request does not authorize a new discussion post or unrelated
messages. Respect any separately required environment approval.

When publication is authorized, use signed commits and a verified signed annotated
tag, then existing workflows. Do not disable signing or change identity to bypass
a failure. Inspect `promote_latest`/`promote_minor` decisions; older maintenance releases and
prereleases must not move global channels backward. Stop on ambiguous publication
state, inspect artifacts, and resume only the safe unfinished stage; never move a
published tag or overwrite an immutable version. Finish with published identities,
workflow outcomes and any partial-stage limitation.
