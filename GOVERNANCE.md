# Governance

This policy describes how spark-on-k8s is maintained today, the roles contributors
can grow into and how people move between them. The criteria below are guidelines
for the maintainers' judgment, not entitlements or quotas. Changes to this policy
follow [Changing this policy](#changing-this-policy).

## Current state

The project has one maintainer, Hussein Awala
([@hussein-awala](https://github.com/hussein-awala)), who owns every area in
[CODEOWNERS](.github/CODEOWNERS), reviews and merges pull requests, and holds the
repository administration rights that releases depend on. Continuity therefore
depends on one person. Growing reviewers and area maintainers, and then a second
maintainer who can release, is the main goal of this policy.

Review is best effort. There is no response-time or long-term support SLA; see
[support boundaries](docs/user-guide/support.md).

## Roles

Each role includes everything the roles before it can do.

| Role | GitHub access | Can |
| --- | --- | --- |
| Contributor | None needed | Open issues, discussions and pull requests; review any pull request; propose work from [the opportunities list](TODO.md) |
| Reviewer/triager | Triage | Label, assign, close and reopen issues and pull requests; mark duplicates; request reviews. Reviews are weighed by area owners but do not satisfy code-owner review |
| Area maintainer | Write; listed in [CODEOWNERS](.github/CODEOWNERS) for named areas | Approve pull requests as code owner for those areas; merge a pull request once its checks pass and every area it touches has a code-owner approval; decide scope within the area consistently with [support boundaries](docs/user-guide/support.md) |
| Maintainer | Admin | Everything above in every area; own workflows, release tooling and this policy; manage repository settings, branch protection, secrets and environments; authorize and perform releases; handle security advisories; grant and remove roles |

An area matches a section of [CODEOWNERS](.github/CODEOWNERS): Python SDK, CLI,
API, run history, Airflow, shared console, Helm chart and images, experimental dbt
adapter, and documentation. Workflows, release tooling and governance files stay
with maintainers because they control publication and access.

Write access can push tags and dispatch workflows, and both can start publication.
Before a non-maintainer receives write access, maintainers restrict release tag
creation and make every publishing workflow require maintainer approval, for
example through protected environments, so area maintainer access cannot start a
release.

## Moving up

Anyone can nominate themselves or someone else in a GitHub discussion or issue,
linking the contributions and reviews that support the nomination. Maintainers
decide as described in [Decisions](#decisions). An accepted nomination is a pull
request that updates the [roster](#roster) and, for area maintainers,
[CODEOWNERS](.github/CODEOWNERS).

All roles above contributor expect the
[contributor workflow](CONTRIBUTING.md#before-a-pull-request): checks chosen and
reported as [development guidance](docs/development.md) describes, canonical
documentation updated with behavior, commits signed with your own identity, and
the [code of conduct](CODE_OF_CONDUCT.md). Accounts with write or admin access must
use two-factor authentication.

**Contributor to reviewer/triager.** Over roughly three months, several merged
pull requests beyond typo fixes, or consistent and accurate help on issues,
discussions and reviews. Reviews respect the explicit contracts in
[AGENTS.md](AGENTS.md): uncertain status is not absence, a submission receipt is
not data success, a timeout is not implicit cancellation, and a shared operator
token is not tenant isolation.

**Reviewer to area maintainer.** Roughly six months of sustained, quality
contributions and reviews in one area. The candidate knows that area's validation
route, including its installed-wheel and real Kubernetes checks, reports
unexecuted checks honestly, and has reviews that caught real problems or kept
changes within scope.

**Area maintainer to maintainer.** Roughly six months as an area maintainer with
reviews or changes across more than one area or in cross-cutting work such as CI or
packaging. The candidate has prepared at least one release candidate with the
[release runbook](docs/releases.md) without publishing it, and follows the
[security policy](SECURITY.md) for private reports. Existing maintainers must all
agree.

## Decisions

Work is discussed in issues, discussions and pull requests, so decisions and their
reasons stay public. Pull requests use lazy consensus: a pull request that follows
[CONTRIBUTING.md](CONTRIBUTING.md), passes its applicable checks and has a code-owner
approval for every area it touches can be merged when no objection remains
unresolved. An objection names what must change or why the change is out of scope.

While the project has a single maintainer, that maintainer makes the final decision
when consensus is not reached, and records significant scope decisions in the
canonical guide or in [review decisions](docs/review-decisions.md).

Once there are several maintainers, a significant change (a new public contract,
dependency or integration, a deprecation or removal, or a change to this policy)
stays open for at least 72 hours so other maintainers can object. If they cannot
agree, a simple majority of maintainers decides and the result is recorded on the
pull request or discussion.

## Releases

Only maintainers authorize and perform releases: pushing version tags, dispatching
the chart and dbt release workflows, approving publishing environments and editing
published GitHub releases. Releases follow the [release runbook](docs/releases.md),
including signed commits, exact-commit eligibility evidence and signed annotated
tags. Anyone can prepare release material in a pull request, such as release notes
or dry-run evidence; a request to prepare a release does not authorize publishing it.

## Stepping down and inactivity

Anyone can step down at any time by opening a pull request or telling a maintainer.
They move to [Emeritus](#roster), their CODEOWNERS entries are removed and their
elevated access is reduced to match their new role.

Reviewers, area maintainers and maintainers with no reviews, triage, merges or
commits in the project for six months are asked, in an issue or by mention, whether
they want to continue. Without a reply within 30 days, or if they decline, they
move to emeritus and their elevated access is removed. Emeritus members can return
by asking a maintainer once they are active again.

Maintainers may remove access immediately for a compromised account or a code of
conduct violation, then follow the [code of conduct](CODE_OF_CONDUCT.md)
enforcement process. A sole maintainer who intends to step down announces it in
[Discussions](https://github.com/hussein-awala/spark-on-k8s/discussions) and either
nominates a successor from the area maintainers or marks the repository as
unmaintained.

## Roster

| Role | Members | Areas |
| --- | --- | --- |
| Maintainer | Hussein Awala ([@hussein-awala](https://github.com/hussein-awala)) | All |
| Area maintainer | None yet | |
| Reviewer/triager | None yet | |
| Emeritus | None | |

## Changing this policy

Propose changes in a pull request to this file. While the project has a single
maintainer, that maintainer approves changes. Afterwards, changes follow the
significant-change rule in [Decisions](#decisions).
