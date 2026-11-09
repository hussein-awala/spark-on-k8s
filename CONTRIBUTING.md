# Contributing

Start with one user-visible outcome and a bounded change. Small bug fixes,
documentation corrections and reproducible example improvements are welcome.
For a feature that adds a dependency, public API or integration, first explain the
workflow it serves, its non-goals and who will maintain its checks. Existing user
authorization to implement a change does not require another discussion first.

[Development guidance](docs/development.md) is the canonical repository map,
environment setup and test-routing guide. [Support boundaries](docs/user-guide/support.md)
explain compatibility and deprecation expectations. Maintainers preparing a release
use the [release runbook](docs/releases.md). Agents also read [AGENTS.md](AGENTS.md)
and the relevant repository-local skill under `.agents/skills/`.

## Choose a first contribution

| Change | Start here | Evidence for review |
| --- | --- | --- |
| Clarify a user path | `docs/user-guide/`, `docs/index.md` | Correct links, strict docs build, clear prerequisite and success checkpoint |
| Correct Python/CLI behavior | `spark_on_k8s/`, matching `tests/` | A focused regression case and the affected profile |
| Improve the console | `frontend/src/`, `frontend/tests/` | Both generated asset sets, browser test and relevant backend checks |
| Improve an example | `examples/`, `tests/integration/` | Actual output assertions for the documented runtime; distinguish local Spark from Kubernetes execution |

A useful issue or PR states: user outcome, current evidence, excluded scope,
implementation/test locations, and acceptance criteria. Select a small candidate
from [the opportunities list](TODO.md); that list is not a release promise or an
assignment. Agree a reviewer for larger contributions before expanding their scope.

## Before a pull request

1. Follow the [check route for the files changed](docs/development.md#choose-checks-by-change).
   Run focused checks during iteration, then the relevant broader profile once.
2. Update the canonical guide for changed public behavior, including migration
   notes for compatibility changes. Keep source-only instructions distinct from
   published releases.
3. Include generated UI assets and intentional lockfile updates. Review the full
   diff for unrelated files and sensitive data.
4. Sign your commits with `git commit -S` using your own configured identity.
   Do not bypass failed signing. A `Signed-off-by` line alone is not a signature.
5. Describe the resulting behavior, tests actually run, and any unavailable runtime
   check. Do not present queued or skipped CI as passing. Use a focused PR title;
   link the relevant issue or discussion when one exists.

## Bugs, questions and security reports

[Report a bug](https://github.com/hussein-awala/spark-on-k8s/issues/new) with package
version or commit, Python/Spark/Airflow versions where relevant, a small reproduction,
expected versus actual result, and sanitized diagnostics. Exclude credentials,
private kubeconfig contents and sensitive application logs. A fix can go straight
to a PR without a separate issue.

Use [Discussions](https://github.com/hussein-awala/spark-on-k8s/discussions/new/choose)
for usage questions or uncommitted ideas. For vulnerabilities, use
[the private reporting policy](SECURITY.md#reporting-a-vulnerability).
Community review is best effort; no response-time or long-term support SLA is offered.

## Roles, review owners and adopters

[GOVERNANCE.md](GOVERNANCE.md) describes the contributor, reviewer, area maintainer
and maintainer roles, how decisions and releases are made, and how to move between
roles. [CODEOWNERS](.github/CODEOWNERS) lists the review owners for each area. If
you use spark-on-k8s, consider adding your organization to [ADOPTERS.md](ADOPTERS.md).
