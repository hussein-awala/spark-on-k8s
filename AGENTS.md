# Repository guidance

Read this file before changing spark-on-k8s. Scope changes to the user's request.
Preserve unrelated work in this shared checkout.

## Find the right boundary

- `spark_on_k8s/client.py`, `utils/`, and `k8s/`: driver submission, lifecycle,
  configuration and synchronous/asynchronous Kubernetes access.
- `jobs.py` and `planning.py`: declarative documents and offline plans. Planning
  must not create cluster resources.
- `cli/`: Click commands and versioned automation contracts. Reuse
  `cli/automation.py`; keep driver logs/progress off JSON stdout.
- `api/`: FastAPI routes, remote submission policy, proxying and standalone UI
  hosting. `airflow/`: provider, operators, triggers and native task UI backend.
- `frontend/src/`: shared React console; standalone and Airflow entry points
  supply transports and context. Do not create another renderer.
- `history/`: optional metadata recorder. Preserve its single-replica SQLite and
  metadata-only boundaries; it is not submission authority or Spark event storage.
- `tests/`: mirrors these boundaries. `tests/integration/`: real runtime checks.
  `examples/`: application artifacts; `chart/`: API and history deployments.
- `docs/`: canonical user/developer guides; `CONTRIBUTING.md`: contributor entry
  point. `.github/workflows/`: authoritative automation and version matrices.

Use [development guidance](docs/development.md) for test routing and
[support boundaries](docs/user-guide/support.md) for compatibility decisions.

## Repository-local skills

Use the relevant skill when its task applies; ordinary small edits do not require
loading every skill:

- `.agents/skills/spark-repo-map/SKILL.md`: locate implementation, tests and docs
  before a cross-surface change or repository onboarding.
- `.agents/skills/spark-change-checks/SKILL.md`: select and run validation, including
  pre-PR evidence and installed-wheel checks.
- `.agents/skills/spark-release/SKILL.md`: prepare or carry out an explicitly
  requested release using verified historical practice and current safeguards.

## Change and validation rules

Use the locked `uv` profiles and isolated environments in `docs/development.md`.
Do not downgrade an Airflow 3 environment to test Airflow 2.11. Run changed-area
checks first, then the affected profiles once; a documentation-only change does
not require the runtime matrix. Packaging and Airflow changes require installed
wheel tests using `python -I tests/run_installed_tests.py`, not only checkout tests.
Runtime lifecycle changes also require the relevant disposable Kubernetes checks;
local Spark or mocks alone are not Kubernetes execution evidence. Report unavailable
infrastructure and rely on the matching CI job rather than silently skipping it.

Build both packaged UI asset trees from `frontend/` when changing shared UI.
Keep `uv.lock`, the npm lock and pinned workflow dependencies consistent with
intentional dependency edits. Do not add dependencies for conveniences the current
standard library or shared helpers already cover.

Before a PR, review the complete diff, run applicable checks, and describe behavior,
validation and any unexecuted integration requirement. Preserve explicit contracts:
uncertain status is not absence; a submission receipt is not data success; timeout
is not implicit cancellation; a shared operator token is not tenant isolation.

## Commits and releases

Sign commits with the contributor's configured signing identity (`git commit -S`).
A sign-off trailer is not a signature. If signing fails, preserve the changes and
report the failure; never disable signing or substitute another identity to proceed.
Verify the signature with the configured trust setup before pushing. An unavailable
local verification key is an unresolved verification result, not proof of tampering.

Commit, push, merge, tag, publish or post a discussion only within the user's
existing authorization. A request to prepare release materials does not authorize
publication. Tag pushes trigger publication workflows; follow
[the release runbook](docs/releases.md) before creating or pushing a release tag.
Do not invent a version from the development version in `pyproject.toml`.
