# Development and validation

This is the canonical check guide for contributors and repository agents. Commands
run from the repository root unless stated otherwise. Workflow files define the
current CI matrices; adjust this guide when those files change.

## Structure and contracts

| Area | Implementation | Checks and docs |
| --- | --- | --- |
| Submission and lifecycle | `client.py`, `utils/`, `k8s/` under `spark_on_k8s/` | `tests/test_spark_client.py`, `tests/utils/`, `tests/k8s/`, `tests/integration/spark/` |
| Documents and plans | `jobs.py`, `planning.py`, `cli/job.py` | `tests/test_jobs.py`, `tests/cli/`, [job files](user-guide/jobs.md) |
| CLI and SQL | `cli/`, `sql/` | `tests/cli/`, `tests/sql/`, [automation](user-guide/automation.md), [SQL](user-guide/sql.md) |
| API, policy and proxies | `api/`, `remote.py` | `tests/api/`, [remote submission](user-guide/remote-submission.md) |
| Airflow | `airflow/` | `tests/airflow/`, [Airflow](user-guide/airflow.md) |
| Shared console | `frontend/src/` | `frontend/tests/`; both Python static asset trees |
| Diagnostics and recorder | `diagnostics.py`, `doctor.py`, `history/` | `tests/test_diagnostics.py`, `tests/history/`, [diagnostics](user-guide/diagnostics.md), [history](user-guide/run-history.md) |
| Packaging and deployment | `pyproject.toml`, `uv.lock`, `docker/`, `chart/` | `tests/chart/`, built wheel checks, [releases](releases.md) |

Keep UI behavior in shared components; host entry points only supply transports and
context. Preserve machine-readable schemas and exit codes separately from human
messages. Treat active, absent, terminal and unknown outcomes as different states.
See [support boundaries](user-guide/support.md) before adding a public contract.

## Choose checks by change

| Touched behavior | Minimum relevant route before PR |
| --- | --- |
| Documentation prose, links or agent instructions | Strict docs build; validate edited skills; no runtime matrix solely for prose |
| Core, CLI, SQL or offline planning | Focused regression tests, core/API profile and Ruff; installed wheel if packaging/import behavior changes |
| Submission, wait, retry, cancel or cleanup | Relevant unit profiles plus real Spark Kubernetes workflow; Airflow Kubernetes workflow for operator/trigger lifecycle changes |
| Airflow or provider packaging | Installed-wheel Airflow 3 and 2.11 profiles; native task UI backend checks on Airflow 3 |
| API/remote/proxy behavior | `tests/api/` and relevant CLI tests; browser checks when browser transports or URLs change |
| Frontend | Format, build, both packaged asset trees and browser tests; relevant API/Airflow backend tests |
| Helm | `tests/chart/` with Helm installed; regenerate chart documentation and strict docs build |
| Dependencies/build/release | Lock check, applicable installed-wheel profiles, package build and affected runtime workflows |
| Connect | `tests/test_connect.py` plus real Connect workflow for client/protocol changes |
| Starter/example runtime | Its output assertions in the advertised runtime; extend the corresponding workflow when support claims change |

Do not run every expensive matrix locally for a small change. Do not omit a
relevant runtime requirement because its unit tests pass. Use CI for unavailable
Docker, Kubernetes or alternate interpreters and record the pending check. Once
checks pass, repeat them only after relevant changes or newly discovered risks.

## Python profiles

Use Python 3.11+ and `uv` 0.13.7, matching CI. Keep `uv.lock` committed. Profiles
below use separate environments so concurrent checks do not replace each other's
packages. The test group is otherwise selected by default.

Core/API (CI covers Python 3.11–3.15, plus selected macOS ARM64 combinations):

```bash
UV_PROJECT_ENVIRONMENT=.venv-core uv sync --python 3.12 --locked --no-default-groups --group test --extra api --extra sql
.venv-core/bin/python -m pytest tests --ignore=tests/airflow
```

Pass a test path or `-k` expression for the initial focused run. Install Helm for
chart tests. The CI profile requires collected tests and rejects skips; unavailable
tools are not a passing result.

Airflow 3 (CI selects 3.4.0 on Python 3.11–3.14; use only `airflow-ui` so the
standalone API extra cannot hide a missing native-UI dependency):

```bash
UV_PROJECT_ENVIRONMENT=.venv-airflow3 uv sync --python 3.12 --locked --no-default-groups --group test --extra airflow-ui
uv build --wheel
uv pip install --python .venv-airflow3/bin/python --no-deps --reinstall dist/*.whl
.venv-airflow3/bin/python -I tests/run_installed_tests.py tests/airflow
uv pip check --python .venv-airflow3/bin/python
```

Airflow 2.11.2 uses a **fresh separate environment** on Python 3.11 or 3.12 (the
CI legacy row uses 3.12). Never downgrade an Airflow 3 environment: its separate
core/task-SDK packages can remain installed.

```bash
UV_PROJECT_ENVIRONMENT=.venv-airflow211 uv sync --python 3.12 --locked --no-default-groups --group test
uv pip install --python .venv-airflow211/bin/python '.[airflow]' 'apache-airflow==2.11.2'
.venv-airflow211/bin/python -c 'import airflow; assert airflow.__version__ == "2.11.2"'
uv build --wheel
uv pip install --python .venv-airflow211/bin/python --no-deps --reinstall dist/*.whl
.venv-airflow211/bin/python -I tests/run_installed_tests.py tests/airflow --ignore=tests/airflow/ui
uv pip check --python .venv-airflow211/bin/python
```

Use the environment's Python directly after installing the wheel or legacy
override: `uv run`/`uv sync` can restore the editable project or locked Airflow 3.
Exclude only the native UI suite from Airflow 2; remaining profile tests must run
without skips. CI also verifies interpreter versions, provider discovery, lazy
plugin registration, packaged assets and absence of unintended API imports.

For core packaging changes, build and install the wheel into `.venv-core` with
`uv pip install --python .venv-core/bin/python --no-deps --reinstall dist/*.whl`,
then run `.venv-core/bin/python -I tests/run_installed_tests.py tests --ignore=tests/airflow`.
The runner rejects editable installations and imports outside the environment.
Use a fresh build output directory or remove only known stale build outputs before
using `dist/*.whl`; it must identify exactly the wheel under review.

## Formatting, packaging and documentation

```bash
UV_PROJECT_ENVIRONMENT=.venv-quality uv sync --python 3.12 --locked --no-default-groups --group lint --group docs --all-extras
.venv-quality/bin/ruff check .
.venv-quality/bin/ruff format --check .
.venv-quality/bin/sqlfluff lint examples/airflow
uv lock --check
uv build
.venv-quality/bin/helm-mkdocs chart -o docs/helm-chart
.venv-quality/bin/mkdocs build --strict
```

For prose-only edits, the last two commands are sufficient once the documentation
environment exists. The full site generates Python API reference pages, so its
build environment needs the documented extras even when the prose itself does not
exercise them. Generated chart docs and `site/` are ignored outputs; do not commit
them. Keep instructions canonical and link them from the README rather than
maintaining additional command copies.

Repository skills are small Markdown packages. If the installed skill-creator
validator is available, run its `scripts/quick_validate.py` against each edited
folder in `.agents/skills/`. Also read every relative reference and dry-run a
representative task without publication: syntactic validation does not establish
that a release skill respects authorization or selects the correct tests.

## Shared frontend

Use Node.js 26 and the committed npm lockfile. From `frontend/`:

```bash
npm ci
npm run format:check
npm run build
npx playwright install chromium
npm test
```

`npm run build` checks TypeScript and regenerates both
`spark_on_k8s/api/webserver/static/ui` and `spark_on_k8s/airflow/ui/static`.
Commit both asset sets and license notices with source changes. CI rebuilds and
rejects stale or untracked assets. Fixtures use mocked services and are not
packaged in the wheel. A passing fixture test is not a browser-to-cluster test.
`npm run dev` serves fixtures; `npm run test:ui` opens the interactive test runner.
For the packaged app, start the API as documented and open `/webserver/apps`,
respecting its configured URL prefix.

## Real runtime checks

Use only a disposable, explicitly selected kind cluster. The workflow setup pins
images/tooling, installs a built wheel, creates its own test resources, applies
bounded deadlines and uploads diagnostics before cleanup. Never point these tests
at an arbitrary current kubeconfig context.

- `.github/workflows/spark-compatibility.yml` runs
  `tests/integration/spark/run_kubernetes.py`. Use its image-building and kind setup
  steps, then pass `--kubeconfig`, `--context`, `--image`, `--spark-version` and
  `--artifacts` as specified by the runner. It covers real Python, SQL, failure and
  Java submissions, executor ownership and resource cleanup. Changes to lifecycle
  behavior extend this harness instead of adding a second client harness.
- `.github/workflows/airflow-kubernetes.yml` runs
  `tests/integration/airflow/run_kubernetes.py` against Airflow 2.11.2 and 3.4.0
  on Python 3.12 with one representative Spark 4.0.5 image. Six cases cover success,
  executor-backed application failure and Pending/Unschedulable startup timeout,
  each synchronously and with real deferred-task resumption. The runner uses
  `DAG.test()` in a Kubernetes pod with real database/XCom/inline triggers; this
  does not exercise a deployed scheduler, separate triggerer process, worker crash
  or durable reattachment after XCom clearing. Retained-XCom adoption failures
  remain covered by operator regression tests.
- `.github/workflows/spark-connect.yml` runs `tests/integration/connect/run.py`
  against a separate real Spark Connect server. This validates a client connection,
  not a Kubernetes Connect-server manager.
- `tests/integration/starters/check.py` verifies ETL/Iceberg output and replay in
  the selected Spark image. Its container-local results do not validate shared
  storage provisioning on a target cluster.

After the Airflow workflow's wheel/image build and disposable-cluster setup, its
host command is:

```bash
.venv/bin/python -I tests/integration/airflow/run_kubernetes.py \
  --kubeconfig "$RUNNER_TEMP/airflow-kubeconfig" --context kind-airflow-compat \
  --airflow-image "airflow-compat:$AIRFLOW_VERSION" --airflow-version "$AIRFLOW_VERSION" \
  --spark-image airflow-spark:4.0.5 --spark-version 4.0.5 \
  --artifacts "$RUNNER_TEMP/airflow-kubernetes-results"
```

A valid result includes `result.json` with all six cases passed, the Airflow log,
Kubernetes pod/event/log evidence and kind logs. See the
[harness boundary](https://github.com/hussein-awala/spark-on-k8s/blob/main/tests/integration/airflow/README.md)
before claiming broader scheduler coverage.

Release helpers have focused offline tests in `tests/release/`:
`python -m pytest tests/release`. Packaging-related PRs also build a nonpublishing
preview in `.github/workflows/release.yml`; actual release eligibility is a separate
exact-commit gate. See the [dry-run procedure](releases.md#implementation-and-dry-run).

The [Spark compatibility guide](user-guide/spark_compatibility.md) describes the
selected version matrix and local Spark fallback. Local mode is useful additional
evidence but does not validate Kubernetes RBAC, pod networking or garbage collection.
Record workflow URL, tested commit, scenario results and any omitted environment
in PR/release validation. A failed or unexecuted scenario is not coverage.
