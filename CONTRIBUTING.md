# Contributing Guidelines

We welcome contributions from the community and are happy to have them.
Please follow this guide when logging issues or making changes.

## Installation

To install the package, you can check [this documentation](README.md#installation).

## Testing

Use Python 3.11+ and [uv](https://docs.astral.sh/uv/) 0.13.7, matching CI.
The committed `uv.lock` separates test, lint, and documentation dependencies.
Run these commands from the repository root. `uv sync` creates an isolated `.venv`;
the `test` group is selected by default, and the commands below choose groups explicitly.

For the core client and API tests on Python 3.11–3.15:

```bash
uv sync --locked --no-default-groups --group test --extra api --extra sql
uv run --no-sync pytest --ignore=tests/airflow
```

For the complete suite with Airflow 3, use Python 3.11–3.14 with the API and native
Airflow task UI extras.
This example selects Python 3.12:

```bash
uv sync --python 3.12 --locked --no-default-groups --group test --all-extras
uv run --no-sync pytest
```

Install Helm to run the chart-rendering tests. Node.js 24+ is required when changing the UI.
CI verifies the actual interpreter and integrations, requires collected tests, and
rejects skipped tests in each profile.

For Airflow 2.11.2, create a separate, fresh environment on Python 3.11 or 3.12.
The normal lock selects Airflow 3; install the legacy version only after syncing
core/API and test dependencies without the Airflow extra:

```bash
uv venv --python 3.12 .venv-airflow211
UV_PROJECT_ENVIRONMENT=.venv-airflow211 uv sync --python 3.12 --locked --no-default-groups --group test --extra api --extra sql
uv pip install --python .venv-airflow211/bin/python '.[airflow]' 'apache-airflow==2.11.2'
.venv-airflow211/bin/python -c 'import airflow; assert airflow.__version__ == "2.11.2"'
.venv-airflow211/bin/python -m pytest --ignore=tests/airflow/ui
uv pip check --python .venv-airflow211/bin/python
```

Use that environment's Python directly for subsequent legacy tests. A normal
`uv run` or `uv sync --extra airflow` can replace the legacy selection with the
locked Airflow 3 dependencies. Do not downgrade an existing Airflow 3 environment:
its separate core and task-SDK packages can remain installed. The Airflow 2 profile
explicitly excludes `tests/airflow/ui`, which requires Airflow 3.4; all remaining
tests must execute without skips.

To run code checks and build documentation:

```bash
uv sync --python 3.12 --locked --no-default-groups --group lint --group docs --all-extras
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync sqlfluff lint examples/airflow
uv run --no-sync helm-mkdocs chart -o docs/helm-chart
uv run --no-sync mkdocs build --strict
uv build
```

CI installs the built wheel for its Airflow checks, verifies provider discovery,
and exercises operators and Spark UI links under Airflow 3.4.0 and 2.11.2. Its Airflow 3
profile installs only the `airflow-ui` extra, independently of the standalone `api`
extra, and includes task authorization, binding, history and log-stream tests.
Keep `uv.lock` committed and use `uv lock --check` to check it without changing it.

## Web console development

The standalone console and Airflow task tab share the React components in
`frontend/src`. Their entry points supply different transports and host context;
keep UI behavior in the shared components rather than adding a separate renderer.
The Python package includes the built JavaScript, CSS, and third-party license notices;
users installing a wheel or source distribution do not need Node.js or a frontend CDN.

Use Node.js 26 (matching CI) and the committed npm lockfile:

```bash
cd frontend
npm ci
npm run build
npx playwright install chromium
npm test
```

`npm run build` checks TypeScript and regenerates both
`spark_on_k8s/api/webserver/static/ui` and `spark_on_k8s/airflow/ui/static`.
Commit both sets of generated files with source changes. CI rebuilds them and rejects
stale or untracked assets. The same `npm test` command runs standalone and Airflow
plugin browser tests, including navigation, actions, filtering, errors, and safe log
rendering. `npm run dev` serves the development fixtures; `npm run test:ui` opens the
interactive test runner. The fixtures use mocked services and are not packaged in the wheel.

To test the packaged application against a Kubernetes environment, start the API as
documented in the README and open `/webserver/apps`. Keep API deployment prefixes in
mind: browser URLs are derived from the server-provided base path, never a fixed host.

## Reporting a bug

If you find a bug in this project, you can report it by opening an issue via this
[link](https://github.com/hussein-awala/spark-on-k8s/issues/new) and providing all the
information can help reproduce it.
If you already have a fix for the bug, you can skip creating an issue and submit a PR to fix it.

**Warning**: if the bug is a security vulnerability, please follow
[this documentation](SECURITY.md#reporting-a-vulnerability) to report it.

## Questions

If you have any question, a new idea, or anything to discuss, you can start a new discussion
via this [link](https://github.com/hussein-awala/spark-on-k8s/discussions/new/choose).
