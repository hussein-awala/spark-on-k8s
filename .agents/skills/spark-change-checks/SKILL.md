---
name: spark-change-checks
description: Select and run spark-on-k8s validation for an implementation, packaging or documentation change and prepare truthful pre-PR evidence. Includes installed-wheel, Airflow and real Kubernetes test routing.
---

# Validate a change

Read root `AGENTS.md`, inspect the actual diff, and use
[development guidance](../../../docs/development.md#choose-checks-by-change) as the
canonical command source. Read workflow files for affected matrices; do not assume
versions in older release notes are current tooling requirements.

1. Select a focused regression check for the changed behavior. Run the relevant
   broader profile once after iteration. Do not repeat every successful check
   without a relevant change or unresolved concern.
2. Use separate locked uv environments. For Airflow 2.11, begin without Airflow 3
   packages, install the legacy override, then use that environment's Python
   directly. Never downgrade/reuse the Airflow 3 environment.
3. For packaging, imports or Airflow behavior, build and install the actual wheel,
   then use `python -I tests/run_installed_tests.py`. Check the interpreter and
   selected Airflow version. Exercise provider discovery and the native Airflow 3
   UI through its `airflow-ui` extra independently of the standalone API extra.
4. Lifecycle changes need the applicable real Kubernetes harness in
   `tests/integration/spark/` or `tests/integration/airflow/`, via its disposable
   kind workflow. Check each required scenario, saved evidence and cleanup.
   Local Spark execution and mock-based pytest are separate evidence.
5. UI changes rebuild both packaged static trees and run browser tests. Documentation
   changes run strict docs; prose alone does not require the runtime matrix.
   Include lock and build checks for dependency/packaging changes.

If local Docker, tooling or a supported interpreter is unavailable, complete the
unaffected checks and identify the exact CI job still required. Do not manufacture
a passing result, weaken scenario assertions, or add skips solely to green CI.
Before a PR, verify that relevant required checks actually executed on the current
commit; pending, cancelled and skipped are not passing runtime evidence.

Review the final diff, signatures and generated assets. Report behavior tested,
commands/profile, result and any pending integration evidence. Sign commits with
the contributor's configured identity when committing is authorized. Creating this
validation report does not itself authorize pushing, merging or publishing.
