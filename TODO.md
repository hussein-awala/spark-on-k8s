# Candidate contributions

This list records uncommitted opportunities, not promised releases. Before starting
substantial work, name the user outcome, excluded scope, checks and review owner.
Use [CONTRIBUTING.md](CONTRIBUTING.md) and the
[support boundaries](docs/user-guide/support.md). Closed feature names are not
backlog items: the shared console, remote submission, diagnostics, job files,
SQL/Connect clients and real Spark runtime checks already exist.

| Candidate | Small first slice | Acceptance evidence |
| --- | --- | --- |
| Reduce setup ambiguity | Improve one documented route based on a reproducible point of confusion | The route names prerequisites, a current command and an observable result; strict docs build passes |
| Improve error explanation | Add one missing actionable message or diagnostic scenario | Regression case preserves uncertainty and avoids leaking secrets |
| Fill a runtime coverage gap | Identify one uncovered lifecycle failure in an existing harness | Bounded real-runtime scenario with saved evidence and cleanup; no duplicate matrix |
| Qualify a requested integration | One recipe reusing the SDK/CLI and existing profiles | Named maintainer, explicit versions and a repeatable output check |
| Move the API image to Python 3.15 | Change the `python:3.14-slim` base of `docker/Dockerfile` and `tests/integration/chart/Dockerfile` to `python:3.15-slim` by digest once PyYAML publishes cp315 manylinux wheels; 6.0.3 has none and the image installs wheels only | `uv pip compile` of the `api` export for Python 3.15 on x86_64 and aarch64 with `--only-binary :all:` succeeds; the image scan clears the interpreter alerts fixed only in 3.15 |
| Modernize dependency scanning | Reconcile the external Snyk project with uv extras/groups | Scanner sees the intended runtime/build profiles; access and external changes explicitly authorized |

## Two bounded first-contribution candidates

These candidates are unassigned. In an issue or draft PR, link the relevant guide
and ask the project maintainer to confirm a reviewer and the small scope before
starting. They are not advertised as assigned or guaranteed review slots.

1. **Annotate one incomplete diagnostic report.** Add a sanitized JSON example to
   `docs/user-guide/diagnostics.md` using the current `Diagnosis.to_dict()` fields.
   Show an available finding alongside a denied or bounded collection; explain why
   `incomplete` and an empty findings list cannot establish health. Acceptance:
   field names match the serializer, no sensitive logs/configuration are included,
   the next read-only inspection is clear, and strict docs build passes. No new
   classifier, UI or live-cluster fixture is in scope.
2. **Explain a notebook session across two cells.** Extend
   `docs/user-guide/spark_connect.md` using the existing session API: show one
   session reused for two small queries and explicit cleanup when exploration ends.
   Acceptance: the example names an existing endpoint and installation extra,
   demonstrates cleanup on error, and states the manual validation performed or
   still needed. Reuse the existing Connect check; no server provisioning,
   authentication subsystem or new notebook dependency is in scope.

The [scheduled Airflow template example](docs/user-guide/airflow.md) is already
maintained with the Airflow integration; improve that shared example instead of
starting another scheduling tutorial.

Generic promises such as "all client features on every interface" or "add more
examples" do not define an implementable task. Confirm whether an existing guide,
configuration escape hatch or shared helper already addresses a request. Prefer a
small supported path over another controller, scheduler or platform subsystem.
