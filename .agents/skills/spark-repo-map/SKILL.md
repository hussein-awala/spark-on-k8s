---
name: spark-repo-map
description: Locate the implementation, contracts, tests and canonical docs for a spark-on-k8s change or repository onboarding. Use for cross-surface planning; small edits with known ownership can proceed directly.
---

# Map a repository change

Read root `AGENTS.md` and the structure table in
[development guidance](../../../docs/development.md#structure-and-contracts).
Map the requested user outcome to the smallest existing implementation boundary,
its tests and its canonical guide before proposing a new subsystem.

- Submission/lifecycle belongs in the SDK and shared managers; CLI and Airflow
  adapt that behavior. Do not silently change each adapter's distinct cleanup policy.
- Job loading/planning is offline; remote policy is an admission boundary, not a
  scheduler or tenancy system. Preserve that separation.
- Standalone and Airflow browser entry points share `frontend/src/` components;
  their transports, authentication and task binding remain host-specific.
- Metadata history is optional observation. Do not use its SQLite store to claim
  transactional submission, authoritative completion or exactly-once data writes.
- Examples and compatibility workflows are evidence for named runtime combinations,
  not blanket guarantees for every cloud, catalog or scheduler.

Inspect the relevant existing implementation and tests. Check
[support boundaries](../../../docs/user-guide/support.md) for documented imports,
formats and deprecation obligations. Reuse a canonical guide instead of copying
commands into another README. For newcomer work, extend
[Start here](../../../docs/user-guide/start-here.md) and keep actual commands in
existing task guides.

State the implementation paths, affected public behavior and smallest useful
validation route. Read the change-checks skill only when selecting/running checks;
read the release skill only for release work. Do not turn repository orientation
into an unrequested audit or feature roadmap.
