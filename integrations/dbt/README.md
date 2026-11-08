# Experimental dbt adapter

Run dbt Core SQL workloads on invocation-scoped Spark compute, using Thrift or
Spark Connect. `spark-on-k8s-dbt run -- build` creates the server lazily, shares
it across dbt threads, and deletes owned compute when the command finishes.

This is an opt-in, separately installed experimental distribution. Its API and
profile format may change. See the repository's
[managed dbt guide](../../docs/user-guide/dbt.md) for installation, profiles,
recovery and support limits. This distribution is not published by the main
package release workflow.
