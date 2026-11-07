# ETL and Iceberg starters

These small projects include input data, repeatable transformations, application
images, and portable Kubernetes job definitions. See the
[starter guide](../../docs/user-guide/starters.md) for build, local verification,
shared-storage requirements, and submission instructions.

- `etl/`: validate and deduplicate JSON order events, quarantine invalid records,
  and write Parquet orders and daily revenue totals.
- `lakehouse/`: merge event updates into an Iceberg table, replay the batch without
  duplicating orders, and inspect an earlier snapshot.

The real execution checks are in `tests/integration/starters/check.py`. ETL is
checked with Spark 3.5.9, 4.0.5, and 4.3.0. Iceberg uses its published Spark 4.0
connector: Spark 4.0.5, Scala 2.13, and Iceberg 1.12.0.
