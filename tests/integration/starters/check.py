from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path

from pyspark.sql import SparkSession


def load_job(path: Path):
    spec = importlib.util.spec_from_file_location("starter_job", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check_etl(spark, project: Path, output: Path):
    job = load_job(project / "etl/job.py")
    expected_daily = [("2026-01-01", "FR", 2, 1750), ("2026-01-02", "US", 1, 2000)]
    for _ in range(2):
        summary = job.run(spark, str(project / "etl/orders.json"), str(output))
        assert summary == {"input_events": 9, "rejected_events": 4, "orders": 4}, summary
        daily = spark.read.parquet(str(output / "daily_revenue")).orderBy("order_date", "country").collect()
        assert [
            (str(row.order_date), row.country, row.orders, row.revenue_cents) for row in daily
        ] == expected_daily
        clean = spark.read.parquet(str(output / "clean_orders"))
        assert clean.where("order_id = 'o1'").first().amount_cents == 1250
        rejected = spark.read.parquet(str(output / "quarantine"))
        assert {row.rejection_reason for row in rejected.collect()} == {
            "invalid_amount",
            "invalid_country",
            "invalid_timestamp",
            "invalid_order_timestamp",
        }
    return {"case": "etl", "replays": 2, "daily_revenue": expected_daily}


def check_lakehouse(spark, project: Path):
    job = load_job(project / "lakehouse/job.py")
    source = project / "lakehouse"
    first = job.run(spark, str(source / "initial.json"), str(source / "updates.json"))
    assert first["orders"] == 3
    assert first["before_snapshot"] != first["after_snapshot"]
    previous = spark.sql(
        f"SELECT order_id, amount_cents FROM {job.TABLE} VERSION AS OF {first['before_snapshot']}"
    )
    assert sorted((row.order_id, row.amount_cents) for row in previous.collect()) == [
        ("o1", 1000),
        ("o2", 500),
    ]
    expected = [("o1", 1250), ("o2", 500), ("o3", 2000)]
    assert sorted((row.order_id, row.amount_cents) for row in spark.table(job.TABLE).collect()) == expected
    second = job.run(spark, str(source / "initial.json"), str(source / "updates.json"))
    assert second["orders"] == 3
    assert sorted((row.order_id, row.amount_cents) for row in spark.table(job.TABLE).collect()) == expected
    # Replaying the batch must not revert the latest value or duplicate inserted orders.
    assert spark.table(job.TABLE).select("order_id").distinct().count() == 3
    return {"case": "iceberg", "replays": 2, "orders": expected, "initial_snapshot": first["before_snapshot"]}


def main():
    parser = argparse.ArgumentParser(description="Execute and verify the starter outputs using real Spark")
    parser.add_argument("--case", choices=["etl", "iceberg"], required=True)
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--spark-version", required=True)
    args = parser.parse_args()
    args.project = args.project.resolve()
    args.work_dir = args.work_dir.resolve()
    args.work_dir.mkdir(parents=True, exist_ok=True)
    builder = SparkSession.builder.config("spark.sql.shuffle.partitions", "2")
    if args.case == "iceberg":
        builder = builder.config(
            "spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )
        builder = builder.config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        builder = builder.config("spark.sql.catalog.lakehouse.type", "hadoop")
        builder = builder.config(
            "spark.sql.catalog.lakehouse.warehouse", (args.work_dir / "warehouse").as_uri()
        )
    spark = builder.getOrCreate()
    try:
        assert spark.version == args.spark_version
        result = (
            check_etl(spark, args.project, args.work_dir / "etl")
            if args.case == "etl"
            else check_lakehouse(spark, args.project)
        )
        print("SPARK_STARTER_OK " + json.dumps({"spark": spark.version, **result}), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
