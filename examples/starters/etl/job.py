from __future__ import annotations

import argparse
import json

from pyspark.sql import SparkSession, Window, functions as F

SCHEMA = (
    "order_id STRING, country STRING, amount_cents LONG, ordered_at STRING, updated_at STRING, status STRING"
)


def run(spark, input_path: str, output_path: str) -> dict[str, int]:
    """Validate order events, select the latest event, and replace derived datasets."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    source = (
        spark.read.schema(SCHEMA)
        .json(input_path)
        .withColumn("country", F.upper(F.trim("country")))
        .withColumn("event_time", F.try_to_timestamp("updated_at"))
        .withColumn("order_time", F.try_to_timestamp("ordered_at"))
    )
    checked = source.withColumn(
        "rejection_reason",
        F.when(F.col("order_id").isNull() | (F.trim("order_id") == ""), "missing_order_id")
        .when(F.col("country").isNull() | ~F.col("country").rlike("^[A-Z]{2}$"), "invalid_country")
        .when(F.col("amount_cents").isNull() | (F.col("amount_cents") < 0), "invalid_amount")
        .when(F.col("event_time").isNull(), "invalid_timestamp")
        .when(F.col("order_time").isNull(), "invalid_order_timestamp")
        .when(F.col("status").isNull() | ~F.col("status").isin("paid", "cancelled"), "invalid_status"),
    ).cache()
    try:
        quarantine = checked.where("rejection_reason IS NOT NULL")
        valid = checked.where("rejection_reason IS NULL").drop("rejection_reason")
        # A stable tie-break makes a replay deterministic. Real systems should prefer
        # an authoritative event sequence when two events have the same timestamp.
        tie_break = F.sha2(F.to_json(F.struct(*[F.col(name) for name in sorted(valid.columns)])), 256)
        order = Window.partitionBy("order_id").orderBy(F.col("event_time").desc(), tie_break.desc())
        clean = valid.withColumn("rank", F.row_number().over(order)).where("rank = 1").drop("rank")
        clean = clean.withColumn("order_date", F.to_date("order_time"))
        daily = (
            clean.where("status = 'paid'")
            .groupBy("order_date", "country")
            .agg(F.count("*").alias("orders"), F.sum("amount_cents").alias("revenue_cents"))
        )
        summary = {
            "input_events": checked.count(),
            "rejected_events": quarantine.count(),
            "orders": clean.count(),
        }
        # Use a dedicated output prefix. Each derived dataset is replaced on replay.
        clean.write.mode("overwrite").parquet(output_path.rstrip("/") + "/clean_orders")
        quarantine.write.mode("overwrite").parquet(output_path.rstrip("/") + "/quarantine")
        daily.write.mode("overwrite").parquet(output_path.rstrip("/") + "/daily_revenue")
        return summary
    finally:
        checked.unpersist()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build validated order and daily revenue datasets")
    parser.add_argument("--input", required=True, help="JSON path accessible to driver and executors")
    parser.add_argument(
        "--output", required=True, help="Dedicated shared output prefix; derived datasets are overwritten"
    )
    args = parser.parse_args()
    session = SparkSession.builder.appName("orders-etl").getOrCreate()
    try:
        print(json.dumps(run(session, args.input, args.output)), flush=True)
    finally:
        session.stop()
