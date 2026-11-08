from __future__ import annotations

import argparse
import json

from pyspark.sql import SparkSession, Window, functions as F

TABLE = "lakehouse.starter.orders"
SCHEMA = "order_id STRING, country STRING, amount_cents LONG, updated_at STRING"


def read_events(spark, path: str):
    events = (
        spark.read.schema(SCHEMA)
        .json(path)
        .withColumn("country", F.upper(F.trim("country")))
        .withColumn("event_time", F.try_to_timestamp("updated_at"))
    )
    invalid = events.where(
        F.col("order_id").isNull()
        | (F.trim("order_id") == "")
        | F.col("country").isNull()
        | ~F.col("country").rlike("^[A-Z]{2}$")
        | F.col("amount_cents").isNull()
        | (F.col("amount_cents") < 0)
        | F.col("event_time").isNull()
    )
    if invalid.limit(1).count():
        raise ValueError("Lakehouse input contains an invalid order event")
    window = Window.partitionBy("order_id").orderBy(
        F.col("event_time").desc(), F.col("amount_cents").desc(), F.col("country").desc()
    )
    return (
        events.withColumn("rank", F.row_number().over(window))
        .where("rank = 1")
        .select("order_id", "country", "amount_cents", "event_time")
    )


def current_snapshot(spark) -> int:
    return spark.table(TABLE + ".history").orderBy(F.col("made_current_at").desc()).first()["snapshot_id"]


def run(spark, initial_path: str, updates_path: str) -> dict[str, int]:
    """Bootstrap the demo table once and merge a replayable batch of order updates."""
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    initial = read_events(spark, initial_path)
    updates = read_events(spark, updates_path)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.starter")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {TABLE} "
        "(order_id STRING, country STRING, amount_cents BIGINT, event_time TIMESTAMP) "
        "USING iceberg TBLPROPERTIES ('format-version'='2')"
    )
    if not spark.table(TABLE).limit(1).count():
        if not initial.limit(1).count():
            raise ValueError("The initial demo dataset must contain at least one order")
        initial.writeTo(TABLE).append()
    before = current_snapshot(spark)
    updates.createOrReplaceTempView("incoming_orders")
    spark.sql(f"""
        MERGE INTO {TABLE} AS target USING incoming_orders AS source
        ON target.order_id = source.order_id
        WHEN MATCHED AND source.event_time > target.event_time THEN
          UPDATE SET country = source.country, amount_cents = source.amount_cents,
                     event_time = source.event_time
        WHEN NOT MATCHED THEN INSERT (order_id, country, amount_cents, event_time)
          VALUES (source.order_id, source.country, source.amount_cents, source.event_time)
    """).collect()
    return {
        "before_snapshot": before,
        "after_snapshot": current_snapshot(spark),
        "orders": spark.table(TABLE).count(),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge a replayable batch into an Iceberg order table")
    parser.add_argument(
        "--initial", required=True, help="Initial demo JSON; inserted only if the table is empty"
    )
    parser.add_argument("--updates", required=True, help="Batch JSON accessible to driver and executors")
    args = parser.parse_args()
    session = SparkSession.builder.appName("orders-lakehouse").getOrCreate()
    try:
        print(json.dumps(run(session, args.initial, args.updates)), flush=True)
    finally:
        session.stop()
