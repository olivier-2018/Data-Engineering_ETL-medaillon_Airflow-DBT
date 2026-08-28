"""Bronze -> silver for customers. Plain latest-event-wins dedup for the
profile fields - the generator's Customer._emit() always carries the
customer's *full* current snapshot (verified_account/disabled_account
included) on every event type, not just the profile fields, so a single
latest-row-wins window is sufficient even across
account_verified/account_disabled/account_enabled events.

created_at is handled separately: a customer's event_type='created' row
fires exactly once, ever, for a given customer_id (Customer.create() calls
it a single time) - so created_at is only ever populated from that one row
(left-joined in below) and passed through upsert()'s coalesce_cols, which
leaves the existing stored value untouched on every later batch that
doesn't happen to contain that customer's 'created' row."""
from __future__ import annotations

import logging

from pyspark.sql import Row
from pyspark.sql.functions import coalesce, col, min as spark_min, row_number
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

from shared_utils import (
    fetch_incremental,
    get_spark_session,
    read_partitions_per_topic,
    read_watermark,
    upsert_partition,
    write_watermark,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BRONZE_TABLE_NAME = "customer_events"  # control.silver_watermarks key
SILVER_TABLE = "silver.customers_current"

BRONZE_COLUMNS = [
    "event_id", "customer_id", "event_type", "name", "email", "address", "tel",
    "country", "city", "segment", "verified_account", "disabled_account", "event_at", "ingested_at",
]

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("name", StringType(), False),
        StructField("email", StringType(), True),
        StructField("address", StringType(), True),
        StructField("tel", StringType(), True),
        StructField("country", StringType(), False),
        StructField("city", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("verified_account", BooleanType(), True),
        StructField("disabled_account", BooleanType(), True),
        StructField("event_at", TimestampType(), False),
        StructField("ingested_at", TimestampType(), False),
    ]
)


def main() -> None:
    spark = get_spark_session("silver-customers")
    spark.sparkContext.setLogLevel("WARN")

    watermark = read_watermark(BRONZE_TABLE_NAME)
    rows = fetch_incremental(
        f"SELECT {', '.join(BRONZE_COLUMNS)} FROM iot.customer_events WHERE ingested_at > %s",
        (watermark,),
    )
    if not rows:
        logger.info("No new customer_events since %s", watermark)
        spark.stop()
        return

    df = spark.createDataFrame([Row(*r) for r in rows], schema=SCHEMA)

    latest = Window.partitionBy("customer_id").orderBy(col("event_at").desc())
    deduped = (
        df.withColumn("rn", row_number().over(latest))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    created_rows = (
        df.filter(col("event_type") == "created")
        .select("customer_id", col("event_at").alias("created_at"))
    )
    # left join: most batches won't contain a 'created' row for a given
    # customer at all (it fired in some earlier batch), so created_at is
    # simply NULL for those - see upsert()'s coalesce_cols handling.
    #
    # Fallback for a customer whose 'created' row never made it into bronze
    # at all (e.g. lost at the Kafka producer before its topic finished
    # auto-creating, on a fresh stack's very first moments): without this,
    # such a customer's first-ever appearance here has no created_at source
    # (no existing silver row to COALESCE against, no 'created' row in this
    # batch either), violating the NOT NULL constraint and permanently
    # failing this job every run (the watermark never advances past a
    # failed batch, so the same broken customer reappears in every retry
    # forever). Falls back to the earliest event_at this customer has in
    # the batch - an honest proxy, not the true creation time, but a real
    # timestamp instead of a permanent crash.
    earliest_event = df.groupBy("customer_id").agg(spark_min("event_at").alias("earliest_event_at"))
    final = (
        deduped.join(created_rows, on="customer_id", how="left")
        .join(earliest_event, on="customer_id", how="left")
        .withColumn("created_at", coalesce(col("created_at"), col("earliest_event_at")))
    )

    def _write_partition(rows_iter) -> None:
        rows = [
            (
                r.customer_id, r.name, r.email, r.address, r.tel, r.country, r.city, r.segment,
                bool(r.verified_account), bool(r.disabled_account), r.event_at, r.created_at,
            )
            for r in rows_iter
        ]
        upsert_partition(
            SILVER_TABLE,
            key_cols=["customer_id"],
            set_cols=[
                "name", "email", "address", "tel", "country", "city", "segment",
                "verified_account", "disabled_account", "updated_at",
            ],
            rows_iter=rows,
            coalesce_cols=["created_at"],
        )

    row_count = final.count()
    final.repartition(read_partitions_per_topic()).foreachPartition(_write_partition)

    new_watermark = df.agg({"ingested_at": "max"}).collect()[0][0]
    write_watermark(BRONZE_TABLE_NAME, new_watermark)

    logger.info("Upserted up to %d customer(s) into %s", row_count, SILVER_TABLE)
    spark.stop()


if __name__ == "__main__":
    main()
