"""Bronze -> silver, customers: incremental read by ingested_at, dedupe to
latest-per-customer_id via a Spark window function, upsert into
silver.customers_current (feeds the dbt customer_snapshot SCD2)."""
from __future__ import annotations

import logging

from pyspark.sql import Row
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

from shared_utils import fetch_incremental, get_spark_session, read_watermark, upsert, write_watermark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WATERMARK_KEY = "customer_events"
COLUMNS = ["event_id", "customer_id", "event_type", "name", "country", "city", "segment", "event_at", "ingested_at"]


def main() -> None:
    spark = get_spark_session("customers-to-silver")
    spark.sparkContext.setLogLevel("WARN")

    watermark = read_watermark(WATERMARK_KEY)
    rows = fetch_incremental(
        f"SELECT {', '.join(COLUMNS)} FROM iot.customer_events WHERE ingested_at > %s ORDER BY ingested_at",
        (watermark,),
    )
    if not rows:
        logger.info("No new customer_events since %s", watermark)
        spark.stop()
        return

    df = spark.createDataFrame([Row(**dict(zip(COLUMNS, r))) for r in rows])

    window = Window.partitionBy("customer_id").orderBy(col("event_at").desc())
    latest = (
        df.withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    upsert_rows = [
        (r["customer_id"], r["name"], r["country"], r["city"], r["segment"], r["event_at"])
        for r in latest.collect()
    ]
    upsert(
        "silver.customers_current",
        key_cols=["customer_id"],
        set_cols=["name", "country", "city", "segment", "updated_at"],
        rows=upsert_rows,
    )

    new_watermark = max(r[COLUMNS.index("ingested_at")] for r in rows)
    write_watermark(WATERMARK_KEY, new_watermark)
    logger.info("Upserted %d customer(s), watermark -> %s", len(upsert_rows), new_watermark)
    spark.stop()


if __name__ == "__main__":
    main()
