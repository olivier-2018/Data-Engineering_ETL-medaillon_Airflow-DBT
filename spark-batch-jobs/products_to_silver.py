"""Bronze -> silver, products: same shape as customers_to_silver.py (feeds
the dbt product_snapshot SCD2)."""
from __future__ import annotations

import logging

from pyspark.sql import Row
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

from shared_utils import fetch_incremental, get_spark_session, read_watermark, upsert, write_watermark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WATERMARK_KEY = "product_events"
COLUMNS = [
    "event_id", "product_id", "event_type", "name", "category", "subcategory",
    "unit_price", "weight_kg", "initial_stock", "event_at", "ingested_at",
]
# Explicit schema, not inferred: `subcategory`/`weight_kg` are nullable in
# iot.product_events, and an incremental batch containing only e.g. price
# "updated" events (which don't set those fields) can leave a column NULL in
# every row - createDataFrame's type inference then fails outright with
# [CANNOT_DETERMINE_TYPE], not just a wrong-but-working guess (confirmed by
# testing). unit_price/weight_kg come through psycopg2 as Decimal, which
# Spark maps fine to DoubleType.
SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("product_id", StringType()),
    StructField("event_type", StringType()),
    StructField("name", StringType()),
    StructField("category", StringType()),
    StructField("subcategory", StringType(), nullable=True),
    StructField("unit_price", DecimalType(10, 2)),
    StructField("weight_kg", DecimalType(6, 2), nullable=True),
    StructField("initial_stock", IntegerType()),
    StructField("event_at", TimestampType()),
    StructField("ingested_at", TimestampType()),
])


def main() -> None:
    spark = get_spark_session("products-to-silver")
    spark.sparkContext.setLogLevel("WARN")

    watermark = read_watermark(WATERMARK_KEY)
    rows = fetch_incremental(
        f"SELECT {', '.join(COLUMNS)} FROM iot.product_events WHERE ingested_at > %s ORDER BY ingested_at",
        (watermark,),
    )
    if not rows:
        logger.info("No new product_events since %s", watermark)
        spark.stop()
        return

    df = spark.createDataFrame([Row(**dict(zip(COLUMNS, r))) for r in rows], schema=SCHEMA)

    window = Window.partitionBy("product_id").orderBy(col("event_at").desc())
    latest = (
        df.withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    upsert_rows = [
        (
            r["product_id"], r["name"], r["category"], r["subcategory"],
            r["unit_price"], r["weight_kg"], r["initial_stock"], r["event_at"],
        )
        for r in latest.collect()
    ]
    upsert(
        "silver.products_current",
        key_cols=["product_id"],
        set_cols=["name", "category", "subcategory", "unit_price", "weight_kg", "initial_stock", "updated_at"],
        rows=upsert_rows,
    )

    new_watermark = max(r[COLUMNS.index("ingested_at")] for r in rows)
    write_watermark(WATERMARK_KEY, new_watermark)
    logger.info("Upserted %d product(s), watermark -> %s", len(upsert_rows), new_watermark)
    spark.stop()


if __name__ == "__main__":
    main()
