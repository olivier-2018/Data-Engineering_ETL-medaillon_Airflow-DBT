"""Bronze -> silver for products. Plain latest-event-wins dedup - every
iot.product_events row (whether Kafka-sourced 'created'/'updated' from the
generator, or a direct-to-Postgres 'refill' row appended by
restock_check.py) carries the product's full catalog attributes, so a
single latest-row-wins window is sufficient regardless of event_type.

Also computes restock_required here (qty < restock_threshold_pct% of
nominal_capacity) via a join against silver.inventory_current - the 20%
figure is read once from config.yaml and materialized into this one
column, so restock_check.py can simply query WHERE restock_required
instead of re-deriving the threshold itself. A product with no
silver.inventory_current row yet (never sold, brand new) defaults to full
stock (nominal_capacity), correctly evaluating to restock_required=false."""
from __future__ import annotations

import logging
import os

import yaml
from pyspark.sql import Row
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import (
    DoubleType,
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

CONFIG_PATH = os.environ.get("CONFIG_PATH", "/opt/config/config.yaml")

BRONZE_TABLE_NAME = "product_events"  # control.silver_watermarks key
SILVER_TABLE = "silver.products_current"

BRONZE_COLUMNS = [
    "event_id", "product_id", "event_type", "name", "brand", "model", "category", "subcategory",
    "unit_price", "weight_kg", "nominal_capacity", "event_at", "ingested_at",
]

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("name", StringType(), False),
        StructField("brand", StringType(), True),
        StructField("model", StringType(), True),
        StructField("category", StringType(), False),
        StructField("subcategory", StringType(), True),
        StructField("unit_price", DoubleType(), False),
        StructField("weight_kg", DoubleType(), True),
        StructField("nominal_capacity", IntegerType(), False),
        StructField("event_at", TimestampType(), False),
        StructField("ingested_at", TimestampType(), False),
    ]
)


def main() -> None:
    spark = get_spark_session("silver-products")
    spark.sparkContext.setLogLevel("WARN")

    with open(CONFIG_PATH) as f:
        threshold_pct = yaml.safe_load(f)["products"]["restock_threshold_pct"]

    watermark = read_watermark(BRONZE_TABLE_NAME)
    # unit_price/weight_kg are DECIMAL in Postgres - psycopg2 returns those as
    # decimal.Decimal, which PySpark's DoubleType verifier rejects outright
    # (it only accepts a native float). Cast to float8 in SQL so the value
    # arriving in Python is already a plain float, matching SCHEMA below.
    select_cols = [
        "unit_price::float8 AS unit_price" if c == "unit_price"
        else "weight_kg::float8 AS weight_kg" if c == "weight_kg"
        else c
        for c in BRONZE_COLUMNS
    ]
    rows = fetch_incremental(
        f"SELECT {', '.join(select_cols)} FROM iot.product_events WHERE ingested_at > %s",
        (watermark,),
    )
    if not rows:
        logger.info("No new product_events since %s", watermark)
        spark.stop()
        return

    df = spark.createDataFrame([Row(*r) for r in rows], schema=SCHEMA)

    latest = Window.partitionBy("product_id").orderBy(col("event_at").desc())
    deduped = (
        df.withColumn("rn", row_number().over(latest))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    product_ids = [r.product_id for r in deduped.select("product_id").collect()]
    stock_rows = fetch_incremental(
        "SELECT product_id, current_stock FROM silver.inventory_current WHERE product_id = ANY(%s::uuid[])",
        ([str(pid) for pid in product_ids],),
    ) if product_ids else []
    current_stock_by_product = {str(pid): stock for pid, stock in stock_rows}

    silver_rows = []
    for r in deduped.collect():
        current_stock = current_stock_by_product.get(r.product_id, r.nominal_capacity)
        restock_required = current_stock < (threshold_pct / 100.0) * r.nominal_capacity
        silver_rows.append(
            (
                r.product_id, r.name, r.brand, r.model, r.category, r.subcategory,
                r.unit_price, r.weight_kg, r.nominal_capacity, restock_required, r.event_at,
            )
        )

    upsert(
        SILVER_TABLE,
        key_cols=["product_id"],
        set_cols=[
            "name", "brand", "model", "category", "subcategory", "unit_price",
            "weight_kg", "nominal_capacity", "restock_required", "updated_at",
        ],
        rows=silver_rows,
    )

    new_watermark = df.agg({"ingested_at": "max"}).collect()[0][0]
    write_watermark(BRONZE_TABLE_NAME, new_watermark)

    logger.info("Upserted %d product(s) into %s", len(silver_rows), SILVER_TABLE)
    spark.stop()


if __name__ == "__main__":
    main()
