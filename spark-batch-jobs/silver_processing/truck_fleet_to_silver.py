"""Bronze -> silver for the truck fleet - a near-static dimension (seeded
once at generator startup, only rare updates), so plain latest-event-wins
dedup with no transition validation needed. silver.truck_fleet_current has
no created_at column at all, so there's nothing to derive/coalesce here."""
from __future__ import annotations

import logging

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

BRONZE_TABLE_NAME = "truck_fleet_events"
SILVER_TABLE = "silver.truck_fleet_current"

BRONZE_COLUMNS = [
    "event_id", "truck_id", "event_type", "name", "brand", "model",
    "size", "capacity", "weight_kg", "event_at", "ingested_at",
]

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("truck_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("name", StringType(), False),
        StructField("brand", StringType(), True),
        StructField("model", StringType(), True),
        StructField("size", StringType(), True),
        StructField("capacity", IntegerType(), False),
        StructField("weight_kg", DoubleType(), True),
        StructField("event_at", TimestampType(), False),
        StructField("ingested_at", TimestampType(), False),
    ]
)


def main() -> None:
    spark = get_spark_session("silver-truck-fleet")
    spark.sparkContext.setLogLevel("WARN")

    watermark = read_watermark(BRONZE_TABLE_NAME)
    # weight_kg is DECIMAL in Postgres - psycopg2 returns decimal.Decimal,
    # which PySpark's DoubleType verifier rejects outright (only a native
    # float is accepted). Cast to float8 in SQL so the value is already a
    # plain float.
    select_cols = ["weight_kg::float8 AS weight_kg" if c == "weight_kg" else c for c in BRONZE_COLUMNS]
    rows = fetch_incremental(
        f"SELECT {', '.join(select_cols)} FROM iot.truck_fleet_events WHERE ingested_at > %s",
        (watermark,),
    )
    if not rows:
        logger.info("No new truck_fleet_events since %s", watermark)
        spark.stop()
        return

    df = spark.createDataFrame([Row(*r) for r in rows], schema=SCHEMA)

    latest = Window.partitionBy("truck_id").orderBy(col("event_at").desc())
    deduped = (
        df.withColumn("rn", row_number().over(latest))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    silver_rows = [
        (r.truck_id, r.name, r.brand, r.model, r.size, r.capacity, r.weight_kg, r.event_at)
        for r in deduped.collect()
    ]
    upsert(
        SILVER_TABLE,
        key_cols=["truck_id"],
        set_cols=["name", "brand", "model", "size", "capacity", "weight_kg", "updated_at"],
        rows=silver_rows,
    )

    new_watermark = df.agg({"ingested_at": "max"}).collect()[0][0]
    write_watermark(BRONZE_TABLE_NAME, new_watermark)

    logger.info("Upserted %d truck(s) into %s", len(silver_rows), SILVER_TABLE)
    spark.stop()


if __name__ == "__main__":
    main()
