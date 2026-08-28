"""Bronze -> silver for purchase-order line items. Each product_on_order_id
is emitted exactly once, ever (immutable - the generator never edits an
existing order's line items after creation), so created_at and updated_at
are both simply the row's own event_at - no derivation/coalesce needed,
unlike domains with a genuine multi-event history per key.

Referential check: purchase_order_id must already exist in
silver.purchase_orders_current (LEFT JOIN, reject orphans) - the same
orphan-detection pattern used throughout this project. Run this job after
purchase_orders_to_silver.py in the same DAG (not a separate Asset
dependency) given this referential coupling."""
from __future__ import annotations

import json
import logging

from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from shared_utils import (
    fetch_incremental,
    get_spark_session,
    read_partitions_per_topic,
    read_watermark,
    route_errors,
    upsert_partition,
    write_watermark,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BRONZE_TABLE_NAME = "product_on_order_events"
SILVER_TABLE = "silver.product_on_orders_current"
ERROR_TABLE = "silver.error_product_on_orders"

BRONZE_COLUMNS = [
    "event_id", "product_on_order_id", "purchase_order_id", "product_id",
    "qty_on_order", "customer_comment", "event_at", "ingested_at",
]

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("product_on_order_id", StringType(), False),
        StructField("purchase_order_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("qty_on_order", IntegerType(), False),
        StructField("customer_comment", StringType(), True),
        StructField("event_at", TimestampType(), False),
        StructField("ingested_at", TimestampType(), False),
    ]
)


def main() -> None:
    spark = get_spark_session("silver-product-on-orders")
    spark.sparkContext.setLogLevel("WARN")

    watermark = read_watermark(BRONZE_TABLE_NAME)
    rows = fetch_incremental(
        f"SELECT {', '.join(BRONZE_COLUMNS)} FROM iot.product_on_order_events WHERE ingested_at > %s",
        (watermark,),
    )
    if not rows:
        logger.info("No new product_on_order_events since %s", watermark)
        spark.stop()
        return

    df = spark.createDataFrame([Row(*r) for r in rows], schema=SCHEMA)

    # str() here matters: psycopg2 returns a UUID column as a uuid.UUID
    # object, but r.purchase_order_id below is a plain string (Spark has no
    # native UUID type) - without this cast, `str(x) not in {UUID(...)}` is
    # always True regardless of whether the order actually exists, since a
    # str and a UUID object are never equal even for the "same" value.
    known_order_ids = {
        str(r[0])
        for r in fetch_incremental("SELECT purchase_order_id FROM silver.purchase_orders_current")
    }

    valid_rows, error_rows = [], []
    for r in df.collect():
        if str(r.purchase_order_id) not in known_order_ids or r.qty_on_order <= 0:
            reason = "orphan_purchase_order" if str(r.purchase_order_id) not in known_order_ids else "invalid_qty"
            error_rows.append((json.dumps({c: str(getattr(r, c)) for c in BRONZE_COLUMNS}), reason))
        else:
            valid_rows.append(r)

    if error_rows:
        route_errors(ERROR_TABLE, error_rows)

    if valid_rows:
        def _write_partition(rows_iter) -> None:
            rows = [
                (
                    r.product_on_order_id, r.purchase_order_id, r.product_id,
                    r.qty_on_order, r.customer_comment, r.event_at, r.event_at,
                )
                for r in rows_iter
            ]
            upsert_partition(
                SILVER_TABLE,
                key_cols=["product_on_order_id"],
                set_cols=[
                    "purchase_order_id", "product_id", "qty_on_order",
                    "customer_comment", "created_at", "updated_at",
                ],
                rows_iter=rows,
            )

        # valid_rows was already collected to the driver (needed for the
        # Python-side orphan-check against known_order_ids) - rebuild a
        # small DataFrame from it (reusing df's own schema, same trick as
        # purchase_orders_to_silver.py, to avoid CANNOT_DETERMINE_TYPE) so
        # repartition+foreachPartition can distribute the write across N
        # genuinely concurrent Spark tasks instead of one driver-side call.
        valid_df = spark.createDataFrame(valid_rows, schema=df.schema)
        valid_df.repartition(read_partitions_per_topic()).foreachPartition(_write_partition)
        logger.info("Upserted up to %d line item(s) into %s", len(valid_rows), SILVER_TABLE)

    new_watermark = df.agg({"ingested_at": "max"}).collect()[0][0]
    write_watermark(BRONZE_TABLE_NAME, new_watermark)
    logger.info("Rejected %d row(s) to %s", len(error_rows), ERROR_TABLE)
    spark.stop()


if __name__ == "__main__":
    main()
