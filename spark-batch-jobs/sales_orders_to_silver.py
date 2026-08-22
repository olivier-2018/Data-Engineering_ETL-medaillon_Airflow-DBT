"""Bronze -> silver, sales orders: incremental read by event_at (§1a: orders
are periodic batch, not streaming); validates the observed order_status
transition sequence per order_id (natural now that bronze holds every
event) against the allowed lifecycle graph, flagging illegal transitions to
silver.error_sales_orders; range/required-field checks; builds
silver.sales_orders_current via latest-event-per-order_id."""
from __future__ import annotations

import logging

from pyspark.sql import Row
from pyspark.sql.functions import col, lag, row_number, udf
from pyspark.sql.types import BooleanType
from pyspark.sql.window import Window

from shared_utils import fetch_incremental, get_spark_session, read_watermark, route_errors, upsert, write_watermark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WATERMARK_KEY = "sales_order_events"
COLUMNS = [
    "event_id", "order_id", "customer_id", "product_id", "quantity",
    "unit_price_snapshot", "order_status", "created_at", "event_at", "ingested_at",
]

_STATUS_RANK = {
    "pending": 0,
    "confirmed": 1,
    "picking": 2,
    "ready_for_dispatch": 3,
    "shipped": 4,
    "delivered": 5,
    "cancelled": 99,  # terminal, reachable from any early rank
}


def _is_valid_transition(prev_status, current_status) -> bool:
    if prev_status is None:
        return True  # first event for this order_id
    if current_status == "cancelled":
        return _STATUS_RANK.get(prev_status, -1) < _STATUS_RANK["shipped"]
    return _STATUS_RANK.get(current_status, -1) >= _STATUS_RANK.get(prev_status, -1)


_is_valid_transition_udf = udf(_is_valid_transition, BooleanType())


def main() -> None:
    spark = get_spark_session("sales-orders-to-silver")
    spark.sparkContext.setLogLevel("WARN")

    watermark = read_watermark(WATERMARK_KEY)
    rows = fetch_incremental(
        f"SELECT {', '.join(COLUMNS)} FROM iot.sales_order_events WHERE ingested_at > %s ORDER BY ingested_at",
        (watermark,),
    )
    if not rows:
        logger.info("No new sales_order_events since %s", watermark)
        spark.stop()
        return

    df = spark.createDataFrame([Row(**dict(zip(COLUMNS, r))) for r in rows])

    order_window = Window.partitionBy("order_id").orderBy("event_at")
    validated = df.withColumn("prev_status", lag("order_status").over(order_window)).withColumn(
        "is_valid", _is_valid_transition_udf(col("prev_status"), col("order_status"))
    )

    range_checked = validated.withColumn(
        "is_valid",
        col("is_valid") & (col("quantity") >= 1) & (col("unit_price_snapshot") > 0),
    )

    bad_rows = [r.asDict() for r in range_checked.filter(~col("is_valid")).collect()]
    for r in bad_rows:
        r["reason_code"] = "invalid_status_transition_or_range"
    route_errors("silver.error_sales_orders", bad_rows)

    good = range_checked.filter(col("is_valid"))
    latest_window = Window.partitionBy("order_id").orderBy(col("event_at").desc())
    latest = (
        good.withColumn("rn", row_number().over(latest_window))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    upsert_rows = [
        (
            r["order_id"], r["customer_id"], r["product_id"], r["quantity"],
            r["unit_price_snapshot"], r["order_status"], r["created_at"], r["event_at"],
        )
        for r in latest.collect()
    ]
    upsert(
        "silver.sales_orders_current",
        key_cols=["order_id"],
        set_cols=["customer_id", "product_id", "quantity", "unit_price_snapshot", "order_status", "created_at", "updated_at"],
        rows=upsert_rows,
    )

    new_watermark = max(r[COLUMNS.index("ingested_at")] for r in rows)
    write_watermark(WATERMARK_KEY, new_watermark)
    logger.info(
        "Upserted %d order(s), rejected %d, watermark -> %s",
        len(upsert_rows), len(bad_rows), new_watermark,
    )
    spark.stop()


if __name__ == "__main__":
    main()
