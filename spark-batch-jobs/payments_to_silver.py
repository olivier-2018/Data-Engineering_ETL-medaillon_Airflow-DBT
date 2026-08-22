"""Bronze -> silver, payments: validates payment_status transition sequence
per order_id, builds silver.payments_current (feeds the dbt
payment_status_snapshot SCD2)."""
from __future__ import annotations

import logging

from pyspark.sql import Row
from pyspark.sql.functions import col, lag, row_number, udf
from pyspark.sql.types import BooleanType
from pyspark.sql.window import Window

from shared_utils import fetch_incremental, get_spark_session, read_watermark, route_errors, upsert, write_watermark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WATERMARK_KEY = "payment_events"
COLUMNS = ["event_id", "order_id", "payment_status", "amount", "event_at", "ingested_at"]

_ALLOWED_NEXT = {
    None: {"authorized", "captured", "failed"},  # first event for this order_id
    "authorized": {"captured", "failed"},
    "captured": {"refunded"},
    "failed": set(),
    "refunded": set(),
}


def _is_valid_transition(prev_status, current_status) -> bool:
    return current_status in _ALLOWED_NEXT.get(prev_status, set())


_is_valid_transition_udf = udf(_is_valid_transition, BooleanType())


def main() -> None:
    spark = get_spark_session("payments-to-silver")
    spark.sparkContext.setLogLevel("WARN")

    watermark = read_watermark(WATERMARK_KEY)
    rows = fetch_incremental(
        f"SELECT {', '.join(COLUMNS)} FROM iot.payment_events WHERE ingested_at > %s ORDER BY ingested_at",
        (watermark,),
    )
    if not rows:
        logger.info("No new payment_events since %s", watermark)
        spark.stop()
        return

    df = spark.createDataFrame([Row(**dict(zip(COLUMNS, r))) for r in rows])

    order_window = Window.partitionBy("order_id").orderBy("event_at")
    validated = (
        df.withColumn("prev_status", lag("payment_status").over(order_window))
        .withColumn("is_valid", _is_valid_transition_udf(col("prev_status"), col("payment_status")) & (col("amount") > 0))
    )

    bad_rows = [r.asDict() for r in validated.filter(~col("is_valid")).collect()]
    for r in bad_rows:
        r["reason_code"] = "invalid_status_transition_or_amount"
    route_errors("silver.error_payments", bad_rows)

    good = validated.filter(col("is_valid"))
    latest_window = Window.partitionBy("order_id").orderBy(col("event_at").desc())
    latest = (
        good.withColumn("rn", row_number().over(latest_window))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    upsert_rows = [(r["order_id"], r["payment_status"], r["amount"], r["event_at"]) for r in latest.collect()]
    upsert(
        "silver.payments_current",
        key_cols=["order_id"],
        set_cols=["payment_status", "amount", "updated_at"],
        rows=upsert_rows,
    )

    new_watermark = max(r[COLUMNS.index("ingested_at")] for r in rows)
    write_watermark(WATERMARK_KEY, new_watermark)
    logger.info(
        "Upserted %d payment(s), rejected %d, watermark -> %s",
        len(upsert_rows), len(bad_rows), new_watermark,
    )
    spark.stop()


if __name__ == "__main__":
    main()
