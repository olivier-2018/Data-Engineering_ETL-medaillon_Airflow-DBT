"""Bronze -> silver for invoices - folds in what a separate payment domain
used to cover (redesign plan decision #6). Validates the
created->pending->settled transition sequence (cancelled reachable only
from created/pending) plus a monotonic non-decreasing payment_reminder
check per invoice_id (extends the same lag()-window continuity pattern
used by inventory_to_silver.py's stock-consistency check).

created_at is not a bronze-carried field (iot.invoice_events has no
created_at column, same reasoning as purchase orders) - derived only from
the one-time status='created' row and passed through upsert()'s
coalesce_cols.

Referential check: purchase_order_id should exist in
silver.purchase_orders_current (silver.invoices_current has a hard FK to
it) - orphans are rejected here defensively, since invoices/purchase-orders
run on independently-scheduled DAGs (redesign plan §7) and could otherwise
race a hard hard FK violation that fails the whole batch insert, not just
one row. Accepted known risk: an invoice whose purchase order genuinely
hasn't landed in silver yet by chance of DAG timing gets permanently
quarantined to the error table (watermarks always advance, no automatic
requeue) rather than retried - the same trade-off this project already
accepts elsewhere (e.g. truck_positions_to_silver.py's orphan check)."""
from __future__ import annotations

import json
import logging

from pyspark.sql import Row
from pyspark.sql.functions import coalesce, col, lag, min as spark_min, row_number
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

from shared_utils import fetch_incremental, get_spark_session, read_watermark, route_errors, upsert, write_watermark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BRONZE_TABLE_NAME = "invoice_events"
SILVER_TABLE = "silver.invoices_current"
ERROR_TABLE = "silver.error_invoices"

BRONZE_COLUMNS = [
    "event_id", "invoice_id", "purchase_order_id", "customer_id",
    "amount", "status", "payment_reminder", "due_at", "event_at", "ingested_at",
]

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("invoice_id", StringType(), False),
        StructField("purchase_order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("status", StringType(), False),
        StructField("payment_reminder", IntegerType(), False),
        StructField("due_at", TimestampType(), True),
        StructField("event_at", TimestampType(), False),
        StructField("ingested_at", TimestampType(), False),
    ]
)

_RANK = {"created": 0, "pending": 1, "settled": 2}
_CANCELLABLE_MAX_RANK = 1  # pending - cancellation only valid up through here


def _is_valid_transition(prev_status: str | None, prev_reminder: int | None, status: str, reminder: int) -> bool:
    if prev_status is None:
        return True
    if status == "cancelled":
        return _RANK.get(prev_status, 99) <= _CANCELLABLE_MAX_RANK
    if prev_status == "cancelled":
        return False
    if prev_reminder is not None and reminder < prev_reminder:
        return False  # payment_reminder must never decrease
    return _RANK.get(status, -1) >= _RANK.get(prev_status, -1)


def main() -> None:
    spark = get_spark_session("silver-invoices")
    spark.sparkContext.setLogLevel("WARN")

    watermark = read_watermark(BRONZE_TABLE_NAME)
    # amount is DECIMAL in Postgres - psycopg2 returns decimal.Decimal, which
    # PySpark's DoubleType verifier rejects outright (only a native float is
    # accepted). Cast to float8 in SQL so the value is already a plain float.
    select_cols = ["amount::float8 AS amount" if c == "amount" else c for c in BRONZE_COLUMNS]
    rows = fetch_incremental(
        f"SELECT {', '.join(select_cols)} FROM iot.invoice_events WHERE ingested_at > %s",
        (watermark,),
    )
    if not rows:
        logger.info("No new invoice_events since %s", watermark)
        spark.stop()
        return

    df = spark.createDataFrame([Row(*r) for r in rows], schema=SCHEMA)

    by_time = Window.partitionBy("invoice_id").orderBy(col("event_at"))
    with_prev = df.withColumn("prev_status", lag("status").over(by_time)).withColumn(
        "prev_reminder", lag("payment_reminder").over(by_time)
    )

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
    for r in with_prev.collect():
        if str(r.purchase_order_id) not in known_order_ids:
            error_rows.append(
                (json.dumps({c: str(getattr(r, c)) for c in BRONZE_COLUMNS}), "orphan_purchase_order")
            )
        elif r.amount <= 0:
            error_rows.append((json.dumps({c: str(getattr(r, c)) for c in BRONZE_COLUMNS}), "invalid_amount"))
        elif not _is_valid_transition(r.prev_status, r.prev_reminder, r.status, r.payment_reminder):
            error_rows.append(
                (json.dumps({c: str(getattr(r, c)) for c in BRONZE_COLUMNS}), "invalid_status_transition_or_reminder")
            )
        else:
            valid_rows.append(r)

    if error_rows:
        route_errors(ERROR_TABLE, error_rows)

    if valid_rows:
        valid_df = spark.createDataFrame(valid_rows).drop("prev_status", "prev_reminder")

        created_rows = (
            valid_df.filter(col("status") == "created")
            .select("invoice_id", col("event_at").alias("created_at"))
        )

        # Fallback for an invoice whose 'created' row never made it into
        # bronze (e.g. lost at the Kafka producer before its topic finished
        # auto-creating, on a fresh stack's very first moments) - without
        # this, such an invoice's first-ever appearance here has no
        # created_at source at all, violating the NOT NULL constraint and
        # permanently failing this job every run (the watermark never
        # advances past a failed batch). Falls back to the earliest
        # event_at this invoice has in the batch.
        earliest_event = valid_df.groupBy("invoice_id").agg(spark_min("event_at").alias("earliest_event_at"))

        latest = Window.partitionBy("invoice_id").orderBy(col("event_at").desc())
        deduped = (
            valid_df.withColumn("rn", row_number().over(latest))
            .filter(col("rn") == 1)
            .drop("rn")
        )
        final = (
            deduped.join(created_rows, on="invoice_id", how="left")
            .join(earliest_event, on="invoice_id", how="left")
            .withColumn("created_at", coalesce(col("created_at"), col("earliest_event_at")))
        )

        silver_rows = [
            (
                r.invoice_id, r.purchase_order_id, r.customer_id, r.amount,
                r.status, r.payment_reminder, r.due_at, r.event_at, r.created_at,
            )
            for r in final.collect()
        ]
        upsert(
            SILVER_TABLE,
            key_cols=["invoice_id"],
            set_cols=[
                "purchase_order_id", "customer_id", "amount", "status",
                "payment_reminder", "due_payment_date", "updated_at",
            ],
            rows=silver_rows,
            coalesce_cols=["created_at"],
        )
        logger.info("Upserted %d invoice(s) into %s", len(silver_rows), SILVER_TABLE)

    new_watermark = df.agg({"ingested_at": "max"}).collect()[0][0]
    write_watermark(BRONZE_TABLE_NAME, new_watermark)
    logger.info("Rejected %d row(s) to %s", len(error_rows), ERROR_TABLE)
    spark.stop()


if __name__ == "__main__":
    main()
