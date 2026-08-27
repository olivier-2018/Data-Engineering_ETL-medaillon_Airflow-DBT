"""Bronze -> silver for purchase orders. Validates the observed
created->invoiced->paid->on-hold->loaded->in-transit->delivered->closed
transition sequence (rank-monotonic), with cancelled reachable only from
ranks 0-3 (created/invoiced/paid/on-hold) - matches the same transition
table the generator's own PurchaseOrder.ALLOWED_TRANSITIONS enforces
(redesign plan §3b/§5), so the generator structurally can't emit an
impossible transition and this job independently re-checks it as
defense-in-depth against a future generator bug. Deliberately hardcoded in
both places rather than sourced from a shared config value - the point of
having two independent implementations is that a bug in one doesn't
silently propagate into the other.

Known simplification (same as the pre-redesign design): the transition
check is batch-local - it only sees `lag(status)` for rows that land in
the *same* incremental batch. A transition split across two batch windows
has no prior row to compare against and is treated as valid (the first-seen
anchor for that order in this batch), so this is a partial, not
exhaustive, cross-batch check.

created_at is not a bronze-carried field (iot.purchase_order_events has no
created_at column) - a status='created' row fires exactly once, ever, per
purchase_order_id, so created_at is derived only from that one row (same
pattern as customers_to_silver.py/invoices_to_silver.py) and passed through
upsert()'s coalesce_cols, which leaves the existing stored value untouched
on every later batch that doesn't happen to contain that order's
'created' row."""
from __future__ import annotations

import json
import logging

from pyspark.sql import Row
from pyspark.sql.functions import coalesce, col, lag, min as spark_min, row_number
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

from shared_utils import fetch_incremental, get_spark_session, read_watermark, route_errors, upsert, write_watermark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BRONZE_TABLE_NAME = "purchase_order_events"
SILVER_TABLE = "silver.purchase_orders_current"
ERROR_TABLE = "silver.error_purchase_orders"

BRONZE_COLUMNS = [
    "event_id", "purchase_order_id", "customer_id", "status", "delivery_address", "contact_tel",
    "invoice_address", "vat_number", "target_delivery_date", "truck_id", "zone_id", "event_at", "ingested_at",
]

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("purchase_order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("status", StringType(), False),
        StructField("delivery_address", StringType(), True),
        StructField("contact_tel", StringType(), True),
        StructField("invoice_address", StringType(), True),
        StructField("vat_number", StringType(), True),
        StructField("target_delivery_date", TimestampType(), True),
        StructField("truck_id", StringType(), True),
        StructField("zone_id", IntegerType(), True),
        StructField("event_at", TimestampType(), False),
        StructField("ingested_at", TimestampType(), False),
    ]
)

_RANK = {
    "created": 0, "invoiced": 1, "paid": 2, "on-hold": 3,
    "loaded": 4, "in-transit": 5, "delivered": 6, "closed": 7,
}
_CANCELLABLE_MAX_RANK = 3  # on-hold - cancellation only valid up through here


def _is_valid_transition(prev_status: str | None, status: str) -> bool:
    if prev_status is None:
        return True  # first row seen for this order in this batch - nothing to compare against
    if status == "cancelled":
        return _RANK.get(prev_status, 99) <= _CANCELLABLE_MAX_RANK
    if prev_status == "cancelled":
        return False  # terminal - no transition out of cancelled is ever valid
    return _RANK.get(status, -1) >= _RANK.get(prev_status, -1)


def main() -> None:
    spark = get_spark_session("silver-purchase-orders")
    spark.sparkContext.setLogLevel("WARN")

    watermark = read_watermark(BRONZE_TABLE_NAME)
    rows = fetch_incremental(
        f"SELECT {', '.join(BRONZE_COLUMNS)} FROM iot.purchase_order_events WHERE ingested_at > %s",
        (watermark,),
    )
    if not rows:
        logger.info("No new purchase_order_events since %s", watermark)
        spark.stop()
        return

    df = spark.createDataFrame([Row(*r) for r in rows], schema=SCHEMA)

    by_time = Window.partitionBy("purchase_order_id").orderBy(col("event_at"))
    with_prev = df.withColumn("prev_status", lag("status").over(by_time))

    # Orphan checks: customers/purchase-orders/truck-fleet run on
    # independently-scheduled DAGs, so a purchase order can legitimately
    # reference a customer_id or truck_id that hasn't landed in silver yet
    # by chance of DAG timing (same accepted race documented in
    # invoices_to_silver.py/product_on_orders_to_silver.py). Without this
    # check, the upsert()'s hard FK constraint fails the row - and since
    # execute_values sends the whole batch in one INSERT, one bad row
    # crashes every row in the batch, not just itself. str() matters here:
    # psycopg2 returns a UUID column as a uuid.UUID object, never equal to
    # a plain str even for the same value.
    known_customer_ids = {
        str(r[0]) for r in fetch_incremental("SELECT customer_id FROM silver.customers_current")
    }
    known_truck_ids = {
        r[0] for r in fetch_incremental("SELECT truck_id FROM silver.truck_fleet_current")
    }

    valid_rows, error_rows = [], []
    for r in with_prev.collect():
        if r.customer_id not in known_customer_ids:
            error_rows.append(
                (json.dumps({c: str(getattr(r, c)) for c in BRONZE_COLUMNS}), "orphan_customer")
            )
        elif r.truck_id is not None and r.truck_id not in known_truck_ids:
            error_rows.append(
                (json.dumps({c: str(getattr(r, c)) for c in BRONZE_COLUMNS}), "orphan_truck")
            )
        elif _is_valid_transition(r.prev_status, r.status):
            valid_rows.append(r)
        else:
            error_rows.append(
                (
                    json.dumps({c: str(getattr(r, c)) for c in BRONZE_COLUMNS}),
                    "invalid_status_transition",
                )
            )

    if error_rows:
        route_errors(ERROR_TABLE, error_rows)

    if valid_rows:
        # Explicit schema, not inferred: inferring from `valid_rows` (a plain
        # list[Row]) re-derives types from raw Python values and fails
        # outright with CANNOT_DETERMINE_TYPE whenever a nullable column
        # (e.g. truck_id, target_delivery_date) happens to be None in every
        # surviving row of a small batch - `with_prev`'s schema is already
        # known and correct, so reuse it instead of re-inferring.
        valid_df = spark.createDataFrame(valid_rows, schema=with_prev.schema).drop("prev_status")

        created_rows = (
            valid_df.filter(col("status") == "created")
            .select("purchase_order_id", col("event_at").alias("created_at"))
        )

        # Fallback for an order whose 'created' row never made it into bronze
        # (e.g. lost at the Kafka producer before its topic finished
        # auto-creating, on a fresh stack's very first moments) - without
        # this, such an order's first-ever appearance here has no created_at
        # source at all (no existing silver row to COALESCE against, and no
        # 'created' row in this batch), which would violate the NOT NULL
        # constraint and permanently fail this job every single run (the
        # watermark never advances past a failed batch, so the same broken
        # order reappears in every retry forever). Falling back to the
        # earliest event_at this order has in the batch is an honest proxy:
        # not the true creation time, but a real, monotonically-consistent
        # timestamp - not NULL, not a runtime crash.
        earliest_event = valid_df.groupBy("purchase_order_id").agg(
            spark_min("event_at").alias("earliest_event_at")
        )

        latest = Window.partitionBy("purchase_order_id").orderBy(col("event_at").desc())
        deduped = (
            valid_df.withColumn("rn", row_number().over(latest))
            .filter(col("rn") == 1)
            .drop("rn")
        )
        final = (
            deduped.join(created_rows, on="purchase_order_id", how="left")
            .join(earliest_event, on="purchase_order_id", how="left")
            .withColumn("created_at", coalesce(col("created_at"), col("earliest_event_at")))
        )

        silver_rows = [
            (
                r.purchase_order_id, r.customer_id, r.status, r.delivery_address, r.contact_tel,
                r.invoice_address, r.vat_number, r.target_delivery_date, r.truck_id, r.zone_id,
                r.event_at, r.created_at,
            )
            for r in final.collect()
        ]
        upsert(
            SILVER_TABLE,
            key_cols=["purchase_order_id"],
            set_cols=[
                "customer_id", "status", "delivery_address", "contact_tel", "invoice_address",
                "vat_number", "target_delivery_date", "truck_id", "zone_id", "updated_at",
            ],
            rows=silver_rows,
            coalesce_cols=["created_at"],
        )
        logger.info("Upserted %d purchase order(s) into %s", len(silver_rows), SILVER_TABLE)

    new_watermark = df.agg({"ingested_at": "max"}).collect()[0][0]
    write_watermark(BRONZE_TABLE_NAME, new_watermark)
    logger.info("Rejected %d row(s) to %s", len(error_rows), ERROR_TABLE)
    spark.stop()


if __name__ == "__main__":
    main()
