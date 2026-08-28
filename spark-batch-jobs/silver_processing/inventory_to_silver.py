"""Bronze -> silver, inventory: stock-consistency check via a lag() window
per product_id (seeded from silver.inventory_current for cross-batch
continuity), flags anomalies to silver.error_inventory_changes, maintains
both silver.inventory_current (latest stock per product) and an append-only
silver.inventory_history."""
from __future__ import annotations

import json
import logging

from psycopg2.extras import execute_values
from pyspark.sql import Row
from pyspark.sql.functions import coalesce, col, lag, lit
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

from shared_utils import (
    fetch_incremental,
    get_spark_session,
    pg_conn,
    read_partitions_per_topic,
    read_watermark,
    route_errors,
    upsert_partition,
    write_watermark,
)

HISTORY_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity_delta", IntegerType(), False),
        StructField("current_stock", IntegerType(), False),
        StructField("change_reason", StringType(), False),
        StructField("changed_at", TimestampType(), False),
    ]
)
CURRENT_STOCK_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("current_stock", IntegerType(), False),
        StructField("changed_at", TimestampType(), False),
    ]
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WATERMARK_KEY = "inventory_changes"
COLUMNS = ["event_id", "product_id", "quantity_delta", "current_stock", "change_reason", "changed_at", "ingested_at"]

_ALLOWED_REASONS = {"purchase", "sale", "adjustment", "return", "restock"}


def _seed_stock(product_ids: list[str]) -> dict[str, int]:
    if not product_ids:
        return {}
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT product_id, current_stock FROM silver.inventory_current WHERE product_id = ANY(%s::uuid[])",
                ([str(p) for p in product_ids],),
            )
            return dict(cur.fetchall())


def _append_history(rows: list[tuple]) -> None:
    if not rows:
        return
    with pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO silver.inventory_history
                    (event_id, product_id, quantity_delta, current_stock, change_reason, changed_at)
                VALUES %s
                ON CONFLICT (event_id) DO NOTHING
                """,
                rows,
            )
        conn.commit()


def _append_history_partition(rows_iter) -> None:
    """foreachPartition-compatible wrapper around _append_history() - same
    pattern as shared_utils.upsert_partition(): each partition opens its
    own connection and appends its slice independently. ON CONFLICT (event_id)
    DO NOTHING makes this safe regardless of how rows are split across
    partitions - every row has its own unique event_id, no cross-partition
    interaction possible for this insert-only table."""
    rows = list(rows_iter)
    if not rows:
        return
    _append_history(rows)


def _upsert_current_stock_partition(rows_iter) -> None:
    """Each partition computes its own local 'latest changed_at per
    product_id' reduction on just its slice of rows, then upserts via the
    existing upsert_partition()/upsert() - safe even if the same product_id's
    changes end up split across two partitions, because upsert()'s own
    `WHERE EXCLUDED.updated_at > table.updated_at` guard (unchanged) rejects
    an older partition's write from clobbering a newer one, regardless of
    which partition's task happens to run/commit first."""
    latest_per_product: dict[str, tuple] = {}
    for r in rows_iter:
        pid = r.product_id
        if pid not in latest_per_product or r.changed_at > latest_per_product[pid][1]:
            latest_per_product[pid] = (r.current_stock, r.changed_at)
    rows = [(pid, stock, changed_at) for pid, (stock, changed_at) in latest_per_product.items()]
    upsert_partition(
        "silver.inventory_current",
        key_cols=["product_id"],
        set_cols=["current_stock", "updated_at"],
        rows_iter=rows,
    )


def main() -> None:
    spark = get_spark_session("inventory-to-silver")
    spark.sparkContext.setLogLevel("WARN")

    watermark = read_watermark(WATERMARK_KEY)
    rows = fetch_incremental(
        f"SELECT {', '.join(COLUMNS)} FROM iot.inventory_changes WHERE ingested_at > %s ORDER BY ingested_at",
        (watermark,),
    )
    if not rows:
        logger.info("No new inventory_changes since %s", watermark)
        spark.stop()
        return

    product_ids = sorted({r[COLUMNS.index("product_id")] for r in rows})
    seed_stock = _seed_stock(product_ids)

    df = spark.createDataFrame([Row(**dict(zip(COLUMNS, r))) for r in rows])
    seed_df = spark.createDataFrame(
        [(pid, stock) for pid, stock in seed_stock.items()], ["product_id", "seed_stock"]
    ) if seed_stock else spark.createDataFrame([], "product_id string, seed_stock int")

    window = Window.partitionBy("product_id").orderBy("changed_at")
    with_lag = (
        df.join(seed_df, on="product_id", how="left")
        .withColumn("prev_in_batch", lag("current_stock").over(window))
        .withColumn("expected_prev", coalesce(col("prev_in_batch"), col("seed_stock"), lit(0)))
    )

    validated = with_lag.withColumn(
        "is_valid",
        (col("expected_prev") + col("quantity_delta") == col("current_stock"))
        & (col("current_stock") >= 0)
        & (col("change_reason").isin(*_ALLOWED_REASONS)),
    )

    bad_rows = [
        (json.dumps({c: str(r[c]) for c in COLUMNS}), "stock_inconsistency_or_invalid_reason")
        for r in validated.filter(~col("is_valid")).collect()
    ]
    route_errors("silver.error_inventory_changes", bad_rows)

    good = validated.filter(col("is_valid")).collect()

    history_rows = [
        (
            str(r["event_id"]), str(r["product_id"]), r["quantity_delta"],
            r["current_stock"], r["change_reason"], r["changed_at"],
        )
        for r in good
    ]
    N = read_partitions_per_topic()
    history_df = spark.createDataFrame(history_rows, schema=HISTORY_SCHEMA)
    history_df.repartition(N).foreachPartition(_append_history_partition)

    # Latest stock per product in this batch (by changed_at) -> silver.inventory_current.
    # The per-product reduction itself moves into _upsert_current_stock_partition
    # (each partition reduces its own slice, safe per that function's own
    # docstring) - only the DataFrame construction/repartition stays here.
    stock_rows = [(str(r["product_id"]), r["current_stock"], r["changed_at"]) for r in good]
    stock_df = spark.createDataFrame(stock_rows, schema=CURRENT_STOCK_SCHEMA)
    stock_df.repartition(N).foreachPartition(_upsert_current_stock_partition)

    new_watermark = max(r[COLUMNS.index("ingested_at")] for r in rows)
    write_watermark(WATERMARK_KEY, new_watermark)
    logger.info(
        "Updated stock for up to %d product(s), rejected %d, watermark -> %s",
        len({r["product_id"] for r in good}), len(bad_rows), new_watermark,
    )
    spark.stop()


if __name__ == "__main__":
    main()
