"""Bronze -> silver, inventory: stock-consistency check via a lag() window
per product_id (seeded from silver.inventory_current for cross-batch
continuity), flags anomalies to silver.error_inventory_changes, maintains
both silver.inventory_current (latest stock per product) and an append-only
silver.inventory_history."""
from __future__ import annotations

import logging

from psycopg2.extras import execute_values
from pyspark.sql import Row
from pyspark.sql.functions import coalesce, col, lag, lit
from pyspark.sql.window import Window

from shared_utils import fetch_incremental, get_spark_session, pg_conn, read_watermark, route_errors, upsert, write_watermark

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

    bad_rows = [r.asDict() for r in validated.filter(~col("is_valid")).collect()]
    for r in bad_rows:
        r["reason_code"] = "stock_inconsistency_or_invalid_reason"
    route_errors("silver.error_inventory_changes", bad_rows)

    good = validated.filter(col("is_valid")).collect()

    history_rows = [
        (r["event_id"], r["product_id"], r["quantity_delta"], r["current_stock"], r["change_reason"], r["changed_at"])
        for r in good
    ]
    _append_history(history_rows)

    # Latest stock per product in this batch (by changed_at) -> silver.inventory_current.
    latest_per_product: dict[str, tuple] = {}
    for r in good:
        pid = r["product_id"]
        if pid not in latest_per_product or r["changed_at"] > latest_per_product[pid][1]:
            latest_per_product[pid] = (r["current_stock"], r["changed_at"])
    upsert_rows = [(pid, stock, changed_at) for pid, (stock, changed_at) in latest_per_product.items()]
    upsert(
        "silver.inventory_current",
        key_cols=["product_id"],
        set_cols=["current_stock", "updated_at"],
        rows=upsert_rows,
    )

    new_watermark = max(r[COLUMNS.index("ingested_at")] for r in rows)
    write_watermark(WATERMARK_KEY, new_watermark)
    logger.info(
        "Updated stock for %d product(s), rejected %d, watermark -> %s",
        len(upsert_rows), len(bad_rows), new_watermark,
    )
    spark.stop()


if __name__ == "__main__":
    main()
