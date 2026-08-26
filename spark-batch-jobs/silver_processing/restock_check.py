"""Automatic restock mechanism - belongs to the silver layer (reads
silver.products_current/inventory_current, applies the restock business
rule) even though its output writes back into bronze tables. Deliberately
NOT a Spark job (no SparkSession, no DataFrame) - pure psycopg2 logic with
no distributed computation to justify a JVM boot. Triggered by Airflow as a
BashOperator task in silver_inventory_dag.py, right after
inventory_to_silver.py's SparkSubmitOperator task - it runs as a plain
Python subprocess inside the airflow-scheduler container (same way `dbt
run`/`dbt snapshot` execute), never via spark-submit.

Reads silver.products_current WHERE restock_required (materialized by
products_to_silver.py at qty < restock_threshold_pct% of nominal_capacity -
the 20% figure itself lives in exactly one place, computed there, not
re-derived here), and for each flagged product appends:

  1. an iot.inventory_changes row (change_reason='restock') bringing stock
     back up to restock_target_pct% of nominal_capacity, and
  2. an iot.product_events row (event_type='refill') carrying the refill
     economics - refill_qty (same delta as #1) and refill_unit_price
     (refill_discount_pct% below the product's listed unit_price) - for
     the gold-layer cost-of-goods-refilled reporting.

Both are instant, immutability-respecting appends (never an UPDATE).

Reads its two percentages from data_generators/config.yaml (mounted
read-only into this container) rather than duplicating them into .env -
that file is the single source of truth for scenario parameters.
"""
from __future__ import annotations

import logging
import os
import uuid

import yaml

from shared_utils import pg_conn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONFIG_PATH = os.environ.get("CONFIG_PATH", "/opt/config/config.yaml")


def main() -> None:
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    target_pct = cfg["products"]["restock_target_pct"]
    refill_discount_pct = cfg["products"]["refill_discount_pct"]

    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.product_id, p.name, p.brand, p.model, p.category, p.subcategory,
                       p.unit_price, p.weight_kg, p.nominal_capacity, ic.current_stock
                FROM silver.products_current p
                JOIN silver.inventory_current ic ON ic.product_id = p.product_id
                WHERE p.restock_required
                """
            )
            due_for_restock = cur.fetchall()

            if not due_for_restock:
                logger.info("No products currently flagged restock_required.")
                return

            inventory_rows = []
            product_event_rows = []
            for (
                product_id, name, brand, model, category, subcategory,
                unit_price, weight_kg, nominal_capacity, current_stock,
            ) in due_for_restock:
                target_stock = int(nominal_capacity * (target_pct / 100.0))
                quantity_delta = target_stock - current_stock
                if quantity_delta <= 0:
                    continue

                inventory_rows.append(
                    (str(uuid.uuid4()), product_id, quantity_delta, target_stock, "restock")
                )
                refill_unit_price = round(float(unit_price) * (1 - refill_discount_pct / 100.0), 2)
                product_event_rows.append(
                    (
                        str(uuid.uuid4()), product_id, "refill", name, brand, model, category, subcategory,
                        unit_price, weight_kg, nominal_capacity, quantity_delta, refill_unit_price,
                    )
                )

            if inventory_rows:
                cur.executemany(
                    """
                    INSERT INTO iot.inventory_changes
                        (event_id, product_id, quantity_delta, current_stock, change_reason, changed_at, ingested_at)
                    VALUES (%s, %s, %s, %s, %s, now(), now())
                    ON CONFLICT (event_id, changed_at) DO NOTHING
                    """,
                    inventory_rows,
                )
            if product_event_rows:
                cur.executemany(
                    """
                    INSERT INTO iot.product_events
                        (event_id, product_id, event_type, name, brand, model, category, subcategory,
                         unit_price, weight_kg, nominal_capacity, refill_qty, refill_unit_price,
                         event_at, ingested_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    product_event_rows,
                )
        conn.commit()

    logger.info("Restocked %d product(s).", len(inventory_rows))


if __name__ == "__main__":
    main()
