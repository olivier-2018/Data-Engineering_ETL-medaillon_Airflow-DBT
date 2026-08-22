"""Automatic restock mechanism (§1c) - NOT a Kafka ingestion job. Reads
silver.inventory_current (fresh after inventory_to_silver.py has run) and
iot.products.initial_stock, and for any product below
products.restock_threshold_pct of its initial_stock, appends one new
change_reason='restock' row directly into iot.inventory_changes - an
instant, immutability-respecting append (never an UPDATE), bringing stock
back up to products.restock_target_pct of initial_stock.

Reads its two thresholds from data_generators/config.yaml (mounted
read-only into this container) rather than duplicating them into .env -
that file is the single source of truth for scenario parameters.

Deliberately NOT a Spark job (no SparkSession, no DataFrame) - it's pure
psycopg2 logic with no distributed computation to justify a JVM boot.
Run directly as `python3 restock_check.py` (e.g. a PythonOperator/
BashOperator task in silver_inventory_dag.py, right after
inventory_to_silver.py), not via spark-submit.
"""
from __future__ import annotations

import logging
import os
import uuid

import yaml

from shared_ingestion_utils import pg_conn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONFIG_PATH = os.environ.get("CONFIG_PATH", "/opt/config/config.yaml")


def main() -> None:
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    threshold_pct = cfg["products"]["restock_threshold_pct"]
    target_pct = cfg["products"]["restock_target_pct"]

    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.product_id, p.initial_stock, ic.current_stock
                FROM iot.products p
                JOIN silver.inventory_current ic ON ic.product_id = p.product_id
                WHERE ic.current_stock < p.initial_stock * (%s / 100.0)
                """,
                (threshold_pct,),
            )
            low_stock = cur.fetchall()

            if not low_stock:
                logger.info("No products below the %s%% restock threshold.", threshold_pct)
                return

            restock_rows = []
            for product_id, initial_stock, current_stock in low_stock:
                target_stock = int(initial_stock * (target_pct / 100.0))
                quantity_delta = target_stock - current_stock
                if quantity_delta <= 0:
                    continue
                restock_rows.append(
                    (str(uuid.uuid4()), product_id, quantity_delta, target_stock, "restock")
                )

            if restock_rows:
                cur.executemany(
                    """
                    INSERT INTO iot.inventory_changes
                        (event_id, product_id, quantity_delta, current_stock, change_reason, changed_at, ingested_at)
                    VALUES (%s, %s, %s, %s, %s, now(), now())
                    ON CONFLICT (event_id, changed_at) DO NOTHING
                    """,
                    restock_rows,
                )
        conn.commit()

    logger.info("Restocked %d product(s).", len(restock_rows))


if __name__ == "__main__":
    main()
