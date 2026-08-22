"""Small data-quality helper functions shared across the silver_*_dag.py
files - "is there new data worth processing" and "did the error rate spike"."""
from __future__ import annotations

from common.pg import pg_conn


def has_new_bronze_data(source_table: str, watermark_key: str) -> bool:
    """Used by each silver DAG's ShortCircuitOperator - skips the (fairly
    heavy) Spark submission entirely when there's nothing new to process."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_processed_ingested_at FROM control.watermarks WHERE table_name = %s",
                (watermark_key,),
            )
            row = cur.fetchone()
            watermark = row[0] if row else "1970-01-01"
            cur.execute(
                f"SELECT EXISTS(SELECT 1 FROM {source_table} WHERE ingested_at > %s)",
                (watermark,),
            )
            return cur.fetchone()[0]


def error_count_since(error_table: str, minutes: int = 15) -> int:
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {error_table} WHERE rejected_at > now() - interval '%s minutes'",
                (minutes,),
            )
            return cur.fetchone()[0]
