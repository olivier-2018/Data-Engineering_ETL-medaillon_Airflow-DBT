"""Shared helpers for the bronze -> silver incremental micro-batch jobs
(§7). Postgres access is via psycopg2 throughout (no JDBC driver in the
Spark image); rows are fetched via a parameterized query, loaded into a
Spark DataFrame via spark.createDataFrame(...) for the distributed/window
transformations, then written back via psycopg2 executemany/execute_values.

Watermark tracking here (control.watermarks, an ingested_at timestamp per
table) is a different scheme from control.kafka_offsets used by
spark-batch-jobs/bronze_ingestion/ - that layer tracks Kafka offsets; this
layer tracks Postgres timestamps, since bronze is already in Postgres by
the time these jobs run.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def pg_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["PIPELINE_DB_USER"],
        password=os.environ["PIPELINE_DB_PASSWORD"],
    )


def read_watermark(table_name: str) -> datetime:
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_processed_ingested_at FROM control.watermarks WHERE table_name = %s",
                (table_name,),
            )
            row = cur.fetchone()
    if row is None:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    return row[0]


def write_watermark(table_name: str, new_watermark: datetime) -> None:
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO control.watermarks (table_name, last_processed_ingested_at, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (table_name) DO UPDATE SET
                    last_processed_ingested_at = EXCLUDED.last_processed_ingested_at,
                    updated_at = now()
                """,
                (table_name, new_watermark),
            )
        conn.commit()


def fetch_incremental(query: str, params: tuple) -> list[tuple]:
    """Runs a parameterized SELECT (already filtered by the caller on
    ingested_at > watermark) and returns raw rows for spark.createDataFrame."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()


def route_errors(error_table: str, rows: list[dict], reason_col: str = "reason_code") -> None:
    """Appends rejected rows to silver.error_<domain> with a reason code -
    bad data is flagged, never silently dropped."""
    if not rows:
        return
    values = [(json.dumps(r, default=str), r.get(reason_col, "unknown")) for r in rows]
    with pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"INSERT INTO {error_table} (raw_payload, reason_code, rejected_at) VALUES %s",
                values,
                template="(%s, %s, now())",
            )
        conn.commit()


def upsert(table: str, key_cols: list[str], set_cols: list[str], rows: list[tuple]) -> None:
    """Generic delete+insert-style upsert for silver current-state tables:
    INSERT ... ON CONFLICT (key_cols) DO UPDATE SET set_cols, guarded so a
    late/out-of-order row never overwrites a newer one (matches the
    dbt-postgres delete+insert convention used one layer up in gold)."""
    if not rows:
        return
    all_cols = key_cols + set_cols
    col_list = ", ".join(all_cols)
    key_list = ", ".join(key_cols)
    set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in set_cols)
    updated_at_col = "updated_at" if "updated_at" in set_cols else set_cols[-1]

    with pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO {table} ({col_list})
                VALUES %s
                ON CONFLICT ({key_list}) DO UPDATE SET {set_clause}
                WHERE EXCLUDED.{updated_at_col} > {table}.{updated_at_col}
                """,
                rows,
                template="(" + ", ".join(["%s"] * len(all_cols)) + ")",
            )
        conn.commit()
