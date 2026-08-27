"""Shared helpers for the bronze -> silver jobs in this directory. These
read already-landed Postgres rows (bronze), incrementally by ingested_at,
tracking progress in control.silver_watermarks - a different bookkeeping
scheme than the Kafka-offset-based control.kafka_offsets used by
../bronze_ingestion/ (this layer is Postgres-to-Postgres, no Kafka
involved), since the two layers read from genuinely different kinds of
sources.

Postgres access is via psycopg2 throughout (no JDBC driver in the Spark
image) - duplicated in each of shared_ingestion_utils.py / shared_utils.py /
shared_streaming_utils.py rather than centralized, since each directory is
mounted and --py-files packaged independently for spark-submit; the
duplication is a handful of lines, an acceptable tradeoff here.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime

import psycopg2
import yaml
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

KAFKA_TOPICS_CONFIG_PATH = os.environ.get("KAFKA_TOPICS_CONFIG_PATH", "/opt/config/kafka_topics.yml")


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


def fetch_incremental(query: str, params: tuple = ()) -> list[tuple]:
    """Runs a parameterized SELECT via psycopg2 (no JDBC driver in the Spark
    image, by design - see redesign plan/ARCHITECTURE.md) and returns raw
    rows. Callers wrap these in spark.createDataFrame(rows, schema) to get
    a genuine Spark DataFrame for the dedup/validation logic that follows."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall()


def read_watermark(table_name: str) -> datetime:
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_processed_ingested_at FROM control.silver_watermarks WHERE table_name = %s",
                (table_name,),
            )
            row = cur.fetchone()
    return row[0] if row else datetime(1970, 1, 1)


def write_watermark(table_name: str, new_watermark: datetime) -> None:
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO control.silver_watermarks (table_name, last_processed_ingested_at, updated_at)
                VALUES (%s, %s, now())
                ON CONFLICT (table_name) DO UPDATE SET
                    last_processed_ingested_at = EXCLUDED.last_processed_ingested_at,
                    updated_at = EXCLUDED.updated_at
                """,
                (table_name, new_watermark),
            )
        conn.commit()


def route_errors(error_table: str, rows: list[tuple], reason_col: str = "reason_code") -> None:
    """Writes rejected rows to a silver.error_* table as raw_payload JSONB +
    a reason code - `rows` is a list of (raw_payload_json_str, reason_code)
    tuples. Never raises the pipeline; bad rows are quarantined, not
    dropped silently and not allowed to fail the whole batch."""
    if not rows:
        return
    with pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"INSERT INTO {error_table} (raw_payload, {reason_col}, rejected_at) VALUES %s",
                rows,
                template="(%s::jsonb, %s, now())",
            )
        conn.commit()
    logger.info("Routed %d row(s) to %s", len(rows), error_table)


def upsert(
    table: str,
    key_cols: list[str],
    set_cols: list[str],
    rows: list[tuple],
    coalesce_cols: list[str] | None = None,
) -> None:
    """Generic upsert used by every *_to_silver.py current-state table -
    ON CONFLICT (key_cols) DO UPDATE, guarded by a late-row check
    (only overwrite if the incoming row is actually newer) so batches that
    arrive out of order can never regress a row to stale data.

    `coalesce_cols` (e.g. a domain's "created_at", derived only from a
    one-time-ever event_type/status like "created" rather than re-sent on
    every event) are set via `COALESCE(EXCLUDED.col, table.col)` instead of
    a blind overwrite: pass NULL for that row's value whenever the current
    incremental batch doesn't contain that entity's originating event, and
    the existing stored value (if any) is left untouched rather than being
    nulled out or overwritten with an unrelated later event's timestamp.
    `rows` tuples must be ordered key_cols + set_cols + coalesce_cols."""
    if not rows:
        return
    coalesce_cols = coalesce_cols or []
    all_cols = key_cols + set_cols + coalesce_cols
    col_list = ", ".join(all_cols)
    set_clause = ", ".join(
        [f"{c} = EXCLUDED.{c}" for c in set_cols]
        + [f"{c} = COALESCE(EXCLUDED.{c}, {table}.{c})" for c in coalesce_cols]
    )
    conflict_cols = ", ".join(key_cols)
    with pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO {table} ({col_list})
                VALUES %s
                ON CONFLICT ({conflict_cols}) DO UPDATE SET {set_clause}
                WHERE EXCLUDED.updated_at > {table}.updated_at
                """,
                rows,
                template="(" + ", ".join(["%s"] * len(all_cols)) + ")",
            )
        conn.commit()


def upsert_partition(
    table: str,
    key_cols: list[str],
    set_cols: list[str],
    rows_iter,
    coalesce_cols: list[str] | None = None,
) -> None:
    """foreachPartition-compatible wrapper around upsert(): takes this
    partition's Row iterator (not the full batch collected to the driver),
    materializes it to a list, and delegates to upsert() unchanged - each
    partition opens and closes its own psycopg2 connection via upsert()'s
    own pg_conn(), so N partitions running concurrently on N Spark executor
    cores means N genuinely concurrent connections/transactions, not one
    shared connection. Must return None: foreachPartition is side-effect
    -only and discards any return value."""
    rows = list(rows_iter)
    if not rows:
        return
    upsert(table, key_cols, set_cols, rows, coalesce_cols=coalesce_cols)


def read_partitions_per_topic() -> int:
    """Reads config/kafka/topics_config.yml's partitions_per_topic (mounted
    read-only into airflow-scheduler at KAFKA_TOPICS_CONFIG_PATH) - the
    same value kafka-init uses to provision the Kafka topics themselves
    (see docs/ARCHITECTURE.md), reused here as N for repartition(N) so a
    job's write-side parallelism matches the topic's own partition count
    instead of a second, independently-drifting literal."""
    with open(KAFKA_TOPICS_CONFIG_PATH) as f:
        return yaml.safe_load(f)["partitions_per_topic"]
