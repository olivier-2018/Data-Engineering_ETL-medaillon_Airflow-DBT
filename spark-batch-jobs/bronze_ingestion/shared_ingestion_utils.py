"""Shared helpers for the 5 periodic-batch Kafka -> bronze ingestion jobs
(§6a). These read Kafka via Spark's BATCH reader (not readStream) on an
Airflow schedule, tracking progress as Kafka offsets in
control.kafka_offsets - a different bookkeeping scheme than the
ingested_at-based control.watermarks used by the bronze -> silver jobs in
../ (bronze_ingestion tracks Kafka offsets; bronze->silver tracks Postgres
timestamps), since these two layers read from genuinely different kinds of
sources.

Postgres access is via psycopg2 throughout (no JDBC driver in the Spark
image) - duplicated in each of shared_ingestion_utils.py /
spark-batch-jobs/shared_utils.py / spark-streaming-jobs/shared_streaming_utils.py
rather than centralized, since each directory is mounted and --py-files
packaged independently for spark-submit; the duplication is a handful of
lines, an acceptable tradeoff here.
"""
from __future__ import annotations

import json
import logging
import os

import psycopg2
from py4j.protocol import Py4JJavaError
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.functions import max as spark_max
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


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


def get_starting_offsets_json(topic: str) -> str:
    """Returns a Spark Kafka-source startingOffsets JSON string: either the
    per-partition next-unread offset recorded by the previous run, or
    "earliest" the very first time this topic is ingested."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT partition, last_offset FROM control.kafka_offsets WHERE topic = %s",
                (topic,),
            )
            rows = cur.fetchall()

    if not rows:
        return "earliest"

    offsets = {str(partition): offset for partition, offset in rows}
    return json.dumps({topic: offsets})


def reset_offsets(topic: str) -> None:
    """Drops the stored watermark for a topic so the next run's
    get_starting_offsets_json() falls back to "earliest", same as a
    never-before-ingested topic. Used when the stored offset turns out to be
    stale (ahead of what the topic can actually serve - e.g. retention
    expired the segments it pointed at, or the topic was reset/recreated
    since the last run)."""
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM control.kafka_offsets WHERE topic = %s", (topic,))
        conn.commit()


def update_offsets(topic: str, kafka_df: DataFrame) -> None:
    """Records max(offset)+1 per partition actually read this run, so the
    next run picks up exactly where this one left off."""
    max_offsets = (
        kafka_df.groupBy("partition")
        .agg(spark_max("offset").alias("max_offset"))
        .collect()
    )
    if not max_offsets:
        return

    values = [(topic, row["partition"], row["max_offset"] + 1) for row in max_offsets]

    with pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO control.kafka_offsets (topic, partition, last_offset, updated_at)
                VALUES %s
                ON CONFLICT (topic, partition) DO UPDATE SET
                    last_offset = EXCLUDED.last_offset,
                    updated_at = EXCLUDED.updated_at
                """,
                values,
                template="(%s, %s, %s, now())",
            )
        conn.commit()


def append_rows(table: str, columns: list[str], rows: list[tuple], conflict_cols: str = "event_id") -> None:
    """Generic idempotent append (ON CONFLICT DO NOTHING) used by every
    bronze ingestion job - bronze is append-only, so there is never an
    UPDATE branch. `conflict_cols` is the exact SQL conflict-target text,
    e.g. "event_id" or "event_id, event_at" for hypertables with a
    composite primary key."""
    if not rows:
        return
    col_list = ", ".join(columns)
    with pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO {table} ({col_list}, ingested_at)
                VALUES %s
                ON CONFLICT ({conflict_cols}) DO NOTHING
                """,
                rows,
                template="(" + ", ".join(["%s"] * len(columns)) + ", now())",
            )
        conn.commit()


def run_ingestion(
    topic: str,
    table: str,
    schema: StructType,
    columns: list[str],
    conflict_cols: str = "event_id",
) -> None:
    """Generic periodic-batch Kafka -> bronze ingestion, parametrized per
    domain - the 5 ingestion job files in this directory are thin wrappers
    around this single implementation, since they're otherwise identical."""
    spark = get_spark_session(f"bronze-ingest-{table}")
    spark.sparkContext.setLogLevel("WARN")

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    starting_offsets = get_starting_offsets_json(topic)
    logger.info("Reading topic=%s startingOffsets=%s", topic, starting_offsets)

    kafka_df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("endingOffsets", "latest")
        .load()
    )

    try:
        is_empty = kafka_df.rdd.isEmpty()
    except Py4JJavaError as exc:
        # Spark's Kafka batch source doesn't validate startingOffsets against
        # the topic's actual available range up front - it only surfaces this
        # as a hard AssertionError once an action runs. This specific message
        # means our stored watermark points past what the topic can currently
        # serve (e.g. retention expired those segments, or the topic was
        # reset/recreated since the last run) - not a real code bug, and not
        # the ordinary "no new messages" case (which returns cleanly above).
        # Confirmed by testing: this is exactly what happens after Kafka
        # retention/topic state changes underneath an existing watermark.
        stale_watermark_reasons = (
            "is after the ending offset",
            # Raised when a topic's partition count grows (e.g. widened from 1
            # to 2 partitions) after this table's control.kafka_offsets rows
            # were written for the old, smaller partition set - Spark's Kafka
            # batch source requires startingOffsets to name every currently
            # -assigned partition, and the new one has no stored watermark yet.
            # Confirmed by testing: widening a topic live triggers exactly
            # this assertion on the very next run.
            "you must specify all TopicPartitions",
        )
        if any(reason in str(exc) for reason in stale_watermark_reasons):
            logger.warning(
                "Stored offset watermark for topic=%s is stale (points past what the "
                "topic can currently serve, or is missing a newly-added partition). "
                "Resetting it and skipping this run - the next run re-ingests from the "
                "earliest available offset, which is safe since bronze appends are "
                "idempotent (ON CONFLICT DO NOTHING).",
                topic,
            )
            reset_offsets(topic)
            spark.stop()
            return
        raise

    if is_empty:
        logger.info("No new messages on topic=%s", topic)
        spark.stop()
        return

    parsed = kafka_df.select(
        "partition",
        "offset",
        from_json(col("value").cast("string"), schema).alias("data"),
    ).select("partition", "offset", "data.*")

    rows = [tuple(row[c] for c in columns) for row in parsed.select(*columns).collect()]
    append_rows(table, columns, rows, conflict_cols)
    update_offsets(topic, parsed)

    logger.info("Ingested %d rows into %s", len(rows), table)
    spark.stop()
