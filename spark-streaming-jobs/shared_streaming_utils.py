"""Shared helpers for the (single) persistent Structured Streaming job,
ingest_truck_positions.py. All Postgres access is via psycopg2 (no JDBC
driver in the Spark image, per config/spark/Dockerfile) - this is what makes
idempotent ON CONFLICT appends and PostGIS ST_MakePoint calls possible."""
from __future__ import annotations

import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, DoubleType


def get_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


TRUCK_POSITION_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("shipment_id", StringType(), False),
        StructField("truck_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("lat", DoubleType(), False),
        StructField("lon", DoubleType(), False),
        StructField("shipment_status", StringType(), False),
        StructField("destination_country", StringType(), False),
        StructField("event_at", StringType(), False),
    ]
)


def pg_conn():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["PIPELINE_DB_USER"],
        password=os.environ["PIPELINE_DB_PASSWORD"],
    )


def write_truck_positions_batch(rows: list[dict]) -> None:
    """Idempotent append into the bronze hypertable (ON CONFLICT DO NOTHING,
    keyed by event_id) plus an upsert into the silver "current position"
    table the live Grafana map reads from - kept in the same transaction so
    the map has near-zero extra latency beyond the micro-batch interval."""
    if not rows:
        return

    bronze_values = [
        (
            r["event_id"],
            r["shipment_id"],
            r["truck_id"],
            r["order_id"],
            r["lon"],
            r["lat"],
            r["shipment_status"],
            r["destination_country"],
            r["event_at"],
        )
        for r in rows
    ]

    # Postgres rejects ON CONFLICT DO UPDATE if a single INSERT's VALUES
    # list contains more than one row for the same conflict key ("cannot
    # affect row a second time") - confirmed by testing: a single 5s
    # micro-batch commonly contains multiple pings for the same shipment_id.
    # Dedupe to the latest event_at per shipment_id before the upsert.
    latest_per_shipment: dict[str, dict] = {}
    for r in rows:
        sid = r["shipment_id"]
        if sid not in latest_per_shipment or r["event_at"] > latest_per_shipment[sid]["event_at"]:
            latest_per_shipment[sid] = r

    silver_values = [
        (
            r["shipment_id"],
            r["truck_id"],
            r["order_id"],
            r["lon"],
            r["lat"],
            r["shipment_status"],
            r["destination_country"],
            r["event_at"],
        )
        for r in latest_per_shipment.values()
    ]

    with pg_conn() as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO iot.truck_position_events
                    (event_id, shipment_id, truck_id, order_id, geog,
                     shipment_status, destination_country, event_at, ingested_at)
                VALUES %s
                ON CONFLICT (event_id, event_at) DO NOTHING
                """,
                bronze_values,
                template="(%s, %s, %s, %s, ST_MakePoint(%s, %s)::geography, %s, %s, %s, now())",
            )

            execute_values(
                cur,
                """
                INSERT INTO silver.truck_positions_current
                    (shipment_id, truck_id, order_id, geog, shipment_status,
                     destination_country, updated_at)
                VALUES %s
                ON CONFLICT (shipment_id) DO UPDATE SET
                    truck_id = EXCLUDED.truck_id,
                    order_id = EXCLUDED.order_id,
                    geog = EXCLUDED.geog,
                    shipment_status = EXCLUDED.shipment_status,
                    destination_country = EXCLUDED.destination_country,
                    updated_at = EXCLUDED.updated_at
                WHERE EXCLUDED.updated_at > silver.truck_positions_current.updated_at
                """,
                silver_values,
                template="(%s, %s, %s, ST_MakePoint(%s, %s)::geography, %s, %s, %s)",
            )
        conn.commit()
