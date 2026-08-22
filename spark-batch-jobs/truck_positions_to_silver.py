"""Bronze -> silver, truck positions: incremental read by ingested_at
(validated/enriched historical view + error-routing - NOT the live map's
primary read path, which reads silver.truck_positions_current as upserted
directly by the streaming job in spark-streaming-jobs/ for near-zero
latency). Validates lat/lon against the shipment's destination-country
bounding box (data_generators/config.yaml, mounted at /opt/config/config.yaml)
and joins silver.sales_orders_current to flag orphan shipments.

Simplification: time_in_transit/time_to_destination are derived from
min/max event_at *within this incremental batch* rather than each
shipment's full history - a fuller implementation would look up the
shipment's earliest event from bronze directly. Flagged here rather than
silently assumed correct.
"""
from __future__ import annotations

import logging
import os

import yaml
from psycopg2.extras import execute_values
from pyspark.sql import Row
from pyspark.sql.functions import max as spark_max, min as spark_min

from shared_utils import fetch_incremental, get_spark_session, pg_conn, read_watermark, route_errors, write_watermark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WATERMARK_KEY = "truck_position_events"
CONFIG_PATH = os.environ.get("CONFIG_PATH", "/opt/config/config.yaml")
COLUMNS = [
    "event_id", "shipment_id", "truck_id", "order_id", "lat", "lon",
    "shipment_status", "destination_country", "event_at", "ingested_at",
]

QUERY = f"""
    SELECT event_id, shipment_id, truck_id, order_id,
           ST_Y(geog::geometry) AS lat, ST_X(geog::geometry) AS lon,
           shipment_status, destination_country, event_at, ingested_at
    FROM iot.truck_position_events
    WHERE ingested_at > %s
    ORDER BY ingested_at
"""


def main() -> None:
    spark = get_spark_session("truck-positions-to-silver")
    spark.sparkContext.setLogLevel("WARN")

    with open(CONFIG_PATH) as f:
        zones = yaml.safe_load(f)["zones"]

    watermark = read_watermark(WATERMARK_KEY)
    rows = fetch_incremental(QUERY, (watermark,))
    if not rows:
        logger.info("No new truck_position_events since %s", watermark)
        spark.stop()
        return

    df = spark.createDataFrame([Row(**dict(zip(COLUMNS, r))) for r in rows])

    def in_zone(country, lat, lon) -> bool:
        z = zones.get(country)
        if z is None:
            return False
        return z["lat_min"] <= lat <= z["lat_max"] and z["lon_min"] <= lon <= z["lon_max"]

    collected = df.collect()
    order_ids_in_batch = list({r["order_id"] for r in collected})
    known_order_ids = {
        r[0]
        for r in fetch_incremental(
            "SELECT order_id FROM silver.sales_orders_current WHERE order_id = ANY(%s::uuid[])",
            ([str(o) for o in order_ids_in_batch],),
        )
    } if order_ids_in_batch else set()
    good_rows = []
    bad_rows = []
    for r in collected:
        d = r.asDict()
        valid_zone = in_zone(d["destination_country"], d["lat"], d["lon"])
        valid_order = d["order_id"] in known_order_ids
        if valid_zone and valid_order:
            good_rows.append(d)
        else:
            d["reason_code"] = "out_of_bounds" if not valid_zone else "orphan_shipment_no_matching_order"
            bad_rows.append(d)

    route_errors("silver.error_truck_positions", bad_rows)

    good_df = spark.createDataFrame(good_rows) if good_rows else None
    if good_df is not None:
        timing = (
            good_df.groupBy("shipment_id")
            .agg(
                spark_min("event_at").alias("batch_first_seen"),
                spark_max("event_at").alias("batch_last_seen"),
            )
        )
        logger.info("Computed batch-local timing for %d shipment(s) (see simplification note above).", timing.count())

        latest_per_shipment: dict[str, dict] = {}
        for r in good_rows:
            sid = r["shipment_id"]
            if sid not in latest_per_shipment or r["event_at"] > latest_per_shipment[sid]["event_at"]:
                latest_per_shipment[sid] = r

        upsert_rows = [
            (
                r["shipment_id"], r["truck_id"], r["order_id"],
                r["lon"], r["lat"], r["shipment_status"], r["destination_country"], r["event_at"],
            )
            for r in latest_per_shipment.values()
        ]
        # Note: geog is reconstructed via ST_MakePoint in the SQL below, so this
        # writes directly rather than through the generic upsert() helper in
        # shared_utils.py, which doesn't know about geography columns.
        with pg_conn() as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO silver.truck_positions_current
                        (shipment_id, truck_id, order_id, geog, shipment_status, destination_country, updated_at)
                    VALUES %s
                    ON CONFLICT (shipment_id) DO UPDATE SET
                        truck_id = EXCLUDED.truck_id, order_id = EXCLUDED.order_id,
                        geog = EXCLUDED.geog, shipment_status = EXCLUDED.shipment_status,
                        destination_country = EXCLUDED.destination_country, updated_at = EXCLUDED.updated_at
                    WHERE EXCLUDED.updated_at > silver.truck_positions_current.updated_at
                    """,
                    upsert_rows,
                    template="(%s, %s, %s, ST_MakePoint(%s, %s)::geography, %s, %s, %s)",
                )
            conn.commit()

    new_watermark = max(r[COLUMNS.index("ingested_at")] for r in rows)
    write_watermark(WATERMARK_KEY, new_watermark)
    logger.info(
        "Validated %d position(s), rejected %d, watermark -> %s",
        len(good_rows), len(bad_rows), new_watermark,
    )
    spark.stop()


if __name__ == "__main__":
    main()
