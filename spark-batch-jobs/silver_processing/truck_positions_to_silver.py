"""Bronze -> silver for truck positions. NOT the live map's primary data
path - the streaming job (spark-streaming-jobs/) already upserts
silver.truck_current_position directly, near-real-time, in the same
transaction as its bronze write. This batch job is the validated/enriched
pass + error-routing (referential checks the streaming path doesn't do),
re-upserting the same target table but guarded by the same
updated_at-newer-wins check the streaming job's own upsert already uses -
so this job can never regress a fresher streaming-written row.

Writes via bespoke raw SQL (not the generic upsert() helper) because geog
is a PostGIS GEOGRAPHY column that needs ST_MakePoint() reconstruction from
lat/lon, not a plain scalar upsert().

Validates: truck_id must exist in silver.truck_fleet_current, and
current_zone_id (if set) must exist in reference.delivery_zones -
consolidation/manifest correctness (which orders got batched together) is
NOT re-validated here, since there's no independent ground truth to check
it against - only structural referential integrity is a silver-layer
concern; "was the batching efficient" is a gold-layer reporting question."""
from __future__ import annotations

import json
import logging

from shared_utils import fetch_incremental, get_spark_session, pg_conn, read_watermark, route_errors, write_watermark

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BRONZE_TABLE_NAME = "truck_position_events"
SILVER_TABLE = "silver.truck_current_position"
ERROR_TABLE = "silver.error_truck_positions"

BRONZE_COLUMNS = [
    "event_id", "truck_id", "ST_Y(geog::geometry) AS lat", "ST_X(geog::geometry) AS lon",
    "truck_status", "current_zone_id", "event_at", "ingested_at",
]


def main() -> None:
    spark = get_spark_session("silver-truck-positions")
    spark.sparkContext.setLogLevel("WARN")

    watermark = read_watermark(BRONZE_TABLE_NAME)
    rows = fetch_incremental(
        f"SELECT {', '.join(BRONZE_COLUMNS)} FROM iot.truck_position_events WHERE ingested_at > %s",
        (watermark,),
    )
    if not rows:
        logger.info("No new truck_position_events since %s", watermark)
        spark.stop()
        return

    known_truck_ids = {r[0] for r in fetch_incremental("SELECT truck_id FROM silver.truck_fleet_current")}
    known_zone_ids = {r[0] for r in fetch_incremental("SELECT zone_id FROM reference.delivery_zones")}

    valid_rows, error_rows = [], []
    for event_id, truck_id, lat, lon, truck_status, current_zone_id, event_at, ingested_at in rows:
        col_names = ["event_id", "truck_id", "lat", "lon", "truck_status", "current_zone_id", "event_at", "ingested_at"]
        col_values = [event_id, truck_id, lat, lon, truck_status, current_zone_id, event_at, ingested_at]
        if truck_id not in known_truck_ids:
            error_rows.append((json.dumps(dict(zip(col_names, map(str, col_values)))), "orphan_truck"))
        elif current_zone_id is not None and current_zone_id not in known_zone_ids:
            error_rows.append((json.dumps(dict(zip(col_names, map(str, col_values)))), "unknown_zone"))
        else:
            valid_rows.append((truck_id, lat, lon, truck_status, current_zone_id, event_at))

    if error_rows:
        route_errors(ERROR_TABLE, error_rows)

    if valid_rows:
        # Dedup to the latest ping per truck within this batch (plain
        # Python here, not a Spark DataFrame - this job's per-row logic is
        # already the main compute; a distributed Window dedup would add
        # overhead with no benefit at this data volume).
        latest_by_truck: dict[str, tuple] = {}
        for truck_id, lat, lon, truck_status, current_zone_id, event_at in valid_rows:
            existing = latest_by_truck.get(truck_id)
            if existing is None or event_at > existing[-1]:
                latest_by_truck[truck_id] = (truck_id, lat, lon, truck_status, current_zone_id, event_at)

        with pg_conn() as conn:
            with conn.cursor() as cur:
                for truck_id, lat, lon, truck_status, current_zone_id, event_at in latest_by_truck.values():
                    cur.execute(
                        f"""
                        INSERT INTO {SILVER_TABLE} (truck_id, geog, truck_status, current_zone_id, updated_at)
                        VALUES (%s, ST_MakePoint(%s, %s)::geography, %s, %s, %s)
                        ON CONFLICT (truck_id) DO UPDATE SET
                            geog = EXCLUDED.geog,
                            truck_status = EXCLUDED.truck_status,
                            current_zone_id = EXCLUDED.current_zone_id,
                            updated_at = EXCLUDED.updated_at
                        WHERE EXCLUDED.updated_at > {SILVER_TABLE}.updated_at
                        """,
                        (truck_id, lon, lat, truck_status, current_zone_id, event_at),
                    )
            conn.commit()
        logger.info("Upserted %d truck position(s) into %s", len(latest_by_truck), SILVER_TABLE)

    new_watermark = max(r[-1] for r in rows)
    write_watermark(BRONZE_TABLE_NAME, new_watermark)
    logger.info("Rejected %d row(s) to %s", len(error_rows), ERROR_TABLE)
    spark.stop()


if __name__ == "__main__":
    main()
