from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from shared_ingestion_utils import run_ingestion

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("truck_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("name", StringType(), False),
        StructField("brand", StringType(), True),
        StructField("model", StringType(), True),
        StructField("size", StringType(), True),
        StructField("capacity", IntegerType(), False),
        StructField("weight_kg", DoubleType(), True),
        StructField("event_at", StringType(), False),
    ]
)

COLUMNS = [
    "event_id", "truck_id", "event_type", "name", "brand", "model", "size",
    "capacity", "weight_kg", "event_at",
]

# Low-volume, seed-once-plus-rare-updates domain (redesign plan §1) - still
# uses the same generic batch-ingestion engine as every other domain, just
# a much smaller/rarer topic.
if __name__ == "__main__":
    run_ingestion(
        topic="iot.truck_fleet_events",
        table="iot.truck_fleet_events",
        schema=SCHEMA,
        columns=COLUMNS,
        conflict_cols="event_id",
    )
