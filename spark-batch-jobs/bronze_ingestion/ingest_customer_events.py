from pyspark.sql.types import StringType, StructField, StructType

from shared_ingestion_utils import run_ingestion

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("name", StringType(), False),
        StructField("country", StringType(), False),
        StructField("city", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("event_at", StringType(), False),
    ]
)

COLUMNS = ["event_id", "customer_id", "event_type", "name", "country", "city", "segment", "event_at"]

if __name__ == "__main__":
    run_ingestion(
        topic="iot.customer_events",
        table="iot.customer_events",
        schema=SCHEMA,
        columns=COLUMNS,
        conflict_cols="event_id",
    )
